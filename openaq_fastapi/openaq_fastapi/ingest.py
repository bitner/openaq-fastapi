import boto3

from .settings import settings
import gzip
import io
import json
import sys
import psycopg2
import time




class StringIteratorIO(io.TextIOBase):
    def __init__(self, iter):
        self._iter = iter
        self._buff = ""

    def readable(self):
        return True

    def _read1(self, n=None):
        while not self._buff:
            try:
                self._buff = next(self._iter)
            except StopIteration:
                break
        ret = self._buff[:n]
        self._buff = self._buff[len(ret):]
        return ret

    def read(self, n=None):
        line = []
        if n is None or n < 0:
            while True:
                m = self._read1()
                if not m:
                    break
                line.append(m)
        else:
            while n > 0:
                m = self._read1(n)
                if not m:
                    break
                n -= len(m)
                line.append(m)
        return "".join(line)



def clean_csv_value(value):
    if value is None:
        return r"\N"
    return str(value).replace("\n", "\\n").replace("\t", " ")


def parse_json(j):
    location = j.pop("location", None)
    value = j.pop("value", None)
    unit = j.pop("unit", None)
    parameter = j.pop("parameter", None)
    country = j.pop("country", None)
    city = j.pop("city", None)
    source_name = j.pop("sourceName", None)
    date = j["date"]["utc"]
    j.pop("date", None)
    source_type = j.pop("sourceType", None)
    mobile = j.pop("mobile", None)
    avpd = j.pop("averagingPeriod", None)
    avpd_unit = avpd_value = None
    if avpd is not None:
        avpd_unit = avpd.pop("unit", None)
        avpd_value = avpd.pop("value", None)
    if (
        "coordinates" in j
        and "longitude" in j["coordinates"]
        and "latitude" in j["coordinates"]
    ):
        c = j.pop("coordinates")
        coords = (
            "SRID=4326;POINT(" + str(c["longitude"]) + " " + str(c["latitude"]) + ")"
        )
    else:
        coords = None

    data = json.dumps(j)

    row = [
        location,
        value,
        unit,
        parameter,
        country,
        city,
        data,
        source_name,
        date,
        coords,
        source_type,
        mobile,
        avpd_unit,
        avpd_value
    ]
    linestr = "\t".join(map(clean_csv_value, row)) + "\n"
    return linestr


AWS_BUCKET='openaq-fetches'
s3 = boto3.resource("s3")

processquery = '''
        CREATE TEMP TABLE IF NOT EXISTS tempfetchdata_sensors AS
        WITH t AS (
        SELECT DISTINCT
            location as site_name,
            unit as units,
            parameter as measurand,
            country,
            city,
            jsonb_merge_agg(data) as data,
            source_name,
            coords::geometry as geom,
            source_type,
            mobile as ismobile,
            avpd_unit,
            avpd_value,
            coords::geometry as cgeom,
            NULL::int as sensor_nodes_id,
            null::int as sensor_systems_id,
            null::int as measurands_id,
            null::int as sensors_id,
            null::jsonb as node_metadata,
            null::jsonb as sensor_metadata,
            array_agg(tfdid) as tfdids
        FROM tempfetchdata
        GROUP BY
            location,
            unit,
            parameter,
            country,
            city,
            coords,
            source_type,
            source_name,
            mobile,
            avpd_unit,
            avpd_value,
            sensor_nodes_id,
            sensor_systems_id,
            measurands_id,
            sensors_id,
            node_metadata,
            sensor_metadata
        )
        SELECT row_number() over () as tfsid, * FROM t;
        CREATE INDEX ON tempfetchdata_sensors (tfsid);


        -- Cleanup fields

        UPDATE tempfetchdata_sensors t SET
        geom = NULL WHERE st_x(geom) = 0 and st_y(geom) =0;

        UPDATE tempfetchdata_sensors SET units  = 'µg/m³'
        WHERE units IN ('µg/m��','��g/m³');

        UPDATE tempfetchdata_sensors SET
        node_metadata =
            jsonb_strip_nulls(
                COALESCE(data, '{}'::jsonb)
                ||
                jsonb_build_object('source_type', 'government', 'origin', 'openaq')
            ),
        sensor_metadata = jsonb_strip_nulls(jsonb_build_object(
            'data_averaging_period_seconds', avpd_value * 3600
            ))
        ;


        CREATE TEMP TABLE IF NOT EXISTS tempfetchdata_nodes AS
        SELECT * FROM (SELECT
            first_notnull(site_name) as site_name,
            first_notnull(source_name) as source_name,
            first_notnull(country) as country,
            first_notnull(city) as city,
            jsonb_merge_agg(node_metadata) as metadata,
            first_notnull(ismobile) as ismobile,
            null::int as sensor_nodes_id,
            null::int as sensor_systems_id,
            st_centroid(st_collect(geom)) as geom,
            array_agg(tfsid) as tfsids
        FROM tempfetchdata_sensors
        WHERE geom IS NOT NULL
        GROUP BY
            sensor_nodes_id,st_snaptogrid(geom, .0001)
        ) AS wgeom
        UNION ALL
        SELECT * FROM
        (SELECT
            site_name,
            source_name,
            first_notnull(country) as country,
            first_notnull(city) as city,
            jsonb_merge_agg(node_metadata) as metadata,
            first_notnull(ismobile) as ismobile,
            null::int as sensor_nodes_id,
            null::int as sensor_systems_id,
            null::geometry as geom,
            array_agg(tfsid) as tfsids
        FROM tempfetchdata_sensors
        WHERE geom IS NULL AND site_name is not null and source_name is not null
        GROUP BY
            site_name, source_name, sensor_nodes_id
        ) as nogeom
        ;



        -- Lookup Node Ids

        UPDATE tempfetchdata_nodes t SET sensor_nodes_id = sn.sensor_nodes_id FROM
        sensor_nodes sn
        WHERE t.geom is not null
        AND st_dwithin(sn.geom, t.geom, .0001);

        UPDATE tempfetchdata_nodes t SET sensor_nodes_id = sn.sensor_nodes_id FROM
        sensor_nodes sn
        WHERE
        t.sensor_nodes_id is null AND
        t.site_name is not null and t.source_name is not null and t.site_name = sn.site_name and t.source_name=sn.source_name;

        -- Update any records that have changed

        UPDATE sensor_nodes s SET
            site_name = COALESCE(t.site_name, s.site_name),
            source_name = COALESCE(t.source_name, s.source_name),
            city = COALESCE(t.city, s.city),
            country = COALESCE(t.country, s.country),
            ismobile = COALESCE(t.ismobile, s.ismobile),
            metadata = COALESCE(s.metadata, '{}'::jsonb) || t.metadata,
            geom = COALESCE(t.geom, s.geom)
        FROM tempfetchdata_nodes t
        WHERE t.sensor_nodes_id = s.sensor_nodes_id AND
        (
            (s.geom IS NULL and t.geom IS NOT NULL)
        OR

            ROW(
                t.sensor_nodes_id,
                t.ismobile,
                t.site_name,
                t.source_name,
                t.city,
                t.country,
                t.metadata
            ) IS DISTINCT FROM (
                s.sensor_nodes_id,
                s.ismobile,
                s.site_name,
                s.source_name,
                s.city,
                s.country,
                s.metadata
            )
        )
        ;

        -- Create new nodes where they don't exist
        WITH sn AS (
        INSERT INTO sensor_nodes (
            site_name,
            metadata,
            geom,
            source_name,
            city,
            country,
            ismobile
        )
        SELECT
            site_name,
            metadata,
            geom,
            source_name,
            city,
            country,
            ismobile
        FROM tempfetchdata_nodes t
        WHERE t.sensor_nodes_id is NULL
        RETURNING *
        )
        UPDATE tempfetchdata_nodes tf SET sensor_nodes_id = sn.sensor_nodes_id
        FROM sn WHERE tf.sensor_nodes_id is null and row(tf.site_name, tf.geom, tf.source_name) is not distinct
        from row(sn.site_name, sn.geom, sn.source_name);


        -- Get sensor systems


        UPDATE tempfetchdata_nodes t SET sensor_systems_id = ss.sensor_systems_id FROM
        sensor_systems ss
        WHERE t.sensor_nodes_id = ss.sensor_nodes_id;

        -- Add any rows that did not get an id into the rejects table and then delete
        INSERT INTO rejects
        SELECT clock_timestamp(), 'sensor_nodes', to_jsonb(tf) FROM
        tempfetchdata_nodes tf WHERE sensor_nodes_id IS NULL;
        DELETE FROM tempfetchdata_nodes WHERE sensor_nodes_id IS NULL;

        -- create sensor systems that don't exist
        WITH ss AS (
        INSERT INTO sensor_systems (sensor_nodes_id)
        SELECT DISTINCT sensor_nodes_id FROM tempfetchdata_nodes t
        WHERE t.sensor_systems_id is NULL AND t.sensor_nodes_id IS NOT NULL
        RETURNING *
        ) UPDATE tempfetchdata_nodes tf SET sensor_systems_id = ss.sensor_systems_id
        FROM ss WHERE tf.sensor_nodes_id=ss.sensor_nodes_id and tf.sensor_systems_id is null;

        -- Add any rows that did not get an id into the rejects table and then delete
        INSERT INTO rejects
        SELECT clock_timestamp(), 'sensor_systems', to_jsonb(tf) FROM
        tempfetchdata_nodes tf WHERE sensor_systems_id IS NULL;
        DELETE FROM tempfetchdata_nodes WHERE sensor_systems_id IS NULL;

        -- merge sensor node / system ids back to sensors table
        UPDATE tempfetchdata_sensors ts SET
            sensor_nodes_id = tn.sensor_nodes_id,
            sensor_systems_id = tn.sensor_systems_id
        FROM
            tempfetchdata_nodes tn
        WHERE
            ts.tfsid = ANY(tn.tfsids);


        -- add any measurands that don't exist
        UPDATE tempfetchdata_sensors t SET measurands_id= m.measurands_id FROM
        measurands m
        WHERE t.measurand = m.measurand AND t.units = m.units;

        WITH m AS (
        INSERT INTO measurands (measurand, units)
        SELECT DISTINCT measurand, units FROM tempfetchdata_sensors t
        WHERE t.measurands_id is NULL
        RETURNING *
        ) UPDATE tempfetchdata_sensors tf SET measurands_id = m.measurands_id
        FROM m WHERE tf.measurand=m.measurand and tf.units=m.units and tf.measurands_id is null;

        -- get cleaned sensors table
        CREATE TEMP TABLE IF NOT EXISTS tempfetchdata_sensors_clean AS
        SELECT
            null::int as sensors_id,
            sensor_nodes_id,
            sensor_systems_id,
            measurands_id,
            jsonb_merge_agg(sensor_metadata) as metadata,
            array_merge_agg(tfdids) as tfdids
        FROM tempfetchdata_sensors
        GROUP BY 1,2,3,4;


        -- get sensor id
        UPDATE tempfetchdata_sensors_clean t SET sensors_id = s.sensors_id
        FROM
        sensors s
        WHERE
            t.sensor_systems_id = s.sensor_systems_id
            and
            t.measurands_id = s.measurands_id
        ;
        -- Add any rows that did not get an id into the rejects table and then delete
        INSERT INTO rejects
        SELECT clock_timestamp(), 'sensors', to_jsonb(tf) FROM
        tempfetchdata_sensors_clean tf WHERE sensor_systems_id IS NULL or measurands_id is null;
        DELETE FROM tempfetchdata_sensors_clean WHERE sensor_systems_id IS NULL or measurands_id is null;

        -- add any sensors that don't exist
        WITH s AS (
            INSERT INTO sensors (
                sensor_systems_id,
                measurands_id,
                metadata
            )
            SELECT
                sensor_systems_id,
                measurands_id,
                metadata
            FROM
                tempfetchdata_sensors_clean tf
            WHERE
                tf.sensors_id IS NULL
            RETURNING *
        ) UPDATE tempfetchdata_sensors_clean tfc
            SET
                sensors_id = s.sensors_id
            FROM s
            WHERE
                tfc.sensors_id IS NULL
                AND
                s.sensor_systems_id = tfc.sensor_systems_id
                AND
                s.measurands_id = tfc.measurands_id
        ;

        UPDATE tempfetchdata t SET sensors_id = ts.sensors_id FROM
        tempfetchdata_sensors_clean ts WHERE
        t.tfdid = ANY(ts.tfdids);

        -- Add any rows that did not get an id into the rejects table and then delete
        INSERT INTO rejects
        SELECT clock_timestamp(), 'sensors', to_jsonb(tf) FROM
        tempfetchdata tf WHERE sensors_id IS NULL;
        DELETE FROM tempfetchdata WHERE sensors_id IS NULL;

        INSERT INTO measurements (sensors_id, datetime, value)
        SELECT sensors_id, datetime, value
        FROM tempfetchdata
        ON CONFLICT DO NOTHING;
        SELECT min(datetime), max(datetime) FROM tempfetchdata;
    '''

def load_fetch_file(key):
    obj = s3.Object(AWS_BUCKET, key)
    with psycopg2.connect(
        settings.DATABASE_WRITE_URL
    ) as connection:
        connection.set_session(autocommit=False)
        with connection.cursor() as cursor:
            cursor.execute('''
                CREATE TEMP TABLE IF NOT EXISTS tempfetchdata (
                    location text,
                    value float,
                    unit text,
                    parameter text,
                    country text,
                    city text,
                    data jsonb,
                    source_name text,
                    datetime timestamptz,
                    coords geography,
                    source_type text,
                    mobile boolean,
                    avpd_unit text,
                    avpd_value float,
                    tfdid int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    sensors_id int
                ) ON COMMIT DROP;
            ''')
            with gzip.GzipFile(fileobj=obj.get()["Body"]) as gz:
                f = io.BufferedReader(gz)
                iterator = StringIteratorIO((parse_json(json.loads(line)) for line in f))
                try:
                    cursor.copy_expert(
                        """
                        COPY tempfetchdata (
                            location,
                            value,
                            unit,
                            parameter,
                            country,
                            city,
                            data,
                            source_name,
                            datetime,
                            coords,
                            source_type,
                            mobile,
                            avpd_unit,
                            avpd_value
                        ) FROM STDIN;
                        """,
                        iterator
                    )
                    print(connection.notices)
                    # print('query:', cursor.query)
                    print('status:', cursor.statusmessage)
                except Exception as e:
                    print('full copy failed', key, e)
            cursor.execute(processquery)
            print(cursor.statusmessage)
            print(connection.notices)
            mindate, maxdate = cursor.fetchone()
            print(f"{mindate} {maxdate}")
            connection.commit()
            # old = connection.isolation_level
            # connection.set_isolation_level(0)
            # cursor.execute('''
            #     CALL refresh_continuous_aggregate('sensors_daily', %s, %s);
            # ''', (mindate, maxdate,))
            # print (connection.notices)
            # connection.commit()
            # connection.set_isolation_level(old)
            # print(f"{mindate} {maxdate}")
            # cursor.execute('''
            #     SELECT month, update_sensors_monthly(month) FROM
            #         generate_series(
            #             date_trunc('month', %s::timestamptz),
            #             date_trunc('month', %s::timestamptz),
            #             '1 month'::interval
            #         ) as month;
            #     REFRESH MATERIALIZED VIEW sensors_yearly;
            #     REFRESH MATERIALIZED VIEW sensors_total;
            #     REFRESH MATERIALIZED VIEW sensors_first_last;
            #     REFRESH MATERIALIZED VIEW sensor_nodes_json;
            #     REFRESH MATERIALIZED VIEW measurements_web_base;
            # ''', (mindate, maxdate,))
            # print (connection.notices)


            connection.commit()

def load_fetch_day(day):
    start = time.time()
    conn = boto3.client('s3')
    prefix=f'realtime-gzipped/{day}'
    keys=[]
    for f in conn.list_objects(Bucket=AWS_BUCKET, Prefix=prefix)['Contents']:
        # print(f['Key'])
        keys.append(f['Key'])


    with psycopg2.connect(
        settings.DATABASE_WRITE_URL
    ) as connection:
        connection.set_session(autocommit=False)
        with connection.cursor() as cursor:
            cursor.execute('''
                CREATE TEMP TABLE IF NOT EXISTS tempfetchdata (
                    location text,
                    value float,
                    unit text,
                    parameter text,
                    country text,
                    city text,
                    data jsonb,
                    source_name text,
                    datetime timestamptz,
                    coords geography,
                    source_type text,
                    mobile boolean,
                    avpd_unit text,
                    avpd_value float,
                    tfdid int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    sensors_id int
                ) ON COMMIT DROP;
            ''')
            for key in keys:
                obj = s3.Object(AWS_BUCKET, key)
                print(f'copying {key}')
                with gzip.GzipFile(fileobj=obj.get()["Body"]) as gz:
                    f = io.BufferedReader(gz)
                    iterator = StringIteratorIO((parse_json(json.loads(line)) for line in f))
                    try:
                        cursor.copy_expert(
                            """
                            COPY tempfetchdata (
                                location,
                                value,
                                unit,
                                parameter,
                                country,
                                city,
                                data,
                                source_name,
                                datetime,
                                coords,
                                source_type,
                                mobile,
                                avpd_unit,
                                avpd_value
                            ) FROM STDIN;
                            """,
                            iterator
                        )
                        # print(connection.notices)
                        # print('query:', cursor.query)
                        # print('status:', cursor.statusmessage)
                    except Exception as e:
                        print('full copy failed', key, e)
            print(f'All data copied {time.time()-start}')
            cursor.execute('''
                DELETE FROM tempfetchdata WHERE datetime
                NOT BETWEEN %s::timestamptz - '1 hour'::interval and %s::timestamptz + '1 days'::interval;
                DELETE FROM tempfetchdata WHERE coords is null and source_name is null and location is null;
                ''', (day, day,)
            )
            print(f'Removed data not from within 24 hours of this day {time.time()-start}')
            cursor.execute(processquery)
            print(f'Processed data {time.time()-start}')
            print(cursor.statusmessage)
            print(connection.notices)
            mindate, maxdate = cursor.fetchone()
            print(f"{mindate} {maxdate}")
            connection.commit()
            # old = connection.isolation_level
            # connection.set_isolation_level(0)
            # cursor.execute('''
            #     CALL refresh_continuous_aggregate('sensors_daily', %s, %s);
            # ''', (mindate, maxdate,))
            # print (connection.notices)
            # connection.commit()
            # connection.set_isolation_level(old)
            # print(f"{mindate} {maxdate}")
            # cursor.execute('''
            #     SELECT month, update_sensors_monthly(month) FROM
            #         generate_series(
            #             date_trunc('month', %s::timestamptz),
            #             date_trunc('month', %s::timestamptz),
            #             '1 month'::interval
            #         ) as month;
            #     REFRESH MATERIALIZED VIEW sensors_yearly;
            #     REFRESH MATERIALIZED VIEW sensors_total;
            #     REFRESH MATERIALIZED VIEW sensors_first_last;
            #     REFRESH MATERIALIZED VIEW sensor_nodes_json;
            #     REFRESH MATERIALIZED VIEW measurements_web_base;
            # ''', (mindate, maxdate,))
            # print (connection.notices)

            # print(f'Updated materialized views {time.time()-start}')


            # connection.commit()

def load_prefix(prefix):
    conn = boto3.client('s3')
    for f in conn.list_objects(Bucket=AWS_BUCKET, Prefix=prefix)['Contents']:
        print(f['Key'])
        load_fetch_file(f['Key'])


def load_range(start, end):
    from datetime import date, timedelta
    step = timedelta(days=1)
    while start < end:
        load_fetch_day(start)
        start += step
