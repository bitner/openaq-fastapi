import gzip
import io
import json
import os
import time
from datetime import datetime, timedelta
from pathlib import Path

import boto3
import psycopg2
import typer

from ..settings import settings

app = typer.Typer()

dir_path = os.path.dirname(os.path.realpath(__file__))


FETCH_BUCKET = settings.OPENAQ_FETCH_BUCKET
s3 = boto3.resource("s3")


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
        self._buff = self._buff[len(ret) :]
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
        coords = "".join(
            (
                "SRID=4326;POINT(",
                str(c["longitude"]),
                " ",
                str(c["latitude"]),
                ")",
            )
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
        avpd_value,
    ]
    linestr = "\t".join(map(clean_csv_value, row)) + "\n"
    return linestr


def get_query(file, **params):
    # print(f"{params}")
    query = Path(os.path.join(dir_path, file)).read_text()
    if params is not None and len(params) >= 1:
        print(f"adding parameters {params}")
        query = query.format(**params)
    return query


def create_staging_table(cursor):
    cursor.execute(get_query("fetch_staging.sql"))


def copy_data(cursor, key):
    obj = s3.Object(FETCH_BUCKET, key)
    print(f"Copying data for {key}")
    with gzip.GzipFile(fileobj=obj.get()["Body"]) as gz:
        f = io.BufferedReader(gz)
        iterator = StringIteratorIO(
            (parse_json(json.loads(line)) for line in f)
        )
        try:
            query = get_query("fetch_copy.sql")
            cursor.copy_expert(query, iterator)
            print("status:", cursor.statusmessage)
        except Exception as e:
            print("full copy failed", key, e)


def process_data(cursor):
    query = get_query("fetch_ingest.sql")
    cursor.execute(query)
    print(cursor.statusmessage)
    results = cursor.fetchone()
    print(f"{results}")
    if results:
        mindate, maxdate = results
        print(f"{mindate} {maxdate}")
        return mindate, maxdate
    return None, None


def filter_data(cursor):
    query = get_query("fetch_filter.sql")
    cursor.execute(query)
    print(f"Deleted {cursor.rowcount} rows.")
    print(cursor.statusmessage)
    results = cursor.fetchone()
    print(f"{results}")
    if results:
        mindate, maxdate = results
        return mindate, maxdate
    return None, None


def update_rollups(cursor, mindate, maxdate):
    if mindate is not None and maxdate is not None:
        print(
            f"Updating rollups from {mindate.isoformat()} to {maxdate.isoformat()}"
        )
        cursor.execute(
            get_query(
                "update_rollups.sql",
                mindate=mindate.isoformat(),
                maxdate=maxdate.isoformat(),
            )
        )
        print(cursor.statusmessage)
    else:
        print("could not get date range, skipping rollup update")


@app.command()
def load_fetch_file(key: str):
    with psycopg2.connect(settings.DATABASE_WRITE_URL) as connection:
        connection.set_session(autocommit=False)
        with connection.cursor() as cursor:
            create_staging_table(cursor)
            copy_data(cursor, key)
            min_date, max_date = filter_data(cursor)
            mindate, maxdate = process_data(cursor)
            update_rollups(cursor, mindate=mindate, maxdate=maxdate)
            connection.commit()


@app.command()
def load_fetch_day(day: str):
    start = time.time()
    conn = boto3.client("s3")
    prefix = f"realtime-gzipped/{day}"
    keys = []
    try:
        for f in conn.list_objects(Bucket=FETCH_BUCKET, Prefix=prefix)[
            "Contents"
        ]:
            # print(f['Key'])
            keys.append(f["Key"])
    except Exception:
        print(f"no data found for {day}")
        return None

    with psycopg2.connect(settings.DATABASE_WRITE_URL) as connection:
        connection.set_session(autocommit=False)
        with connection.cursor() as cursor:
            create_staging_table(cursor)
            for key in keys:
                copy_data(cursor, key)
            print(f"All data copied {time.time()-start}")
            filter_data(cursor)
            mindate, maxdate = process_data(cursor)
            update_rollups(cursor, mindate=mindate, maxdate=maxdate)
            connection.commit()


def load_prefix(prefix):
    conn = boto3.client("s3")
    for f in conn.list_objects(Bucket=FETCH_BUCKET, Prefix=prefix)["Contents"]:
        print(f["Key"])
        load_fetch_file(f["Key"])


@app.command()
def load_range(
    start: datetime = typer.Argument(datetime.utcnow().date().isoformat()),
    end: datetime = typer.Argument(datetime.utcnow().date().isoformat()),
):
    print(
        f"Loading data from {start.date().isoformat()}"
        f" to {end.date().isoformat()}"
    )

    step = timedelta(days=1)
    while start <= end:
        load_fetch_day(f"{start.date().isoformat()}")
        start += step


def handler(event, context):
    print(event)
    records = event.get("Records")
    try:
        for record in records:
            bucket = record["s3"]["bucket"]["name"]
            object = record["s3"]["object"]["name"]
            print(f"Processing {bucket} {object}")
            if bucket == "openaq":
                load_fetch_file(object)
    except Exception as e:
        print(f"Exception: {e}")


if __name__ == "__main__":
    app()
