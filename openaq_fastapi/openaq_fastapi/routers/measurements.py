import logging

import orjson as json
from dateutil.tz import UTC
from datetime import timedelta, datetime
from fastapi import APIRouter, Depends, Query
from pydantic.typing import Literal
from .base import (
    DB,
    APIBase,
    City,
    Country,
    DateRange,
    Geo,
    HasGeo,
    Location,
    Measurands,
    OpenAQResult,
    Meta,
    Sort,
)

logger = logging.getLogger("locations")
logger.setLevel(logging.DEBUG)

router = APIRouter()


class Measurements(
    Location, City, Country, Geo, Measurands, HasGeo, APIBase, DateRange
):
    order_by: Literal["city", "country", "location", "datetime"] = Query(
        "datetime"
    )
    sort: Sort = "desc"
    is_mobile: bool = None

    def where(self):
        wheres = []
        if self.lon and self.lat:
            wheres.append(
                " st_dwithin(st_makepoint(:lon, :lat)::geography,"
                " b.geom::geography, :radius) "
            )
        for f, v in self:
            if v is not None:
                if f == "location" and all(isinstance(x, int) for x in v):
                    wheres.append(" sensor_nodes_id = ANY(:location) ")
                elif f == "location":
                    wheres.append(" site_name = ANY(:location) ")
                elif f == "parameter":
                    wheres.append(" measurand = ANY(:measurand) ")
                elif f == "unit":
                    wheres.append(" units = ANY(:unit) ")
                elif f == "is_mobile":
                    wheres.append(" ismobile = :is_mobile ")

                elif f in ["country", "city"]:
                    wheres.append(f"{f} = ANY(:{f})")
        wheres = list(filter(None, wheres))
        if len(wheres) > 0:
            return (" AND ").join(wheres)
        return " TRUE "


@router.get("/v1/measurements", response_model=OpenAQResult)
@router.get("/v2/measurements", response_model=OpenAQResult)
async def measurements_get(
    db: DB = Depends(),
    m: Measurements = Depends(Measurements.depends()),
):
    date_from = m.date_from
    date_to = m.date_to

    # get overall summary numbers
    q = f"""
        SELECT
            sum(value_count),
            min(st),
            max(et)
        FROM rollups
        LEFT JOIN groups_w_measurand USING (groups_id, measurands_id)
        LEFT JOIN groups_sensors USING (groups_id)
        LEFT JOIN measurements_fastapi_base b USING (sensors_id)
        WHERE rollup = 'total' and type='node'
            AND
            {m.where()}
        """
    rows = await db.fetch(q, m.dict())
    logger.debug(f"{rows}")
    if rows is None:
        return OpenAQResult()
    try:
        total_count = rows[0][0]
        range_start = rows[0][1].replace(tzinfo=UTC)
        range_end = rows[0][2].replace(tzinfo=UTC)
    except Exception:
        return OpenAQResult()

    if date_from is None:
        date_from = range_start
    else:
        date_from = max(date_from, range_start)

    if date_to is None:
        date_to = range_end
    else:
        date_to = min(date_to, range_end, datetime.now().replace(tzinfo=UTC))

    count = None
    # if time is unbounded, we can just use the total count
    if (date_from == range_start) and (date_to == range_end):
        count = total_count

    date_from_adj = date_from
    date_to_adj = date_to

    qparams = m.dict()
    qparams["date_from_adj"] = date_from_adj
    qparams["date_to_adj"] = date_to_adj
    if False:

        # Get stats from previous month
        q = f"""
            SELECT
                st,
                sum(value_count)
            FROM rollups
            LEFT JOIN groups_w_measurand USING (groups_id, measurands_id)
            LEFT JOIN groups_sensors USING (groups_id)
            LEFT JOIN measurements_fastapi_base USING (sensors_id)
            WHERE rollup = 'month' and type='node'
                AND
                {m.where()}
            AND
                st
                BETWEEN :date_to_adj::timestamptz - '2 months'::interval
                AND :date_to_adj::timestamptz
            GROUP BY st
            ORDER BY st DESC OFFSET 1 LIMIT 1
            ;
        """
        row = await db.fetchrow(q, qparams)
        logger.debug(f"row {row}")
        if row:
            st = row[0]
            monthcount = row[1]
            logger.debug(f"Last month {st} {monthcount}")

            # estimate time it would take to fulfill page requirements
            # buffer by a factor of 2 for variability
            seconds_per_count = 30 * 24 * 60 * 60 / int(monthcount)
            delta = timedelta(seconds=int(seconds_per_count * m.limit * 10))
            logger.debug(f"delta: {delta}")
        else:
            delta = timedelta(days=30)
            seconds_per_count = 60 * 30

        count = int(
            (date_to_adj - date_from_adj).total_seconds() / seconds_per_count
        )

        qparams["date_from_adj"] = date_from_adj
        qparams["date_to_adj"] = date_to_adj

        if False and count is None:
            estimated_count_q = f"""
                EXPLAIN (FORMAT JSON)
                SELECT 1 FROM measurements_fastapi_base b
                LEFT JOIN all_measurements a USING (sensors_id)
                WHERE
                    datetime >= :date_from_adj::timestamptz
                    AND
                    datetime <= :date_to_adj::timestamptz
                    AND
                    {m.where()}
                ;
                """

            estimate_j = await db.fetchval(estimated_count_q, qparams)
            estimate_d = json.loads(estimate_j)
            logger.debug(estimate_d[0])
            count = estimate_d[0]["Plan"]["Plan Rows"]
            logger.debug(f"Estimate: {count}")
            pass

    # if we are ordering by time, keep us from searching everything
    # for paging
    delta = timedelta(days=15)
    if m.order_by == "datetime":
        if m.sort == "asc":
            date_to_adj = date_from_adj + delta
        else:
            date_from_adj = date_to_adj - delta

    qparams["date_from_adj"] = date_from_adj
    qparams["date_to_adj"] = date_to_adj

    count = total_count
    results = []
    if count > 0:
        if m.sort == "asc":
            rangestart = date_from
            rangeend = date_from + delta
        else:
            rangeend = date_to
            rangestart = date_to - delta

        logger.debug(f"Entering loop {count} {rangestart} {rangeend}")
        rc = 0
        qparams["rangestart"] = rangestart
        qparams["rangeend"] = rangeend
        while (
            rc < m.limit
            and rangestart >= date_from
            and rangeend <= date_to
        ):
            logger.debug(f"looping... {rc} {rangestart} {rangeend}")
            q = f"""
            WITH t AS (
                SELECT
                    site_name as location,
                    measurand as parameter,
                    value,
                    datetime,
                    timezone,
                    COALESCE(a.geom, b.geom, NULL) as geom,
                    units as unit,
                    country,
                    city,
                    ismobile
                FROM measurements_fastapi_base b
                LEFT JOIN all_measurements a USING (sensors_id)
                WHERE {m.where()}
                AND datetime
                BETWEEN :rangestart::timestamptz
                AND :rangeend::timestamptz
                ORDER BY "{m.order_by}" {m.sort}
                OFFSET :offset
                LIMIT :limit
                ), t1 AS (
                    SELECT
                        location,
                        parameter,
                        json_build_object(
                                    'utc', format_timestamp(datetime, 'UTC'),
                                    'local', format_timestamp(datetime, timezone)
                                ) as date,
                        unit,
                        json_build_object(
                                'latitude', st_y(geom),
                                'longitude', st_x(geom)
                            ) as coordinates,
                        country,
                        city,
                        ismobile as "isMobile"
                    FROM t
                )
                SELECT {count}::int as count, row_to_json(t1) as json FROM t1;
            """

            rows = await db.fetch(q, qparams)
            if rows:
                logger.debug(f"{len(rows)} rows found")
                rc = rc + len(rows)
                if len(rows) > 0 and rows[0][1] is not None:
                    results.append(
                        [
                            json.loads(r[1])
                            for r in rows
                            if isinstance(r[1], str)
                        ]
                    )
            logger.debug(f"ran query... {rc} {rangestart} {date_from_adj}{rangeend} {date_to_adj}")
            if m.sort == "desc":
                rangestart -= delta
                rangeend -= delta
            else:
                rangestart += delta
                rangeend += delta
            logger.debug(f"stepped ranges... {rc} {rangestart} {date_from_adj}{rangeend} {date_to_adj}")
            qparams["rangestart"] = rangestart
            qparams["rangeend"] = rangeend
    meta = Meta(
        page=m.page,
        limit=m.limit,
        found=count,
    )
    output = OpenAQResult(meta=meta, results=results)

    # output = await db.fetchOpenAQResult(q, m.dict())

    return output
