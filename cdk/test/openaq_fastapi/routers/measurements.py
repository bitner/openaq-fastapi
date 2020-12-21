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
    isMobile: bool = None

    def where(self):
        wheres = []
        if self.lon and self.lat:
            wheres.append(
                " st_dwithin(st_makepoint(:lon, :lat)::geography,"
                " b.geog, :radius) "
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
                elif f == "isMobile":
                    wheres.append(" ismobile = :mobile ")

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
    count = None
    date_from = m.date_from
    date_to = m.date_to

    rolluptype = "node"
    joins = """
        LEFT JOIN rollups.groups_sensors USING (groups_id)
        LEFT JOIN rollups.measurements_fastapi_base b
        ON (groups_sensors.sensors_id=b.sensors_id)
    """
    params = m.dict()
    params['mobile'] = m.isMobile
    where = m.where()
    if m.isMobile is None:
        if (
            (m.location is None or len(m.location)) == 0
            and m.is_mobile is None
            and m.coordinates is None
        ):
            joins = ""
            if m.country is None or len(m.country) == 0:
                rolluptype = "total"
            else:
                rolluptype = "country"
                params = {"country": m.country}
                where = " name =ANY(:country) "
    # get overall summary numbers
    q = f"""
        SELECT
            sum(value_count),
            min(first_datetime),
            max(last_datetime)
        FROM rollups.rollups
        LEFT JOIN rollups.groups_view USING (groups_id, measurands_id)
        {joins}
        WHERE rollup = 'month' and type='{rolluptype}'
            AND
            st >= :date_from::timestamptz
            AND
            st < :date_to::timestamptz
            AND
            {where}
        """
    rows = await db.fetch(q, params)
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

    count = total_count
    # if time is unbounded, we can just use the total count
    if (date_from == range_start) and (date_to == range_end):
        count = total_count

    date_from_adj = date_from
    date_to_adj = date_to


    params["date_from_adj"] = date_from_adj
    params["date_to_adj"] = date_to_adj

    days = (date_to_adj - date_from_adj).total_seconds() / (24 * 60 * 60)
    logger.debug(f" days {days}")

    # if we are ordering by time, keep us from searching everything
    # for paging
    delta = timedelta(days=5)
    if m.order_by == "datetime":
        if m.sort == "asc":
            date_to_adj = date_from_adj + delta
        else:
            date_from_adj = date_to_adj - delta

    params["date_from_adj"] = date_from_adj
    params["date_to_adj"] = date_to_adj

    # count = total_count
    results = []
    if count > 0:
        if m.sort == "asc":
            rangestart = date_from
            rangeend = min(date_from + delta, date_to)
        else:
            rangeend = date_to
            rangestart = max(date_to - delta, date_from)

        logger.debug(f"Entering loop {count} {rangestart} {rangeend}")
        rc = 0
        params["rangestart"] = rangestart
        params["rangeend"] = rangeend
        while rc < m.limit and rangestart >= date_from and rangeend <= date_to:
            logger.debug(f"looping... {rc} {rangestart} {rangeend}")
            q = f"""
            WITH t AS (
                SELECT
                    sensors_id as location_id,
                    site_name as location,
                    measurand as parameter,
                    value,
                    datetime,
                    timezone,
                    COALESCE(a.geog, b.geog, NULL) as geog,
                    units as unit,
                    country,
                    city,
                    ismobile
                FROM measurements_all a
                LEFT JOIN rollups.measurements_fastapi_base b USING (sensors_id)
                WHERE {m.where()}
                AND datetime
                BETWEEN :rangestart::timestamptz
                AND :rangeend::timestamptz
                ORDER BY "{m.order_by}" {m.sort}
                OFFSET :offset
                LIMIT :limit
                ), t1 AS (
                    SELECT
                        location_id,
                        location,
                        parameter,
                        json_build_object(
                            'utc',
                            format_timestamp(datetime, 'UTC'),
                            'local',
                            format_timestamp(datetime, timezone)
                        ) as date,
                        unit,
                        json_build_object(
                                'latitude', st_y(geog::geometry),
                                'longitude', st_x(geog::geometry)
                            ) as coordinates,
                        country,
                        city,
                        ismobile as "isMobile"
                    FROM t
                )
                SELECT {count}::bigint as count,
                row_to_json(t1) as json FROM t1;
            """

            rows = await db.fetch(q, params)
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
            logger.debug(
                f"ran query... {rc} {rangestart}"
                f" {date_from_adj}{rangeend} {date_to_adj}"
            )
            if m.sort == "desc":
                rangestart -= delta
                rangeend -= delta
            else:
                rangestart += delta
                rangeend += delta
            logger.debug(
                f"stepped ranges... {rc} {rangestart}"
                f" {date_from_adj}{rangeend} {date_to_adj}"
            )
            params["rangestart"] = rangestart
            params["rangeend"] = rangeend
    meta = Meta(
        page=m.page,
        limit=m.limit,
        found=count,
    )
    output = OpenAQResult(meta=meta, results=results)

    # output = await db.fetchOpenAQResult(q, m.dict())

    return output
