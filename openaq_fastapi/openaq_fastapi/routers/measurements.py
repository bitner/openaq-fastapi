from enum import Enum
import logging
import os
from typing import Optional

import orjson as json
from dateutil.tz import UTC
from datetime import timedelta, datetime
from fastapi import APIRouter, Depends, Query
from starlette.responses import Response
from ..db import DB
from ..models.responses import Meta
from ..models.queries import (
    APIBase,
    City,
    Country,
    DateRange,
    Geo,
    HasGeo,
    Location,
    Measurands,
    Sort,
)
import csv
import io

from openaq_fastapi.models.responses import (
    OpenAQResult,
)

logger = logging.getLogger("locations")
logger.setLevel(logging.DEBUG)

router = APIRouter()


def meas_csv(rows):
    output = io.StringIO()
    writer = csv.writer(output)
    header = [
        "location",
        "city",
        "country",
        "utc",
        "local",
        "parameter",
        "value",
        "unit",
        "latitude",
        "longitude",
    ]
    writer.writerow(header)
    for r in rows:
        try:
            row = [
                r["location"],
                r["city"],
                r["country"],
                r["date"]["utc"],
                r["date"]["local"],
                r["parameter"],
                r["value"],
                r["unit"],
                r["coordinates"]["latitude"],
                r["coordinates"]["longitude"],
            ]
            writer.writerow(row)
        except Exception as e:
            logger.debug(e)

    return output.getvalue()


class MeasOrder(str, Enum):
    city = "city"
    country = "country"
    location = "location"
    datetime = "datetime"


class Measurements(
    Location, City, Country, Geo, Measurands, HasGeo, APIBase, DateRange
):
    order_by: MeasOrder = Query("datetime")
    sort: Sort = "desc"
    isMobile: bool = None
    project: Optional[int] = None

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
        wheres.append(self.where_geo())
        wheres = list(filter(None, wheres))
        if len(wheres) > 0:
            return (" AND ").join(wheres)
        return " TRUE "


@router.get("/v1/measurements", tags=["v1"])
@router.get("/v2/measurements", tags=["v2"])
async def measurements_get(
    db: DB = Depends(),
    m: Measurements = Depends(Measurements.depends()),
    format: Optional[str] = None,
):
    count = None
    date_from = m.date_from
    date_to = m.date_to
    where = m.where()
    params = m.params()

    rolluptype = "node"

    if m.project is not None:
        locations = await db.fetchval(
            """
            SELECT nodes_from_project(:project::int);
            """,
            params,
        )
        params["locations"] = locations
        where = f"{where} AND sensor_nodes_id = ANY(:locations) "

    joins = """
        LEFT JOIN groups_sensors USING (groups_id)
        LEFT JOIN measurements_fastapi_base b
        ON (groups_sensors.sensors_id=b.sensors_id)
    """
    params["mobile"] = m.isMobile
    if m.isMobile is None:
        if (
            (m.location is None or len(m.location) == 0)
            and m.isMobile is None
            and m.coordinates is None
            and m.project is None
        ):
            joins = ""
            if m.country is None or len(m.country) == 0:
                rolluptype = "total"
            else:
                rolluptype = "country"
                params["country"] = m.country
                where = " name =ANY(:country) "
    # get overall summary numbers
    q = f"""
        SELECT
            sum(value_count),
            min(first_datetime),
            max(last_datetime)
        FROM rollups
        LEFT JOIN groups_view USING (groups_id, measurands_id)
        {joins}
        WHERE rollup = 'month' and type='{rolluptype}'
            AND
            st >= :date_from::timestamptz
            AND
            st < :date_to::timestamptz
            AND
            {where}
        """
    logger.debug(f"Params: {params}")
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
        date_to = min(
            date_to, range_end, datetime.utcnow().replace(tzinfo=UTC)
        )

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
    delta = timedelta(days=1)
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
                    sensor_nodes_id as location_id,
                    site_name as location,
                    measurand as parameter,
                    value,
                    datetime,
                    timezone,
                    CASE WHEN lon is not null and lat is not null THEN
                        json_build_object(
                            'latitude',lat,
                            'longitude', lon
                            )
                        WHEN b.geog is not null THEN
                        json_build_object(
                                'latitude', st_y(geog::geometry),
                                'longitude', st_x(geog::geometry)
                            )
                        ELSE NULL END AS coordinates,
                    units as unit,
                    country,
                    city,
                    ismobile
                FROM measurements a
                LEFT JOIN measurements_fastapi_base b USING (sensors_id)
                WHERE {m.where()}
                AND datetime >= :rangestart::timestamptz
                AND datetime <= :rangeend::timestamptz
                ORDER BY "{m.order_by}" {m.sort}
                OFFSET :offset
                LIMIT :limit
                ), t1 AS (
                    SELECT
                        location_id as "locationId",
                        location,
                        parameter,
                        value,
                        json_build_object(
                            'utc',
                            format_timestamp(datetime, 'UTC'),
                            'local',
                            format_timestamp(datetime, timezone)
                        ) as date,
                        unit,
                        coordinates,
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
                    results.extend(
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
        website=os.getenv("APP_HOST", "/"),
        page=m.page,
        limit=m.limit,
        found=count,
    )

    if format == "csv":
        return Response(
            content=meas_csv(results),
            media_type="text/csv",
            headers={
                "Content-Disposition": "attachment;filename=measurements.csv"
            },
        )

    output = OpenAQResult(meta=meta, results=results)

    # output = await db.fetchOpenAQResult(q, m.dict())

    return output
