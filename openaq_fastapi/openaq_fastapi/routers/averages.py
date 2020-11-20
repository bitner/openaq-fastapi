import logging
import time
from typing import Optional

import orjson as json
from fastapi import APIRouter, Depends, Query

from .base import DB, Filters, MeasurementPaging, Spatial, Temporal

logger = logging.getLogger("locations")
logger.setLevel(logging.DEBUG)

router = APIRouter()


@router.get("/averages")
async def locations_get(
    db: DB = Depends(),
    paging: MeasurementPaging = Depends(),
    filters: Filters = Depends(),
    spatial: Optional[Spatial] = Query("country"),
    temporal: Optional[Temporal] = Query("year"),
):
    start = time.time()
    paging_q = await paging.sql()
    where_q = await filters.sql()
    date_from = paging.date_from
    date_to = paging.date_to
    date_from_adj = paging.date_from_adj
    date_to_adj = paging.date_to_adj

    params = {
        "date_to": date_to,
        "date_from": date_from,
        "date_to_adj": date_to_adj,
        "date_from_adj": date_from_adj,
    }
    params.update(paging_q["params"])
    params.update(where_q["params"])

    where_sql = where_q["q"]
    paging_sql = paging_q["q"]

    if spatial == "location":
        spatial = "site_name"
        spatial_sql = "b. country, b.city, b.site_name, b.lat, b.lon"
        final_query = """
            SELECT
                total,
                json_build_object(
                    'country', country,
                    'city', city,
                    'location', site_name,
                    'coordinates', json_build_object(
                        'longitude', lon,
                        'latitude', lat
                        ),
                    'parameter', measurand,
                    'date', d::date,
                    'measurement_count', measurement_count,
                    'average', average,
                    'unit', units
                ) as json
            FROM t
        """

    elif spatial == "city":
        spatial_sql = "b.country, b.city"
        final_query = """
            SELECT
                total,
                json_build_object(
                    'country', country,
                    'city', city,
                    'parameter', measurand,
                    'date', d::date,
                    'measurement_count', measurement_count,
                    'average', average,
                    'unit', units
                ) as json
            FROM t
        """
    else:
        spatial_sql = "b.country"
        final_query = """
            SELECT
                total,
                json_build_object(
                    'country', country,
                    'parameter', measurand,
                    'date', d::date,
                    'measurement_count', measurement_count,
                    'average', average,
                    'unit', units
                ) as json
            FROM t
        """

    if temporal == "day":
        q = f"""
            WITH t AS (
                SELECT
                    count(*) over () as total,
                    {spatial_sql},
                    day as d,
                    measurand,
                    units,
                    sum(value_count) as measurement_count,
                    sum(value_sum) / sum(value_count) as average
                FROM
                    measurements_web_base b
                    LEFT JOIN sensors_daily USING (sensors_id)
                WHERE
                    day >= :date_from::timestamptz
                    AND
                    day <= :date_to::timestamptz
                    AND
                    {where_sql}
                GROUP BY
                    day,
                    measurand,
                    units,
                    {spatial_sql}
                {paging_sql}
            )
            {final_query}
            """
    elif temporal == "month":
        q = f"""
        WITH t AS (
            SELECT
                count(*) over () as total,
                {spatial_sql},
                month as d,
                measurand,
                units,
                sum(value_count) as measurement_count,
                sum(value_sum) / sum(value_count) as average
            FROM
                measurements_web_base b
                LEFT JOIN sensors_monthly USING (sensors_id)
            WHERE
                month >= :date_from::timestamptz
                AND
                month <= :date_to::timestamptz
                AND
                {where_sql}
            GROUP BY
                month,
                measurand,
                units,
                {spatial_sql}
            {paging_sql}
        )
        {final_query}
        """
    else:
        q = f"""
        WITH t AS (
            SELECT
                count(*) over () as total,
                {spatial_sql},
                year as d,
                measurand,
                units,
                sum(value_count) as measurement_count,
                sum(value_sum) / sum(value_count) as average
            FROM
                measurements_web_base b
                LEFT JOIN sensors_yearly USING (sensors_id)
            WHERE
                year >= :date_from::timestamptz
                AND
                year <= :date_to::timestamptz
                AND
                {where_sql}
            GROUP BY
                year,
                measurand,
                units,
                {spatial_sql}
            {paging_sql}
        )
        {final_query}
        """

    rows = await db.fetch(q, params)

    total = rows[0][0]

    if len(rows) > 0:
        json_rows = [json.loads(r["json"]) for r in rows]
    else:
        json_rows = []

    meta = {
        "name": "openaq-api",
        "license": "CC BY 4.0",
        "website": "https://docs.openaq.org/",
        "page": paging.page,
        "limit": paging.limit,
        "found": total,
    }

    logger.debug("Total Time: %s", time.time() - start)
    return {"meta": meta, "results": json_rows}
