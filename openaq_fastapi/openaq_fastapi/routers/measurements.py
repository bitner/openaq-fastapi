import logging
import time

import orjson as json
from fastapi import APIRouter, Depends

from .base import DB, Filters, MeasurementPaging

logger = logging.getLogger("locations")
logger.setLevel(logging.DEBUG)

router = APIRouter()


@router.get("/measurements")
async def locations_get(
    db: DB = Depends(),
    paging: MeasurementPaging = Depends(),
    filters: Filters = Depends(),
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

    monthly_count_q = f"""
        SELECT
            sum(value_count) as cnt
        FROM
            measurements_web_base
            LEFT JOIN sensors_monthly USING (sensors_id)
        WHERE
            month >= :date_from::timestamptz
            AND
            month <= :date_to::timestamptz - '1 month'::interval
            AND
            {where_sql}
        """

    monthly_count = await db.fetchval(monthly_count_q, params)
    if not monthly_count:
        monthly_count = 0

    remainder_count_q = f"""
        SELECT
            count(*) as cnt
        FROM measurements_web
        WHERE
            (
                datetime <
                date_trunc('month', :date_from::timestamptz)
                + '1 month'::interval
                OR
                datetime >
                date_trunc('month', :date_to::timestamptz)
            )
            AND
                datetime >= :date_from::timestamptz
            AND
                datetime < :date_to::timestamptz
            AND {where_sql}
        """

    remainder_count = await db.fetchval(remainder_count_q, params)
    if not remainder_count:
        remainder_count = 0

    count = monthly_count + remainder_count

    if count > 0:
        q = f"""
            SELECT
                json_build_object(
                    'location', site_name,
                    'parameter', measurand,
                    'date',json_build_object(
                                'utc', datetime
                            ),
                    'value', value,
                    'unit', units,
                    'coordinates', json_build_object(
                            'latitude', lat,
                            'longitude', lon
                        ),
                    'country', country,
                    'city', city
                ) as json
            FROM measurements_web
            WHERE {where_sql}
            AND datetime
            BETWEEN :date_from_adj::timestamptz
            AND :date_to_adj::timestamptz
            {paging_sql}
        """

        rows = await db.fetch(q, params)
        logger.debug("Time to before rows loaded: %s", time.time() - start)
        json_rows = [json.loads(r["json"]) for r in rows]
    else:
        json_rows = []

    meta = {
        "name": "openaq-api",
        "license": "CC BY 4.0",
        "website": "https://docs.openaq.org/",
        "page": paging.page,
        "limit": paging.limit,
        "found": monthly_count + remainder_count,
    }

    logger.debug("Total Time: %s", time.time() - start)
    return {"meta": meta, "results": json_rows}
