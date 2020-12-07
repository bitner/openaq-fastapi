import logging
import time
from dateutil.tz import UTC

import orjson as json
from fastapi import APIRouter, Depends

from .base import DB, MeasurementFilters, MeasurementPaging
from datetime import timedelta

logger = logging.getLogger("locations")
logger.setLevel(logging.DEBUG)

router = APIRouter()


@router.get("/measurements")
async def locations_get(
    db: DB = Depends(),
    paging: MeasurementPaging = Depends(),
    filters: MeasurementFilters = Depends(),
):
    start = time.time()
    paging_q = await paging.sql()
    where_q = await filters.sql()
    date_from = paging.date_from
    date_to = paging.date_to
    include_fields_q = await filters.measurement_fields()
    params={}
    count = None
    params.update(paging_q["params"])
    params.update(where_q["params"])

    where_sql = where_q["q"]
    paging_sql = paging_q["q"]


    # get overall summary numbers
    summary_q = f"""
        SELECT
            sum(value_count) as count,
            min(first_datetime) as first,
            max(last_datetime) as last
        FROM sensors_total
        LEFT JOIN sensors_first_last USING (sensors_id)
        LEFT JOIN sensors USING (sensors_id)
        LEFT JOIN sensor_systems USING (sensor_systems_id)
        LEFT JOIN sensor_nodes USING (sensor_nodes_id)
        LEFT JOIN sensor_nodes_json USING (sensor_nodes_id)
        LEFT JOIN measurands USING (measurands_id)
        WHERE {where_sql}
        """
    summary = await db.fetchrow(summary_q, params)
    if summary is None:
        return {"error": "no sensors found matching search"}

    logger.debug('total_count %s, min %s, max %s', summary[0], summary[1], summary[2])
    total_count = summary[0]
    range_start = summary[1].replace(tzinfo=UTC)
    range_end = summary[2].replace(tzinfo=UTC)

    # if time is unbounded, we can just use the total count
    if (date_from is None or date_from == range_start) and (date_to is None or date_to == range_end):
        count = total_count

    # snap date_from and date_to to the data range
    if date_from is None:
        date_from = date_from_adj = range_start
    else:
        date_from = date_from_adj = max(date_from, range_start)

    if date_to is None:
        date_to = date_to_adj = range_end
    else:
        date_to = date_to_adj = min(date_to, range_end)

    # estimate time it would take to fulfill page requirements
    # buffer by a factor of 10 for variability
    delta = (range_end-range_start)/int(total_count) * int(paging.totalrows) * 10

    # if we are ordering by time, keep us from searching everything
    # for paging
    if paging.order_by == 'datetime':
        if paging.sort == 'asc':
            date_to_adj = date_from_adj + delta
        else:
            date_from_adj  = date_to_adj - delta

    params.update( {
        "date_to": date_to,
        "date_from": date_from,
        "date_to_adj": date_to_adj,
        "date_from_adj": date_from_adj
    })


    # get estimated count
    if count is None:
        estimated_count_q = f"""
            EXPLAIN (FORMAT JSON)
            SELECT 1 FROM measurements_web
            WHERE
                datetime >= :date_from::timestamptz
                AND
                datetime <= :date_to::timestamptz
                AND
                {where_sql}
            ;
            """

        estimate_j = await db.fetchval(estimated_count_q, params)
        count = json.loads(estimate_j)[0]['Plan']['Plan Rows']
        logger.debug(f"Estimate: {count}")

    # # get estimated count for adjusted date to see if we need to widen the window
    # estimated_count_q = f"""
    #     EXPLAIN (FORMAT JSON)
    #     SELECT 1 FROM measurements_web
    #     WHERE
    #         datetime >= :date_from_adj::timestamptz
    #         AND
    #         datetime <= :date_to_adj::timestamptz
    #         AND
    #         {where_sql}
    #     ;
    #     """

    # estimate_j = await db.fetchval(estimated_count_q, params)
    # limcount = json.loads(estimate_j)[0]['Plan']['Plan Rows']
    # logger.debug(f"Estimate: {limcount}")

    # tries = 0;
    # while count > 0 and limcount < 100000 and tries < 10:
    #     date_from_adj = date_from_adj - timedelta(days=pow(3,tries))
    #     params['date_from_adj'] = date_from_adj
    #     estimate_j = await db.fetchval(estimated_count_q, params)
    #     limcount = json.loads(estimate_j)[0]['Plan']['Plan Rows']
    #     logger.debug(f"Estimate: {limcount}")
    #     tries += 1



    # monthly_count_q = f"""
    #     SELECT
    #         sum(value_count) as cnt
    #     FROM
    #         measurements_web_base
    #         LEFT JOIN sensors_monthly USING (sensors_id)
    #     WHERE
    #         month >= :date_from::timestamptz
    #         AND
    #         month <= :date_to::timestamptz - '1 month'::interval
    #         AND
    #         {where_sql}
    #     ;
    #     """

    # monthly_row = await db.fetchrow(monthly_count_q, params)
    # # logger.debug(f"monthly row {monthly_row['total']} {monthly_row['month']}")
    # monthly_count = monthly_row['cnt']
    # # if monthly_row['month']:
    # #     date_from_adj = monthly_row['month']
    # #     params['date_from_adj'] = date_from_adj
    # if not monthly_count:
    #     monthly_count = 0

    # remainder_count_q = f"""
    #     SELECT
    #         sum(value_count) as cnt
    #     FROM measurements_web_base
    #     LEFT JOIN sensors_daily USING (sensors_id)
    #     WHERE
    #             day >= date_trunc('month', :date_to::timestamptz)
    #         AND
    #             day < :date_to::timestamptz
    #         AND {where_sql}
    #     """

    # remainder_count = await db.fetchval(remainder_count_q, params)
    # if not remainder_count:
    #     remainder_count = 0

    # count = monthly_count + remainder_count

    if count > 0:
        q = f"""
        WITH t AS (
            SELECT
                site_name as location,
                measurand as parameter,
                datetime,
                json->>'timezone' as timezone,
                lat,
                lon,
                units as unit,
                country,
                city
                {include_fields_q}
            FROM measurements_web
            WHERE {where_sql}
            AND datetime
            BETWEEN :date_from_adj::timestamptz
            AND :date_to_adj::timestamptz
            {paging_sql}
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
                            'latitude', lat,
                            'longitude', lon
                        ) as coordinates,
                    country,
                    city
                    {include_fields_q}
                FROM t
            )
            SELECT row_to_json(t1) as json FROM t1;
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
        "found": count,
    }

    logger.debug("Total Time: %s", time.time() - start)
    return {"meta": meta, "results": json_rows}
