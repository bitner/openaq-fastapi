import logging
import time

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
    date_from_adj = paging.date_from_adj
    date_to_adj = paging.date_to_adj
    include_fields_q = await filters.measurement_fields()

    params = {
        "date_to": date_to,
        "date_from": date_from,
        "date_to_adj": date_to_adj,
        "date_from_adj": date_from_adj,
        "totalrows": paging.totalrows,
    }
    params.update(paging_q["params"])
    params.update(where_q["params"])

    where_sql = where_q["q"]
    paging_sql = paging_q["q"]

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

    if summary[1] > date_from:
        date_from = summary[1]
    if summary[1] > date_from_adj:
        date_from_adj = summary[1]
    if summary[2] < date_to:
        date_to = summary[2]
    if summary[2] < date_to_adj:
        date_to_adj = summary[2]

    date_from_adj = date_to_adj - timedelta(days=3)
    params.update( {
        "date_to": date_to,
        "date_from": date_from,
        "date_to_adj": date_to_adj,
        "date_from_adj": date_from_adj
    })

    # monthly_count_q = f"""
    #     WITH base AS (
    #         SELECT
    #             month,
    #             sum(value_count) as cnt
    #         FROM
    #             measurements_web_base
    #             LEFT JOIN sensors_monthly USING (sensors_id)
    #         WHERE
    #             month >= :date_from::timestamptz
    #             AND
    #             month <= :date_to::timestamptz - '1 month'::interval
    #             AND
    #             {where_sql}
    #         GROUP BY 1 ORDER BY 1 DESC
    #     ),
    #     subgroups AS (
    #         SELECT
    #             month,
    #             --sum(cnt) OVER () AS total,
    #             sum(cnt) OVER (
    #                 ORDER BY month DESC
    #                 ROWS BETWEEN
    #                 UNBOUNDED PRECEDING
    #                 AND
    #                 CURRENT ROW) AS running
    #         FROM base
    #     ),
    #     gtmonth AS (
    #     SELECT month, running FROM subgroups WHERE running>:totalrows
    #     ORDER BY 1 DESC LIMIT 1 )
    #     SELECT
    #         sum(cnt) as total,
    #         CASE WHEN gtmonth.month IS NOT NULL THEN gtmonth.month ELSE :date_from::timestamptz END as month
    #     FROM
    #         base, gtmonth
    #     GROUP BY base.month, gtmonth.month
    #     UNION ALL SELECT 0, :date_from::timestamptz
    #     ORDER BY total desc limit 1;
    #     """

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
