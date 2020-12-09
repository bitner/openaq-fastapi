import logging
import time
from datetime import timedelta

import orjson as json
from dateutil.tz import UTC
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
    OBaseModel,
    OpenAQResult,
    Project,
    Spatial,
    Temporal,
)

logger = logging.getLogger("locations")
logger.setLevel(logging.DEBUG)

router = APIRouter()


class Measurements(
    Location, City, Country, Geo, Measurands, HasGeo, APIBase, DateRange
):
    order_by: Literal[
        "city", "country", "location", "sourceName", "datetime"
    ] = Query("datetime")

    def where(self):
        wheres = []
        for f, v in self:
            logger.debug(f"{f} {v}")
            if v is None:
                logger.debug('value none')
                continue
            else:
                if f == "project":
                    wheres.append(" groups_id = ANY(:project) ")
                elif f in ['has_geo','measurand','units','country','city','location']:
                    logger.debug('setting q')
                    wheres.append(f"{f} = ANY(:{f})")
        if len(wheres) > 0:
            return (" AND ").join(wheres)
        return " TRUE "


@router.get("/v1/measurements", response_model=OpenAQResult)
@router.get("/v2/measurements", response_model=OpenAQResult)
async def locations_get(
    db: DB = Depends(),
    m: Measurements = Depends(Measurements.depends()),
):
    date_from = m.date_from
    date_to = m.date_to

    # get overall summary numbers
    q = f"""
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
        WHERE {m.where()}
        """
    rows = await db.fetch(q, m.dict())
    logger.debug(f"{rows}")
    if rows is None:
        return OpenAQResult()
    try:
        total_count = rows[0][0]
        range_start = rows[0][1].replace(tzinfo=UTC)
        range_end = rows[0][2].replace(tzinfo=UTC)
    except:
        return OpenAQResult()

    if m.date_from is None:
        m.date_from = range_start
    else:
        m.date_from = max(date_from, range_start)

    if m.date_to is None:
        m.date_to = range_end
    else:
        m.date_to = min(date_to, range_end)

    count=None
    # if time is unbounded, we can just use the total count
    if (m.date_from == range_start) and (m.date_to == range_end):
        count = total_count

    # snap date_from and date_to to the data range
    if m.date_from is None:
        m.date_from = m.date_from_adj = range_start
    else:
        m.date_from = m.date_from_adj = max(m.date_from, range_start)

    if m.date_to is None:
        m.date_to = m.date_to_adj = range_end
    else:
        m.date_to = m.date_to_adj = min(m.date_to, range_end)

    # estimate time it would take to fulfill page requirements
    # buffer by a factor of 10 for variability
    delta = (range_end - range_start) / int(total_count) * 100000

    # if we are ordering by time, keep us from searching everything
    # for paging
    if m.order_by == "datetime":
        if m.sort == "asc":
            m.date_to_adj = m.date_from_adj + delta
        else:
            m.date_from_adj = m.date_to_adj - delta

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
                {m.where()}
            ;
            """

        estimate_j = await db.fetchval(estimated_count_q, m.dict())
        count = json.loads(estimate_j)[0]["Plan"]["Plan Rows"]
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

            FROM measurements_web
            WHERE {m.where()}
            AND datetime
            BETWEEN :date_from_adj::timestamptz
            AND :date_to_adj::timestamptz
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
                            'latitude', lat,
                            'longitude', lon
                        ) as coordinates,
                    country,
                    city
                FROM t
            )
            SELECT {count}::int as count, row_to_json(t1) as json FROM t1;
        """

    output = await db.fetchOpenAQResult(q, m.dict())

    return output
