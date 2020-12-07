import logging
import time
import re
from typing import Optional

import orjson as json
from fastapi import APIRouter, Depends, Query
from datetime import timedelta
from dateutil.tz import UTC

from .base import DB, MeasurementFilters, MeasurementPaging, Spatial, Temporal

logger = logging.getLogger("locations")
logger.setLevel(logging.DEBUG)

router = APIRouter()


@router.get("/averages")
async def locations_get(
    db: DB = Depends(),
    paging: MeasurementPaging = Depends(),
    filters: MeasurementFilters = Depends(),
    spatial: Spatial = Query(...),
    temporal: Temporal = Query(...),
):
    start = time.time()
    paging_q = await paging.sql()
    where_q = await filters.sql()
    date_from = paging.date_from
    date_to = paging.date_to

    params = {}
    params.update(paging_q["params"])
    params.update(where_q["params"])

    params.update( {
        "date_to": date_to,
        "date_from": date_from,
    })

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
    if summary is None or summary[0] is None:
        return {"error": "no sensors found matching search"}
    logger.debug('total_count %s, min %s, max %s', summary[0], summary[1], summary[2])
    total_count = summary[0]
    range_start = summary[1].replace(tzinfo=UTC)
    range_end = summary[2].replace(tzinfo=UTC)

    if date_from is None:
        date_from = range_start
    else:
        date_from = max(date_from, range_start)

    if date_to is None:
        date_to = range_end
    else:
        date_to = min(date_to, range_end)


    params.update( {
        "date_to": date_to,
        "date_from": date_from,
    })

    if spatial == 'project':
       spatial='source_name'


    q = f"""
        WITH base AS (
        SELECT
            jsonb_build_object(
                'parameter', measurand,
                'unit', units,
                :temporal, d::date,
                :spatial, name,
                'subtitle', subtitle,
                'measurement_count', value_count,
                'average', round((value_sum/value_count)::numeric, 4)
            ) || coalesce(rollups.metadata, '{}'::jsonb) as json
        FROM rollups
        JOIN groups USING (groups_id)
        JOIN sensors USING (sensors_id)
        JOIN measurands USING (measurands_id)
        WHERE
            rollup = :temporal
            AND
            name = :spatial
            AND
            :date_from <= sd
            AND
            :date_to > sd
            {paging_sql}
        )
        SELECT jsonb_agg(json), count(*)
        FROM base;
        """

    rows = await db.fetch(q, params)


    if len(rows) > 0:
        total = rows[0][0]
        json_rows = [json.loads(r["json"]) for r in rows]
    else:
        total = 0
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
