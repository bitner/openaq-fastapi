import logging
import time
import re
from typing import Optional

import orjson as json
from fastapi import APIRouter, Depends, Query
from datetime import timedelta
from dateutil.tz import UTC

from .base import DB, Spatial, Temporal, DateRange, OBaseModel, APIBase, Country, Location, Project, Measurands, OpenAQResult

logger = logging.getLogger("averages")
logger.setLevel(logging.DEBUG)

router = APIRouter()



class Averages(APIBase, Country, Location, Project, Measurands, DateRange):
    spatial: Spatial = Query(...)
    temporal: Temporal = Query(...)
    def where(self):
        wheres=[]
        if self.spatial == 'country' and self.country is not None:
            wheres.append(f"name = ANY(:country)")
        if self.spatial == 'project' and self.project is not None:
            wheres.append(f"name = ANY(:project)")
        if self.spatial == 'location' and self.location is not None:
            wheres.append(f"name = ANY(:location)")
        for f, v in self:
            logger.debug(f"{f} {v}")
            if v is not None and f in ['measurand','units']:
                wheres.append(f"{f} = ANY(:{f})")
        if len(wheres) >0:
            return (' AND ').join(wheres)
        return " TRUE "


@router.get("/v1/averages", response_model=OpenAQResult)
@router.get("/v2/averages", response_model=OpenAQResult)
async def averages_v2_get(
    db: DB = Depends(),
    av: Averages = Depends(Averages.depends()),
):
    date_from = av.date_from
    date_to = av.date_to


    q = f"""
        SELECT
            min(st),
            max(et)
        FROM rollups
        LEFT JOIN groups_w_measurand USING (groups_id, measurands_id)
        WHERE
            rollup = 'total'
            AND
            type = :spatial::text
            AND
            {av.where()}
        """

    rows = await db.fetch(q, av.dict())
    if rows is None:
        return OpenAQResult()
    try:
        range_start = rows[0][0].replace(tzinfo=UTC)
        range_end = rows[0][1].replace(tzinfo=UTC)
    except:
        return OpenAQResult()

    if date_from is None:
        av.date_from = range_start
    else:
        av.date_from = max(date_from, range_start)

    if date_to is None:
        av.date_to = range_end
    else:
        av.date_to = min(date_to, range_end)

    temporal = av.temporal
    if av.temporal == "moy":
        temporal = "month"
        temporal_order = "to_char(st, 'MM')"
        temporal_col = "to_char(st, 'Mon')"
    elif av.temporal == "dow":
        temporal = "day"
        temporal_col = "to_char(st, 'Dy')"
        temporal_order = "to_char(st, 'ID')"


    where = f"""
        WHERE
                rollup = :temporal::text
                AND
                type = :spatial::text
                AND
                st >= :date_from
                AND
                st < :date_to
                AND
                {av.where()}
    """


    if av.temporal in ['dow','moy']:
        baseq = f"""
            SELECT
                measurand as parameter,
                units as unit,
                st,
                {temporal_col} as {av.temporal},
                {temporal_order} as o,
                name,
                subtitle,
                sum(value_count) as measurement_count,
                round((sum(value_sum)/sum(value_count))::numeric, 4) as average
            FROM rollups
            LEFT JOIN groups_w_measurand USING (groups_id, measurands_id)
            {where}
            GROUP BY
                1,2,3,4,5,6,7
        """
    else:
        baseq = f"""
            SELECT
                measurand as parameter,
                units as unit,
                st,
                st::date as {av.temporal},
                st as o,
                name,
                subtitle,
                value_count as measurement_count,
                round((value_sum/value_count)::numeric, 4) as average
            FROM rollups
            LEFT JOIN groups_w_measurand USING (groups_id, measurands_id)
            {where}
        """


    q = f"""
        WITH base AS (
            {baseq}
        )
        SELECT count(*) over () as count, to_jsonb(base)-'{{o,st}}'::text[]
        FROM base
        ORDER BY o DESC
        OFFSET :offset
        LIMIT :limit;
        """
    av.temporal = temporal
    output = await db.fetchOpenAQResult(q, av.dict())

    return output
