import logging
from typing import List

import jq
from fastapi import APIRouter, Depends, Path, Query
from fastapi.responses import HTMLResponse
from markdown import markdown
from pydantic.typing import Literal, Optional

from .base import (
    DB,
    APIBase,
    City,
    Country,
    Geo,
    HasGeo,
    Location,
    Measurands,
    OpenAQResult,
    Project,
    Sort,
    SourceName,
)

logger = logging.getLogger("locations")
logger.setLevel(logging.DEBUG)

router = APIRouter()


class Cities(City, Country, APIBase):
    order_by: Literal[
        "city", "country", "firstUpdated", "lastUpdated"
    ] = Query("city")


@router.get("/v1/cities", response_model=OpenAQResult)
@router.get("/v2/cities", response_model=OpenAQResult)
async def cities_get(
    db: DB = Depends(), cities: Cities = Depends(Cities.depends())
):

    q = f"""
    WITH t AS (
    SELECT
        city,
        country,
        sum(value_count) as count,
        count(*) as locations,
        to_char(min(first_datetime),'YYYY-MM-DD') as "firstUpdated",
        to_char(max(last_datetime), 'YYYY-MM-DD') as "lastUpdated",
        array_agg(DISTINCT measurand) as parameters
    FROM
    sensor_nodes
    LEFT JOIN sensor_systems USING (sensor_nodes_id)
    LEFT JOIN sensors USING (sensor_systems_id)
    LEFT JOIN rollups USING (sensors_id, measurands_id)
    LEFT JOIN groups_view USING (groups_id, measurands_id)
    WHERE rollup='total' AND groups_view.type='node'
    AND {cities.where()}
    GROUP BY
    1,2
    ORDER BY "{cities.order_by}" {cities.sort}
    OFFSET :offset
    LIMIT :limit
    )
    SELECT count(*) OVER () as count,
        to_jsonb(t) FROM t;
    """

    output = await db.fetchOpenAQResult(q, cities.dict())

    return output


class Countries(Country, APIBase):
    order_by: Literal[
        "country", "firstUpdated", "lastUpdated", "count", "locations"
    ] = Query("country")

    def where(self):
        wheres = []
        for f, v in self:
            if v is not None:
                logger.debug(f" setting where for {f} {v} ")
                if f == "country":
                    wheres.append(" cl.iso = ANY(:country) ")
        if len(wheres) > 0:
            return (" AND ").join(wheres)
        return " TRUE "


@router.get("/v1/countries/{country_id}", response_model=OpenAQResult)
@router.get("/v2/countries/{country_id}", response_model=OpenAQResult)
@router.get("/v1/countries", response_model=OpenAQResult)
@router.get("/v2/countries", response_model=OpenAQResult)
async def countries_get(
    db: DB = Depends(),
    countries: Countries = Depends(Countries.depends()),
):
    order_by = countries.order_by
    if countries.order_by == "lastUpdated":
        order_by = "last_datetime"
    elif countries.order_by == "firstUpdated":
        order_by = "first_datetime"
    elif countries.order_by == "country":
        order_by = "iso"
    elif countries.order_by == "count":
        order_by = "sum(value_count)"
    elif countries.order_by == "locations":
        order_by = "count(*)"

    q = f"""
    WITH t AS (
    SELECT
        cl.iso as code,
        cl.name,
        cl.bbox,
        cities,
        sum(value_count) as count,
        count(*) as locations,
        min(first_datetime) as "firstUpdated",
        max(last_datetime) as "lastUpdated",
        array_agg(DISTINCT measurand) as parameters
    FROM countries_lookup cl
    JOIN groups_view gv ON (gv.name=cl.iso)
    JOIN rollups r USING (groups_id, measurands_id)
    JOIN LATERAL (
        SELECT count(DISTINCT city) as cities FROM sensor_nodes
        WHERE country=cl.iso
        ) cities ON TRUE
    WHERE r.rollup='total' AND gv.type='country'
    AND
    {countries.where()}
    GROUP BY
    1,2,3,4
    ORDER BY {order_by} {countries.sort}
    OFFSET :offset
    LIMIT :limit
    )
    SELECT count(*) OVER () as count, to_jsonb(t) as json FROM t

    """

    output = await db.fetchOpenAQResult(q, countries.dict(exclude_unset=True))

    return output


class Sources(SourceName, APIBase):
    order_by: Literal["sourceName", "firstUpdated", "lastUpdated"] = Query(
        "sourceName"
    )

    def where(self):
        wheres = []
        for f, v in self:
            if v is not None:
                logger.debug(f" setting where for {f} {v} ")
                if f == "sourceId":
                    wheres.append(" sources_id = ANY(:source_id) ")
                elif f == "sourceName":
                    wheres.append(" sources.name = ANY(:source_name) ")
                elif f == "sourceSlug":
                    wheres.append(" sources.slug = ANY(:source_slug) ")
        if len(wheres) > 0:
            return (" AND ").join(wheres)
        return " TRUE "


@router.get("/v1/sources", response_model=OpenAQResult)
@router.get("/v2/sources", response_model=OpenAQResult)
async def sources_get(
    db: DB = Depends(),
    sources: Sources = Depends(Sources.depends()),
):
    qparams = sources.dict(exclude_unset=True)
    qparams.update(
        {
            "source_id": sources.sourceId,
            "source_name": sources.sourceName,
            "source_slug": sources.sourceSlug,
        }
    )

    q = f"""
    WITH t AS (
    SELECT
        sources_id as "sourceId",
        slug as "sourceSlug",
        sources.name as "sourceName",
        sources.metadata as data,
        case when readme is not null then
        '/v2/sources/readmes/' || slug
        else null end as readme,
        sum(value_count) as count,
        count(*) as locations,
        to_char(min(first_datetime),'YYYY-MM-DD') as "firstUpdated",
        to_char(max(last_datetime), 'YYYY-MM-DD') as "lastUpdated",
        array_agg(DISTINCT measurand) as parameters
    FROM sources
    LEFT JOIN sensor_nodes_sources USING (sources_id)
    LEFT JOIN sensor_systems USING (sensor_nodes_id)
    LEFT JOIN sensors USING (sensor_systems_id)
    LEFT JOIN rollups USING (sensors_id, measurands_id)
    LEFT JOIN groups_view USING (groups_id, measurands_id)
    WHERE rollup='total' AND groups_view.type='node'
    AND {sources.where()}
    GROUP BY
    1,2,3,4,5
    ORDER BY "{sources.order_by}" {sources.sort}
    OFFSET :offset
    LIMIT :limit
    )
    SELECT count(*) OVER () as count,
        to_jsonb(t) FROM t;
    """

    output = await db.fetchOpenAQResult(q, qparams)

    return output


class Parameters(SourceName, APIBase):
    order_by: Literal["id", "name", "preferredUnit"] = Query("id")


@router.get("/v1/parameters", response_model=OpenAQResult)
@router.get("/v2/parameters", response_model=OpenAQResult)
async def parameters_get(
    db: DB = Depends(),
    parameters: Parameters = Depends(Parameters.depends()),
):

    q = f"""
    WITH t AS (
    SELECT
        measurands_id as id,
        upper(measurand) as name,
        upper(measurand) as description,
        units as "preferredUnit"
    FROM measurands
    ORDER BY "{parameters.order_by}" {parameters.sort}
    )
    SELECT count(*) OVER () as count, row_to_json(t) as json FROM t
    LIMIT :limit
    OFFSET :offset
    """

    output = await db.fetchOpenAQResult(q, parameters.dict())

    return output


class Projects(Project, Measurands, APIBase):
    order_by: Literal[
        "id", "name", "subtitle", "firstUpdated", "lastUpdated"
    ] = Query("lastUpdated")

    def where(self):
        wheres = []
        for f, v in self:
            if v is not None:
                logger.debug(f" setting where for {f} {v} ")
                if f == "project" and all(isinstance(x, int) for x in v):
                    logger.debug(" using int id")
                    wheres.append(" groups_id = ANY(:project) ")
                elif f == "project":
                    wheres.append(" name = ANY(:project) ")
                elif f == "units":
                    wheres.append(" units = ANY(:units) ")

                elif f == "parameter":
                    if all(isinstance(x, int) for x in v):
                        wheres.append(
                            """
                            measurands_id = ANY (:parameter)
                            """
                        )
                    else:
                        wheres.append(
                            """
                            measurand = ANY (:parameter)
                            """
                        )

                # elif isinstance(v, List):
                #    wheres.append(f"{f} = ANY(:{f})")

        if len(wheres) > 0:
            return (" AND ").join(wheres)
        return " TRUE "


@router.get("/v2/projects/{project_id}", response_model=OpenAQResult)
@router.get("/v2/projects", response_model=OpenAQResult)
async def projects_get(
    db: DB = Depends(),
    projects: Projects = Depends(Projects.depends()),
):

    q = f"""
        WITH bysensor AS (
            SELECT
                groups_id as "id",
                name,
                subtitle,
                --geog,
                value_count as count,
                value_sum / value_count as average,
                locations,
                measurand as parameter,
                units as unit,
                measurands_id as parameterId,
                nodes_from_sensors(sensors_id_arr) as location_ids,
                last(last_value, last_datetime) as "lastValue",
                max(last_datetime) as "lastUpdated",
                min(first_datetime) as "firstUpdated",
                min(minx) as minx,
                min(miny) as miny,
                max(maxx) as maxx,
                max(maxy) as maxy
            FROM
                rollups LEFT JOIN groups_view
                USING (groups_id, measurands_id)
            WHERE
                type='source' AND rollup='total'
                AND {projects.where()}
            GROUP BY 1,2,3,4,5,6,7,8,9,10
            ORDER BY "{projects.order_by}" {projects.sort}

        )
        , overall as (
        SELECT
            "id",
            name,
            subtitle,
            ARRAY[minx, miny, maxx, maxy] as bbox,
            --array_agg(distinct sources),
            sum(count) as measurements,
            max(locations) as locations,
            max("lastUpdated") as "lastUpdated",
            min("firstUpdated") as "firstUpdated",
            array_merge_agg(location_ids) as locationIds,
            jsonb_agg(to_jsonb(bysensor) -
            '{{
                id,
                name,
                subtitle,
                geog,
                sources,
                location_ids,
                minx,
                miny,
                maxx,
                maxy
            }}'::text[]) as parameters
            FROM bysensor
            GROUP BY 1,2,3,4
        )
        select count(*) OVER () as count, to_jsonb(overall) as json
        from overall
        LIMIT :limit
        OFFSET :offset
            ;
    """

    output = await db.fetchOpenAQResult(q, projects.dict())

    return output


class Locations(Location, City, Country, Geo, Measurands, HasGeo, APIBase):
    order_by: Literal[
        "city",
        "country",
        "location",
        "sourceName",
        "firstUpdated",
        "lastUpdated",
        "count",
    ] = Query("lastUpdated")
    sort: Optional[Sort] = Query("desc")
    isMobile: Optional[bool] = None
    sourceName: Optional[List[str]] = None
    modelName: Optional[List[str]] = None
    manufacturerName: Optional[List[str]] = None

    def where(self):
        wheres = []

        for f, v in self:
            if v is not None:
                if f == "project":
                    if all(isinstance(x, int) for x in self.project):
                        wheres.append("groups_id = ANY(:project)")
                    else:
                        wheres.append("name = ANY(:project)")
                elif f == "location":
                    if all(isinstance(x, int) for x in v):
                        wheres.append(" id = ANY(:location) ")
                    else:
                        wheres.append(" name = ANY(:location) ")
                elif f == "country":
                    wheres.append(" country = ANY(:country) ")
                elif f == "city":
                    wheres.append(" city = ANY(:city) ")
                elif f == "parameter":
                    if all(isinstance(x, int) for x in v):
                        wheres.append(
                            """
                            parameters @> ANY(
                                jsonb_array_query('parameterId',:parameter::int[])
                                )
                            """
                        )
                    else:
                        wheres.append(
                            """
                            parameters @> ANY(
                                jsonb_array_query('parameter',:parameter::text[])
                                )
                            """
                        )
                elif f == "sourceName":
                    wheres.append(
                        """
                        sources @> ANY(
                            jsonb_array_query('name',:sourcename::text[])
                            ||
                            jsonb_array_query('id',:sourcename::text[])
                            )
                        """
                    )
                elif f == "modelName":
                    wheres.append(
                        """
                        manufacturers @> ANY(
                            jsonb_array_query('modelName',:model_name::text[])
                            )
                        """
                    )
                elif f == "manufacturerName":
                    wheres.append(
                        """
                        manufacturers @> ANY(
                            jsonb_array_query('manufacturerName',:manufacturer_name::text[])
                            )
                        """
                    )
                elif f == "isMobile":
                    wheres.append(f' "isMobile" = {bool(v)} ')
                # elif isinstance(v, List):
                #     wheres.append(f"{f} = ANY(:{f})")
        if len(wheres) > 0:
            return (" AND ").join(wheres)
        return " TRUE "


@router.get("/v2/locations/{location_id}", response_model=OpenAQResult)
@router.get("/v2/locations", response_model=OpenAQResult)
async def locations_get(
    db: DB = Depends(),
    locations: Locations = Depends(Locations.depends()),
):
    order_by = locations.order_by
    if order_by == "location":
        order_by = "name"
    if order_by == "count":
        order_by = "measurements"

    qparams = locations.dict(exclude_none=True)
    qparams["sourcename"] = locations.sourceName
    qparams["model_name"] = locations.modelName
    qparams["manufacturer_name"] = locations.manufacturerName

    q = f"""
        WITH t1 AS (
            SELECT *, row_number() over () as row
            FROM locations_base_v2
            WHERE
            {locations.where()}
            ORDER BY "{order_by}" {locations.sort}
            LIMIT :limit
            OFFSET :offset
        ),
        nodes AS (
            SELECT count(distinct id) as nodes
            FROM locations_base_v2
            WHERE
            {locations.where()}
        ),
        t2 AS (
        SELECT
        row,
        jsonb_strip_nulls(
            to_jsonb(t1) - '{{json,source_name,geog, row}}'::text[]
        ) as json
        FROM t1 group by row, t1, json
        )
        SELECT nodes as count, json
        FROM t2, nodes
        ORDER BY row

        ;
        """

    output = await db.fetchOpenAQResult(q, qparams)

    return output


@router.get("/v1/latest/{location_id}", response_model=OpenAQResult)
@router.get("/v2/latest/{location_id}", response_model=OpenAQResult)
@router.get("/v1/latest", response_model=OpenAQResult)
@router.get("/v2/latest", response_model=OpenAQResult)
async def latest_get(
    db: DB = Depends(),
    locations: Locations = Depends(Locations.depends()),
):
    data = await locations_get(db, locations)
    meta = data.meta
    res = data.results
    if len(res) == 0:
        return data

    latest_jq = jq.compile(
        """
        .[] |
            {
                location: .name,
                city: .city,
                country: .country,
                coordinates: .coordinates,
                measurements: [
                    .parameters[] | {
                        parameter: .measurand,
                        value: .lastValue,
                        lastUpdated: .lastUpdated,
                        unit: .unit
                    }
                ]
            }

        """
    )

    ret = latest_jq.input(res).all()
    return OpenAQResult(meta=meta, results=ret)


@router.get("/v1/locations/{location_id}", response_model=OpenAQResult)
@router.get("/v1/locations", response_model=OpenAQResult)
async def locationsv1_get(
    db: DB = Depends(),
    locations: Locations = Depends(Locations.depends()),
):
    data = await locations_get(db, locations)
    meta = data.meta
    res = data.results

    latest_jq = jq.compile(
        """
        .[] |
            {
                id: .id,
                country: .country,
                city: .city,
                location: .name,
                soureName: .source_name,
                sourceType: .sources[0].name,
                coordinates: .coordinates,
                firstUpdated: .firstUpdated,
                lastUpdated: .lastUpdated,
                parameters : [ .parameters[].measurand ],
                countsByMeasurement: [
                    .parameters[] | {
                        parameter: .measurand,
                        count: .count
                    }
                ],
                count: .parameters| map(.count) | add
            }

        """
    )

    ret = latest_jq.input(res).all()
    return OpenAQResult(meta=meta, results=ret)


@router.get("/v2/sources/readme/{slug}")
async def readme_get(
    db: DB = Depends(),
    slug: str = Path(...),
):
    q = """
        SELECT readme FROM sources WHERE slug=:slug
        """

    readme = await db.fetchval(q, {"slug": slug})
    readme = str.replace(readme, "\\", "")

    return HTMLResponse(content=markdown(readme), status_code=200)
