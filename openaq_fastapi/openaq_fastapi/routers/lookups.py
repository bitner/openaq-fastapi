import logging
from typing import List

from fastapi import APIRouter, Depends, Query
from pydantic.typing import Literal, Optional
import jq
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
    SourceName,
    Sort,
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
        country,
        city,
        sum(value_count) as count,
        count(*) as locations,
        to_char(min(first_datetime),'YYYY-MM-DD') as "firstUpdated",
        to_char(max(last_datetime), 'YYYY-MM-DD') as "lastUpdated",
        array_agg(DISTINCT measurand) as parameters
    FROM sensors_total
    LEFT JOIN sensors_first_last USING (sensors_id)
    LEFT JOIN sensors USING (sensors_id)
    LEFT JOIN sensor_systems USING (sensor_systems_id)
    LEFT JOIN sensor_nodes USING (sensor_nodes_id)
    LEFT JOIN sensor_nodes_json USING (sensor_nodes_id)
    LEFT JOIN measurands USING (measurands_id)
    WHERE
    {cities.where()}
    GROUP BY
    country,
    city
    ORDER BY "{cities.order_by}" {cities.sort}
    OFFSET :offset
    LIMIT :limit
    )
    SELECT count(*) OVER () as count, row_to_json(t) as json FROM t

    """

    output = await db.fetchOpenAQResult(q, cities.dict())

    return output


class Countries(Country, APIBase):
    order_by: Literal["country", "firstUpdated", "lastUpdated"] = Query(
        "country"
    )

@router.get("/v1/countries/{country_id}", response_model=OpenAQResult)
@router.get("/v2/countries/{country_id}", response_model=OpenAQResult)
@router.get("/v1/countries", response_model=OpenAQResult)
@router.get("/v2/countries", response_model=OpenAQResult)
async def countries_get(
    db: DB = Depends(),
    countries: Countries = Depends(Countries.depends()),
):

    q = f"""
    WITH t AS (
    SELECT
        country as code,
        name,
        sum(value_count) as count,
        count(*) as locations,
        count(distinct city) as cities,
        to_char(min(first_datetime),'YYYY-MM-DD') as "firstUpdated",
        to_char(max(last_datetime), 'YYYY-MM-DD') as "lastUpdated",
        array_agg(DISTINCT measurand) as parameters
    FROM sensors_total
    LEFT JOIN sensors_first_last USING (sensors_id)
    LEFT JOIN sensors USING (sensors_id)
    LEFT JOIN sensor_systems USING (sensor_systems_id)
    LEFT JOIN sensor_nodes USING (sensor_nodes_id)
    LEFT JOIN sensor_nodes_json USING (sensor_nodes_id)
    LEFT JOIN measurands USING (measurands_id)
    LEFT JOIN countries ON (country=iso_a2)
    WHERE
    {countries.where()}
    GROUP BY
    1,2
    ORDER BY "{countries.order_by}" {countries.sort}
    OFFSET :offset
    LIMIT :limit
    )
    SELECT count(*) OVER () as count, row_to_json(t) as json FROM t

    """

    output = await db.fetchOpenAQResult(q, countries.dict())

    return output


class Sources(SourceName, APIBase):
    order_by: Literal["sourceName", "firstUpdated", "lastUpdated"] = Query(
        "sourceName"
    )


@router.get("/v1/sources", response_model=OpenAQResult)
@router.get("/v2/sources", response_model=OpenAQResult)
async def sources_get(
    db: DB = Depends(),
    sources: Sources = Depends(Sources.depends()),
):

    q = f"""
    WITH t AS (
    SELECT
        source_name as "sourceName",
        data::jsonb as data,
        sum(value_count) as count,
        count(*) as locations,
        to_char(min(first_datetime),'YYYY-MM-DD') as "firstUpdated",
        to_char(max(last_datetime), 'YYYY-MM-DD') as "lastUpdated",
        array_agg(DISTINCT measurand) as parameters
    FROM sources
    LEFT JOIN sensor_nodes USING (source_name)
    LEFT JOIN sensor_nodes_json USING (sensor_nodes_id)
    LEFT JOIN sensor_systems USING (sensor_nodes_id)
    LEFT JOIN sensors USING (sensor_systems_id)
    LEFT JOIN sensors_total USING (sensors_id)
    LEFT JOIN sensors_first_last USING (sensors_id)
    LEFT JOIN measurands USING (measurands_id)
    WHERE
    {sources.where()}
    GROUP BY
    1,2
    ORDER BY "{sources.order_by}" {sources.sort}
    OFFSET :offset
    LIMIT :limit
    )
    SELECT count(*) OVER () as count,
        (coalesce(data, '{{}}'::jsonb) || jsonb_build_object(
            'count', count,
            'locations', locations,
            'firstUpdated', "firstUpdated",
            'lastUpdated', "lastUpdated",
            'parameters', parameters
            )) as json FROM t

    """

    output = await db.fetchOpenAQResult(q, sources.dict())

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


class Projects(Project, APIBase):
    order_by: Literal[
        "id", "name", "subtitle", "firstUpdated", "lastUpdated"
    ] = Query("id")

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
                elif isinstance(v, List):
                    wheres.append(f"{f} = ANY(:{f})")
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
                geog,
                value_count as count,
                value_sum / value_count as average,
                locations,
                measurand,
                units as unit,
                sources,
                last(last_value, last_datetime) as "lastValue",
                max(last_datetime) as "lastUpdated",
                min(first_datetime) as "firstUpdated"
            FROM
                rollups.rollups LEFT JOIN rollups.groups_view
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
            (st_asgeojson(geog)::jsonb)->'coordinates' as coordinates,
            sources,
            sum(count) as measurements,
            max(locations) as locations,
            jsonb_agg(to_jsonb(bysensor) -
            '{{id,name,subtitle,geog,sources}}'::text[]) as parameters
            FROM bysensor
            GROUP BY 1,2,3,4,5
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
    ] = Query("lastUpdated")
    sort: Optional[Sort] = Query("desc")
    isMobile: Optional[bool] = None

    def where(self):
        wheres = []

        for f, v in self:
            if v is not None:
                if f == "project":
                    if all(isinstance(x, int) for x in self.project):
                        wheres.append("groups_id = ANY(:project)")
                    else:
                        wheres.append("name = ANY(:project)")
                elif f == "location" and all(isinstance(x, int) for x in v):
                    wheres.append(" id = ANY(:location) ")
                elif f == "location":
                    wheres.append(" name = ANY(:location) ")
                elif f == "country":
                    wheres.append(" country = ANY(:country) ")
                elif f == "city":
                    wheres.append(" country = ANY(:city) ")
                elif f == "parameter":
                    wheres.append(
                        """
                        parameters @> ANY(
                            jsonb_array_query('measurand',:parameter::text[])
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

    q = f"""
        WITH t1 AS (
            SELECT *
            FROM locations_base_v2
            WHERE
            {locations.where()}
            ORDER BY "{locations.order_by}" {locations.sort}
        ),
        nodes AS (
            SELECT count(distinct id) as nodes
            FROM locations_base_v2
            WHERE
            {locations.where()}
        ),
        t2 AS (
        SELECT to_jsonb(t1) - '{{json,source_name,geog}}'::text[] as json
        FROM t1 group by t1, json
        )
        SELECT nodes as count, json
        FROM t2, nodes
        LIMIT :limit
        OFFSET :offset
        ;
        """

    output = await db.fetchOpenAQResult(q, locations.dict())

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
