from datetime import date
from typing import Dict, List, Optional, Union
from pydantic import AnyUrl
from pydantic.main import BaseModel BaseModel
from pydantic.typing import Any


class Meta(BaseModel):
    name: str = "openaq-api"
    license: str = "CC BY 4.0d"
    website: str = f"{settings.OPENAQ_FASTAPI_URL}/docs"
    page: int = 1
    limit: int = 100
    found: int = 0
    # TODO do these also belong here?
    # offset: int
    # sort: str


class OpenAQResult(BaseModel):
    meta: Meta = Meta()
    results: List[Any] = []


class MeasurementsRow(BaseModel):
    location_id: int
    location: str
    parameter: str
    date: Dict[str, str]  # datetime string?
    unit: str
    coordiantes: Dict[str, float]
    country: str
    city: str
    isMobile: bool


class OpenAQMeasurementsResult(OpenAQResult):
    results: List[MeasurementsRow]


class AveragesRow(BaseModel):
    id: int
    name: str
    unit: str
    year: str
    average: float
    subtitle: str
    parameter: str
    measurement_count: int


class OpenAQAveragesResult(OpenAQResult):
    results: List[AveragesRow]


class CitiesRow(BaseModel):
    country: str
    city: str
    count: int
    locations: int
    firstUpdated: str
    lastUpdated: str
    parameters: List[str]


class OpenAQCitiesResult(OpenAQResult):
    results: List[CitiesRow]


class CountriesRow(BaseModel):
    code: str
    name: str
    locations: int
    firstUpdated: date
    lastUpdated: date
    parameters: List[str]


class OpenAQCountriesResult(OpenAQResult):
    results: List[CountriesRow]


class SourcesRow(BaseModel):
    url: AnyUrl
    name: str
    count: int
    active: bool
    adapter: str
    country: str
    contacts: List[str]
    locations: int
    sourceURL: AnyUrl
    parameters: List[str]
    description: str
    lastUpdated: str
    firstUpdated: str


class OpenAQSourcesResult(OpenAQResult):
    results: List[SourcesRow]


class ParametersRow(BaseModel):
    id: int
    name: str
    description: str
    preferredUnit: str


class OpenAQParametersResult(OpenAQResult):
    results: List[ParametersRow]


class ProjectsRow(BaseModel):
    id: int
    name: str
    sources: str  #TODO can't find example that isn't null
    subtitle: str
    locations: int
    # parameters: List[Dict[]]  # TODO can you do complex dicts like this?
    coordinates: List[List[float]]
    measurements: int


class OpenAQProjectsResult(OpenAQResult):
    results: List[ProjectsRow]


class LocationsRow(BaseModel):
    id: int
    city: str
    name: str
    country: str
    # sources: List[Dict[]]  # TODO can you do complex dicts like this?
    isMobile: bool
    # parameters: List[Dict[]]  # TODO can you do complex dicts like this?
    sourceType: str
    coordinates: Dict[str, float]
    lastUpdated: str
    firstUpdated: str
    measurements: int


class OpenAQLocationsResult(OpenAQResult):
    results: List[LocationsRow]


class LatestRow(BaseModel):
    location: str
    city: str
    country: str
    coordinates: Dict[str, float]
    # measurements: List[Dict[]]  # TODO can you do complex dicts like this?


class OpenAQLatestResult(OpenAQResult):
    results: List[LatestRow]
