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


class CoordinatesDict(BaseModel):
    latitude: float
    longitude: float


class DateDict(BaseModel):
    utc: str
    local: str


class MeasurementsRow(BaseModel):
    location_id: int
    location: str
    parameter: str
    date: DateDict
    unit: str
    coordiantes: CoordinatesDict
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


class ProjectParameterDetails(BaseModel):
    unit: str
    count: int
    average: float
    lastValue: float
    locations: int
    measurand: str
    lastUpdated: str
    firstUpdated: str


class ProjectsRow(BaseModel):
    id: int
    name: str
    sources: str  # TODO can't find example that isn't null
    subtitle: str
    locations: int
    parameters: List[ProjectParameterDetails]
    coordinates: List[List[float]]
    measurements: int


class OpenAQProjectsResult(OpenAQResult):
    results: List[ProjectsRow]


class SourceDetails(BaseModel):
    url: str  # TODO added as string to encompass "unused"; should this be AnyUrl instead?
    city: str
    name: str
    active: bool
    adapter: str
    country: str
    contacts: List[str]
    sourceURL: AnyUrl
    description: str


class LocationParameterDetails(BaseModel):
    id: int
    unit: str
    count: int
    average: float
    lastValue: float
    measurand: str
    lastUpdated: str
    firstUpdated: str


class LocationsRow(BaseModel):
    id: int
    city: str
    name: str
    country: str
    sources: List[SourceDetails]
    isMobile: bool
    parameters: List[LocationParameterDetails]
    sourceType: str
    coordinates: CoordinatesDict
    lastUpdated: str
    firstUpdated: str
    measurements: int


class OpenAQLocationsResult(OpenAQResult):
    results: List[LocationsRow]


class MeasurementDetails(BaseModel):
    parameter: str
    value: float
    lastUpdated: str
    unit: str


class LatestRow(BaseModel):
    location: str
    city: str
    country: str
    coordinates: CoordinatesDict
    measurements: List[MeasurementDetails]


class OpenAQLatestResult(OpenAQResult):
    results: List[LatestRow]
