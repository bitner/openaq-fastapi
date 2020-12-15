from datetime import date
from typing import Dict, List, Optional, Union
from pydantic
from pydantic.main import BaseModel BaseModel
from pydantic.typing import Any


class Meta(BaseModel):
    name: str = "openaq-api"
    license: str = "CC BY 4.0d"
    website: str = f"{settings.OPENAQ_FASTAPI_URL}/docs"
    page: int = 1
    limit: int = 100
    found: int = 0


class OpenAQResult(BaseModel):
    meta: Meta = Meta()
    results: List[Any] = []


class CountriesRow(BaseModel):
    code: str
    name: str
    locations: int
    firstUpdated: date
    lastUpdated: date
    parameters: List[str]


class OpenAQCountriesResult(OpenAQResult):
    results: List[CountriesRow]