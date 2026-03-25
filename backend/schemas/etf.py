from datetime import date

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field




class SEtfShort(BaseModel):
    """Return compact ETF row for top rating list."""

    secid: str
    shortname: str | None
    return_1y: float
    return_5y: float
    div_yield: float
    score: float

    model_config = ConfigDict(
        from_attributes=True,
        json_schema_extra={
            "examples": [
                {
                    "secid": "SBMX",
                    "shortname": "БПИФ Сбер - Индекс МосБиржи",
                    "return_1y": 14.21,
                    "return_5y": 89.44,
                    "div_yield": 3.1,
                    "score": 112.95,
                }
            ]
        },
    )




class SEtfDetail(BaseModel):
    """Return detailed ETF information by SECID."""

    secid: str
    shortname: str | None
    isin: str | None
    currency: str | None
    lotsize: int | None
    price_date: date
    price: float
    return_1y: float
    return_5y: float
    div_yield: float
    score: float

    model_config = ConfigDict(
        from_attributes=True,
        json_schema_extra={
            "examples": [
                {
                    "secid": "SBMX",
                    "shortname": "БПИФ Сбер - Индекс МосБиржи",
                    "isin": "RU000A0JX0J2",
                    "currency": "SUR",
                    "lotsize": 1,
                    "price_date": "2026-03-24",
                    "price": 18.44,
                    "return_1y": 14.21,
                    "return_5y": 89.44,
                    "div_yield": 3.1,
                    "score": 112.95,
                }
            ]
        },
    )




class SUpdateResponse(BaseModel):
    """Return update process summary for ETF cache refresh."""

    status: str = Field(..., examples=["ok"])
    total_etfs: int = Field(..., examples=[120])
    updated_records: int = Field(..., examples=[98])

    model_config = ConfigDict(
        from_attributes=True,
        json_schema_extra={
            "examples": [
                {
                    "status": "ok",
                    "total_etfs": 120,
                    "updated_records": 98,
                }
            ]
        },
    )




class SAllowedCheck(BaseModel):
    """Receive optional telegram username for update permission check."""

    username: str | None = Field(default=None, examples=["@vlados7529"])

    model_config = ConfigDict(
        from_attributes=True,
        json_schema_extra={
            "examples": [
                {
                    "username": "@vlados7529",
                }
            ]
        },
    )
