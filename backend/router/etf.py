import asyncio
import os
import secrets

import aiohttp

from fastapi import APIRouter
from fastapi import Header
from fastapi import HTTPException
from sqlalchemy.exc import SQLAlchemyError

from repositories.etf import EtfRepository
from schemas.etf import SAllowedCheck
from schemas.etf import SEtfDetail
from schemas.etf import SEtfShort
from schemas.etf import SUpdateResponse




router = APIRouter(
    prefix="/etf",
    tags=["ETF"],
)




@router.get("/top", response_model=list[SEtfShort])
async def get_top_etfs(limit: int = 10) -> list[SEtfShort]:
    """Return top ETF rating from cached metrics ordered by score."""
    try:
        safe_limit = max(1, min(limit, 100))
        return await EtfRepository.get_top_metrics(safe_limit)

    except SQLAlchemyError:
        raise HTTPException(status_code=500, detail="Database error while loading top ETFs")




@router.get("/info/{secid}", response_model=SEtfDetail)
async def get_etf_info(secid: str) -> SEtfDetail:
    """Return ETF details and metrics; data is served strictly from cache."""
    try:
        item = await EtfRepository.get_info_by_secid(secid)
        if item is None:
            raise HTTPException(status_code=404, detail="ETF not found in cache")

        return item

    except SQLAlchemyError:
        raise HTTPException(status_code=500, detail="Database error while loading ETF details")




@router.post("/update", response_model=SUpdateResponse)
async def update_etf_cache(
    payload: SAllowedCheck | None = None,
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> SUpdateResponse:
    """Run full MOEX refresh only for valid API key or allowlisted Telegram username."""
    expected_key = os.getenv("BOT_API_KEY", "").strip()

    try:
        is_authorized = False

        if expected_key and x_api_key and secrets.compare_digest(x_api_key, expected_key):
            is_authorized = True

        if not is_authorized and payload is not None and payload.username:
            is_authorized = await EtfRepository.is_user_allowed(payload.username)

        if not is_authorized:
            raise HTTPException(status_code=403, detail="Update is allowed only for trusted bot source")

        total_etfs, updated_records = await EtfRepository.update_etf_data()

        return SUpdateResponse(
            status="ok",
            total_etfs=total_etfs,
            updated_records=updated_records,
        )

    except HTTPException:
        raise

    except SQLAlchemyError:
        raise HTTPException(status_code=500, detail="Database error while updating ETF cache")

    except (aiohttp.ClientError, asyncio.TimeoutError):
        raise HTTPException(status_code=502, detail="MOEX service is temporarily unavailable")
