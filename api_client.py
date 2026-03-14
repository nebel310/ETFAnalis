import asyncio
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

import aiohttp




BASE_URL = "https://iss.moex.com/iss"
MAX_RETRIES = 3
REQUEST_TIMEOUT_SECONDS = 15



@dataclass(slots=True)
class EtfStaticData:
    """Store static ETF instrument fields fetched from MOEX."""

    secid: str
    shortname: str | None
    isin: str | None
    currency: str | None
    lotsize: int | None
    prevprice: float | None



class MoexApiClient:
    """Provide asynchronous access to MOEX ISS endpoints with retries."""

    def __init__(self) -> None:
        """Initialize the API client container state."""
        self._session: aiohttp.ClientSession | None = None


    async def __aenter__(self) -> "MoexApiClient":
        """Create an aiohttp session for the API client context."""
        timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SECONDS)
        self._session = aiohttp.ClientSession(timeout=timeout)
        return self


    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        """Close the aiohttp session when leaving the context manager."""
        if self._session is not None:
            await self._session.close()


    async def _request_json(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """Request JSON payload from MOEX with retry and backoff logic."""
        if self._session is None:
            raise RuntimeError("MoexApiClient session is not initialized.")

        request_params = {"iss.meta": "off"}
        if params:
            request_params.update(params)

        url = f"{BASE_URL}{path}"

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                async with self._session.get(url, params=request_params) as response:
                    if response.status >= 500:
                        raise aiohttp.ClientResponseError(
                            request_info=response.request_info,
                            history=response.history,
                            status=response.status,
                            message="MOEX server error",
                            headers=response.headers,
                        )
                    response.raise_for_status()
                    return await response.json()
            except (aiohttp.ClientError, asyncio.TimeoutError):
                if attempt == MAX_RETRIES:
                    raise
                await asyncio.sleep(0.5 * attempt)

        raise RuntimeError("Unexpected request retry flow.")


    @staticmethod
    def _rows_from_block(payload: dict[str, Any], block_name: str) -> list[dict[str, Any]]:
        """Convert ISS block column/data structure into dictionary rows."""
        block = payload.get(block_name, {})
        columns = block.get("columns", [])
        data_rows = block.get("data", [])

        rows: list[dict[str, Any]] = []
        for values in data_rows:
            row = {columns[index]: values[index] for index in range(min(len(columns), len(values)))}
            rows.append(row)

        return rows


    async def fetch_etf_list(self) -> list[EtfStaticData]:
        """Fetch and filter ETF securities from MOEX shares board."""
        payload = await self._request_json("/engines/stock/markets/shares/boards/TQBR/securities.json")
        rows = self._rows_from_block(payload, "securities")

        etfs: list[EtfStaticData] = []
        for row in rows:
            sectype = str(row.get("SECTYPE") or "").upper()
            instrid = str(row.get("INSTRID") or "").upper()
            if sectype != "ETF" and instrid != "ETF":
                continue

            secid = str(row.get("SECID") or "").strip().upper()
            if not secid:
                continue

            lotsize_raw = row.get("LOTSIZE")
            prevprice_raw = row.get("PREVPRICE")

            lotsize_value = int(lotsize_raw) if lotsize_raw is not None else None
            prevprice_value = float(prevprice_raw) if prevprice_raw is not None else None

            etfs.append(
                EtfStaticData(
                    secid=secid,
                    shortname=row.get("SHORTNAME"),
                    isin=row.get("ISIN"),
                    currency=row.get("CURRENCYID"),
                    lotsize=lotsize_value,
                    prevprice=prevprice_value,
                )
            )

        return etfs


    async def fetch_current_price(self, secid: str) -> tuple[date, float | None]:
        """Fetch latest tradable price for a security with fallback chain."""
        payload = await self._request_json(f"/engines/stock/markets/shares/boards/TQBR/securities/{secid}.json")

        market_rows = self._rows_from_block(payload, "marketdata")
        securities_rows = self._rows_from_block(payload, "securities")

        market_row = market_rows[0] if market_rows else {}
        security_row = securities_rows[0] if securities_rows else {}

        price_candidates = [
            market_row.get("LAST"),
            market_row.get("MARKETPRICE"),
            security_row.get("PREVPRICE"),
        ]

        price_value: float | None = None
        for candidate in price_candidates:
            if candidate is None:
                continue
            try:
                price_value = float(candidate)
                if price_value > 0:
                    break
            except (TypeError, ValueError):
                continue

        trade_date_raw = market_row.get("SYSTIME") or market_row.get("TRADEDATE")
        if isinstance(trade_date_raw, str) and trade_date_raw:
            parsed = trade_date_raw.split(" ")[0]
            price_date = date.fromisoformat(parsed)
        else:
            price_date = date.today()

        return price_date, price_value


    async def fetch_close_near_date(self, secid: str, target_date: date, fallback_days: int = 10) -> tuple[date, float] | None:
        """Fetch close price on target date or nearest previous day within fallback window."""
        for shift in range(0, fallback_days + 1):
            candidate_date = target_date - timedelta(days=shift)
            next_day = candidate_date + timedelta(days=1)

            payload = await self._request_json(
                f"/engines/stock/markets/shares/boards/TQBR/securities/{secid}/candles.json",
                params={
                    "from": candidate_date.isoformat(),
                    "till": next_day.isoformat(),
                    "interval": 24,
                },
            )

            candle_rows = self._rows_from_block(payload, "candles")
            for row in candle_rows:
                close_value = row.get("close")
                begin_raw = row.get("begin")
                if close_value is None or begin_raw is None:
                    continue

                try:
                    close = float(close_value)
                    if close <= 0:
                        continue
                    candle_date = date.fromisoformat(str(begin_raw).split(" ")[0])
                    return candle_date, close
                except (TypeError, ValueError):
                    continue

        return None


    async def fetch_dividends_last_12m(self, secid: str, since_date: date) -> list[tuple[date, float]]:
        """Fetch dividends paid since a given date for a security."""
        payload = await self._request_json(f"/securities/{secid}/dividends.json")
        dividend_rows = self._rows_from_block(payload, "dividends")

        result: list[tuple[date, float]] = []
        for row in dividend_rows:
            date_raw = row.get("valueDate") or row.get("registryClosedate")
            dividend_raw = row.get("dividend") or row.get("value")
            if not date_raw or dividend_raw is None:
                continue

            try:
                payout_date = date.fromisoformat(str(date_raw).split(" ")[0])
                payout_value = float(dividend_raw)
            except (TypeError, ValueError):
                continue

            if payout_date >= since_date and payout_value > 0:
                result.append((payout_date, payout_value))

        return result
