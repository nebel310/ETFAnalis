import asyncio
import os

from dataclasses import dataclass
from datetime import date
from datetime import timedelta
from typing import Any

import aiohttp




DEFAULT_MOEX_BASE_URL = "https://iss.moex.com/iss"
MAX_RETRIES = 3
REQUEST_TIMEOUT_SECONDS = 15
ETF_BOARDS = ("TQTF", "TQIF", "TQBR")




@dataclass(slots=True)
class EtfStaticData:
    """Store static ETF fields received from MOEX."""

    secid: str
    shortname: str | None
    isin: str | None
    currency: str | None
    lotsize: int | None
    prevprice: float | None
    board: str




def calculate_return_percent(current_price: float, historical_price: float | None) -> float:
    """Calculate percent return between historical and current prices."""
    if historical_price is None or historical_price <= 0 or current_price <= 0:
        return 0.0

    return ((current_price / historical_price) - 1) * 100




def calculate_dividend_yield_percent(total_dividends: float, current_price: float) -> float:
    """Calculate dividend yield percent from annual payouts and current price."""
    if current_price <= 0:
        return 0.0

    return (total_dividends / current_price) * 100




def calculate_score(div_yield: float, return_1y: float, return_5y: float) -> float:
    """Calculate custom ETF ranking score value."""
    return (div_yield * 3) + return_1y + return_5y




class MoexApiClient:
    """Provide asynchronous MOEX ISS access with retries and fallbacks."""

    def __init__(self) -> None:
        """Initialize MOEX client internal state."""
        self._session: aiohttp.ClientSession | None = None
        self._base_url = os.getenv("MOEX_BASE_URL", DEFAULT_MOEX_BASE_URL).rstrip("/")


    async def __aenter__(self) -> "MoexApiClient":
        """Open aiohttp session for request execution."""
        timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SECONDS)
        self._session = aiohttp.ClientSession(timeout=timeout)
        return self


    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        """Close aiohttp session on context exit."""
        if self._session is not None:
            await self._session.close()


    async def _request_json(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """Fetch JSON from MOEX endpoint with retry and backoff."""
        if self._session is None:
            raise RuntimeError("MoexApiClient session is not initialized.")

        request_params = {"iss.meta": "off"}
        if params:
            request_params.update(params)

        url = f"{self._base_url}{path}"

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

        raise RuntimeError("Unexpected MOEX retry flow.")


    @staticmethod
    def _rows_from_block(payload: dict[str, Any], block_name: str) -> list[dict[str, Any]]:
        """Map ISS block data and columns into dictionary rows."""
        block = payload.get(block_name, {})
        columns = block.get("columns", [])
        data_rows = block.get("data", [])

        rows: list[dict[str, Any]] = []
        for values in data_rows:
            row = {columns[index]: values[index] for index in range(min(len(columns), len(values)))}
            rows.append(row)

        return rows


    @staticmethod
    def _to_int(value: Any) -> int | None:
        """Parse integer from raw input when possible."""
        if value is None:
            return None

        try:
            return int(value)
        except (TypeError, ValueError):
            return None


    @staticmethod
    def _to_float(value: Any) -> float | None:
        """Parse float from raw input when possible."""
        if value is None:
            return None

        try:
            return float(value)
        except (TypeError, ValueError):
            return None


    @staticmethod
    def _row_looks_like_etf(row: dict[str, Any]) -> bool:
        """Detect whether securities row belongs to ETF instrument."""
        keywords = (
            str(row.get("SECTYPE") or "").upper(),
            str(row.get("INSTRID") or "").upper(),
            str(row.get("GROUP") or "").upper(),
            str(row.get("SECNAME") or "").upper(),
            str(row.get("SHORTNAME") or "").upper(),
        )

        if any(value == "ETF" for value in keywords[:2]):
            return True

        text_blob = " ".join(keywords)
        return "ETF" in text_blob or "БПИФ" in text_blob


    async def _fetch_board_etfs(self, board: str) -> list[EtfStaticData]:
        """Fetch ETF-like securities from one MOEX board."""
        payload = await self._request_json(f"/engines/stock/markets/shares/boards/{board}/securities.json")
        rows = self._rows_from_block(payload, "securities")

        mapped: list[EtfStaticData] = []
        for row in rows:
            secid = str(row.get("SECID") or "").strip().upper()
            if not secid:
                continue

            if board == "TQBR" and not self._row_looks_like_etf(row):
                continue

            mapped.append(
                EtfStaticData(
                    secid=secid,
                    shortname=row.get("SHORTNAME"),
                    isin=row.get("ISIN"),
                    currency=row.get("CURRENCYID"),
                    lotsize=self._to_int(row.get("LOTSIZE")),
                    prevprice=self._to_float(row.get("PREVPRICE")),
                    board=board,
                )
            )

        return mapped


    async def fetch_etf_list(self) -> list[EtfStaticData]:
        """Fetch ETF securities list from fallback board sequence."""
        unique_by_secid: dict[str, EtfStaticData] = {}

        for board in ETF_BOARDS:
            board_items = await self._fetch_board_etfs(board)
            for item in board_items:
                if item.secid in unique_by_secid:
                    continue

                unique_by_secid[item.secid] = item

        return list(unique_by_secid.values())


    async def fetch_current_price(self, secid: str, board: str) -> tuple[date, float | None]:
        """Fetch current trade price with fallback to previous price."""
        payload = await self._request_json(f"/engines/stock/markets/shares/boards/{board}/securities/{secid}.json")

        market_rows = self._rows_from_block(payload, "marketdata")
        securities_rows = self._rows_from_block(payload, "securities")

        market_row = market_rows[0] if market_rows else {}
        security_row = securities_rows[0] if securities_rows else {}

        price_candidates = (
            market_row.get("LAST"),
            market_row.get("MARKETPRICE"),
            security_row.get("PREVPRICE"),
        )

        parsed_price: float | None = None
        for candidate in price_candidates:
            value = self._to_float(candidate)
            if value is None or value <= 0:
                continue

            parsed_price = value
            break

        trade_date_raw = market_row.get("SYSTIME") or market_row.get("TRADEDATE")
        if isinstance(trade_date_raw, str) and trade_date_raw:
            trade_date = date.fromisoformat(trade_date_raw.split(" ")[0])
        else:
            trade_date = date.today()

        return trade_date, parsed_price


    async def fetch_close_near_date(
        self,
        secid: str,
        board: str,
        target_date: date,
        fallback_days: int = 10,
    ) -> tuple[date, float] | None:
        """Fetch close at target date or nearest previous date within fallback window."""
        for shift in range(0, fallback_days + 1):
            candidate_date = target_date - timedelta(days=shift)
            next_day = candidate_date + timedelta(days=1)

            payload = await self._request_json(
                f"/engines/stock/markets/shares/boards/{board}/securities/{secid}/candles.json",
                params={
                    "from": candidate_date.isoformat(),
                    "till": next_day.isoformat(),
                    "interval": 24,
                },
            )

            candle_rows = self._rows_from_block(payload, "candles")
            for row in candle_rows:
                close_raw = row.get("close")
                begin_raw = row.get("begin")
                if close_raw is None or begin_raw is None:
                    continue

                close_value = self._to_float(close_raw)
                if close_value is None or close_value <= 0:
                    continue

                try:
                    candle_date = date.fromisoformat(str(begin_raw).split(" ")[0])
                except ValueError:
                    continue

                return candle_date, close_value

        return None


    async def fetch_dividends_last_12m(self, secid: str, since_date: date) -> list[tuple[date, float]]:
        """Fetch dividend payouts since specified date."""
        payload = await self._request_json(f"/securities/{secid}/dividends.json")
        dividend_rows = self._rows_from_block(payload, "dividends")

        result: list[tuple[date, float]] = []
        for row in dividend_rows:
            date_raw = row.get("valueDate") or row.get("registryClosedate")
            value_raw = row.get("dividend") or row.get("value")
            if date_raw is None or value_raw is None:
                continue

            try:
                payout_date = date.fromisoformat(str(date_raw).split(" ")[0])
            except ValueError:
                continue

            payout_value = self._to_float(value_raw)
            if payout_value is None or payout_value <= 0:
                continue

            if payout_date >= since_date:
                result.append((payout_date, payout_value))

        return result
