from datetime import date, timedelta

from sqlalchemy import Select, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from api_client import EtfStaticData, MoexApiClient
from models import AllowedUserModel, DividendModel, EtfMetricModel, EtfModel, PriceModel




def calculate_return_percent(current_price: float, historical_price: float | None) -> float:
    """Calculate percent return from historical price to current price."""
    if historical_price is None or historical_price <= 0 or current_price <= 0:
        return 0.0

    return ((current_price / historical_price) - 1) * 100



def calculate_dividend_yield_percent(total_dividends: float, current_price: float) -> float:
    """Calculate dividend yield percent from trailing payouts and current price."""
    if current_price <= 0:
        return 0.0

    return (total_dividends / current_price) * 100



def calculate_score(div_yield: float, return_1y: float, return_5y: float) -> float:
    """Calculate ETF ranking score based on returns and dividend yield."""
    return (div_yield * 3) + return_1y + return_5y



async def _upsert_etf(session: AsyncSession, etf: EtfStaticData) -> None:
    """Insert or update static ETF data in cache."""
    statement = insert(EtfModel).values(
        secid=etf.secid,
        shortname=etf.shortname,
        isin=etf.isin,
        currency=etf.currency,
        lotsize=etf.lotsize,
        prevprice=etf.prevprice,
    )

    statement = statement.on_conflict_do_update(
        index_elements=[EtfModel.secid],
        set_={
            "shortname": etf.shortname,
            "isin": etf.isin,
            "currency": etf.currency,
            "lotsize": etf.lotsize,
            "prevprice": etf.prevprice,
        },
    )

    await session.execute(statement)



async def _upsert_price(session: AsyncSession, secid: str, price_date: date, close: float) -> None:
    """Insert or update historical daily close price for an ETF."""
    statement = insert(PriceModel).values(secid=secid, date=price_date, close=close)
    statement = statement.on_conflict_do_update(
        index_elements=[PriceModel.secid, PriceModel.date],
        set_={"close": close},
    )
    await session.execute(statement)



async def _upsert_dividend(session: AsyncSession, secid: str, payout_date: date, value: float) -> None:
    """Insert or update dividend payout row in cache."""
    statement = insert(DividendModel).values(secid=secid, date=payout_date, value=value)
    statement = statement.on_conflict_do_update(
        index_elements=[DividendModel.secid, DividendModel.date],
        set_={"value": value},
    )
    await session.execute(statement)



async def _upsert_metric(
    session: AsyncSession,
    secid: str,
    price_date: date,
    price: float,
    return_1y: float,
    return_5y: float,
    div_yield: float,
    score: float,
) -> None:
    """Insert or update latest calculated metric row for an ETF."""
    statement = insert(EtfMetricModel).values(
        secid=secid,
        price_date=price_date,
        price=price,
        return_1y=return_1y,
        return_5y=return_5y,
        div_yield=div_yield,
        score=score,
    )

    statement = statement.on_conflict_do_update(
        index_elements=[EtfMetricModel.secid],
        set_={
            "price_date": price_date,
            "price": price,
            "return_1y": return_1y,
            "return_5y": return_5y,
            "div_yield": div_yield,
            "score": score,
        },
    )

    await session.execute(statement)



async def refresh_etf_cache(session: AsyncSession) -> tuple[int, int]:
    """Refresh ETF static data, price history, dividends, and metrics from MOEX."""
    today = date.today()
    one_year_date = today - timedelta(days=365)
    five_year_date = today - timedelta(days=365 * 5)

    updated_metrics = 0
    total_etfs = 0

    try:
        async with MoexApiClient() as api_client:
            etf_list = await api_client.fetch_etf_list()
            total_etfs = len(etf_list)

            for etf in etf_list:
                await _upsert_etf(session, etf)

                price_date, current_price = await api_client.fetch_current_price(etf.secid)
                if current_price is None or current_price <= 0:
                    if etf.prevprice is None or etf.prevprice <= 0:
                        continue
                    current_price = etf.prevprice
                    price_date = today

                await _upsert_price(session, etf.secid, price_date, current_price)

                close_1y_data = await api_client.fetch_close_near_date(etf.secid, one_year_date)
                close_5y_data = await api_client.fetch_close_near_date(etf.secid, five_year_date)

                historical_1y = close_1y_data[1] if close_1y_data else None
                historical_5y = close_5y_data[1] if close_5y_data else None

                if close_1y_data is not None:
                    await _upsert_price(session, etf.secid, close_1y_data[0], close_1y_data[1])

                if close_5y_data is not None:
                    await _upsert_price(session, etf.secid, close_5y_data[0], close_5y_data[1])

                dividends = await api_client.fetch_dividends_last_12m(etf.secid, one_year_date)

                total_dividends = 0.0
                for payout_date, payout_value in dividends:
                    total_dividends += payout_value
                    await _upsert_dividend(session, etf.secid, payout_date, payout_value)

                return_1y = calculate_return_percent(current_price, historical_1y)
                return_5y = calculate_return_percent(current_price, historical_5y)
                div_yield = calculate_dividend_yield_percent(total_dividends, current_price)
                score = calculate_score(div_yield, return_1y, return_5y)

                await _upsert_metric(
                    session=session,
                    secid=etf.secid,
                    price_date=price_date,
                    price=current_price,
                    return_1y=return_1y,
                    return_5y=return_5y,
                    div_yield=div_yield,
                    score=score,
                )
                updated_metrics += 1

        await session.commit()
    except Exception:
        await session.rollback()
        raise

    return total_etfs, updated_metrics



def get_top_metrics_query(limit: int) -> Select[tuple[EtfMetricModel, EtfModel]]:
    """Build query to fetch top ETFs ordered by score descending."""
    return (
        select(EtfMetricModel, EtfModel)
        .join(EtfModel, EtfModel.secid == EtfMetricModel.secid)
        .order_by(EtfMetricModel.score.desc())
        .limit(limit)
    )



def get_info_query(secid: str) -> Select[tuple[EtfMetricModel, EtfModel]]:
    """Build query to fetch one ETF metric row with static info by SECID."""
    return (
        select(EtfMetricModel, EtfModel)
        .join(EtfModel, EtfModel.secid == EtfMetricModel.secid)
        .where(EtfMetricModel.secid == secid.upper())
        .limit(1)
    )



async def is_user_allowed(session: AsyncSession, username: str | None) -> bool:
    """Check whether a Telegram username can execute update command."""
    if not username:
        return False

    normalized = username.strip().lower()
    if not normalized:
        return False

    if not normalized.startswith("@"):
        normalized = f"@{normalized}"

    statement = select(AllowedUserModel.id).where(AllowedUserModel.username.ilike(normalized)).limit(1)
    result = await session.execute(statement)
    return result.scalar_one_or_none() is not None



def format_percent(value: float) -> str:
    """Format float value as percentage with two decimals."""
    return f"{value:.2f}%"



def format_price(value: float) -> str:
    """Format float value as price with four decimals for consistency."""
    return f"{value:.4f}"
