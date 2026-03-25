from datetime import date
from datetime import timedelta

from sqlalchemy import Select
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from database import new_session
from models.etf import AllowedUserModel
from models.etf import DividendModel
from models.etf import EtfMetricModel
from models.etf import EtfModel
from models.etf import PriceModel
from schemas.etf import SEtfDetail
from schemas.etf import SEtfShort
from utils.moex_client import EtfStaticData
from utils.moex_client import MoexApiClient
from utils.moex_client import calculate_dividend_yield_percent
from utils.moex_client import calculate_return_percent
from utils.moex_client import calculate_score




class EtfRepository:
    """Provide persistence and aggregation operations for ETF data."""

    @classmethod
    async def get_top_metrics(cls, limit: int) -> list[SEtfShort]:
        """Return top ETF metrics sorted by score descending."""
        async with new_session() as session:
            statement = (
                select(EtfMetricModel, EtfModel)
                .join(EtfModel, EtfModel.secid == EtfMetricModel.secid)
                .order_by(EtfMetricModel.score.desc())
                .limit(limit)
            )
            result = await session.execute(statement)
            rows = result.all()

            items: list[SEtfShort] = []
            for metric, etf in rows:
                items.append(
                    SEtfShort(
                        secid=metric.secid,
                        shortname=etf.shortname,
                        return_1y=metric.return_1y,
                        return_5y=metric.return_5y,
                        div_yield=metric.div_yield,
                        score=metric.score,
                    )
                )

            return items


    @classmethod
    async def get_info_by_secid(cls, secid: str) -> SEtfDetail | None:
        """Return detailed ETF info and latest metrics by SECID."""
        async with new_session() as session:
            statement = (
                select(EtfMetricModel, EtfModel)
                .join(EtfModel, EtfModel.secid == EtfMetricModel.secid)
                .where(EtfMetricModel.secid == secid.upper())
                .limit(1)
            )
            result = await session.execute(statement)
            row = result.first()
            if row is None:
                return None

            metric, etf = row
            return SEtfDetail(
                secid=metric.secid,
                shortname=etf.shortname,
                isin=etf.isin,
                currency=etf.currency,
                lotsize=etf.lotsize,
                price_date=metric.price_date,
                price=metric.price,
                return_1y=metric.return_1y,
                return_5y=metric.return_5y,
                div_yield=metric.div_yield,
                score=metric.score,
            )


    @classmethod
    async def _upsert_etf(cls, session: AsyncSession, etf: EtfStaticData) -> None:
        """Insert or update static ETF fields by SECID."""
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


    @classmethod
    async def _upsert_price(cls, session: AsyncSession, secid: str, price_date: date, close: float) -> None:
        """Insert or update ETF historical close value."""
        statement = insert(PriceModel).values(secid=secid, date=price_date, close=close)
        statement = statement.on_conflict_do_update(
            index_elements=[PriceModel.secid, PriceModel.date],
            set_={"close": close},
        )
        await session.execute(statement)


    @classmethod
    async def _upsert_dividend(cls, session: AsyncSession, secid: str, payout_date: date, value: float) -> None:
        """Insert or update ETF dividend payout value."""
        statement = insert(DividendModel).values(secid=secid, date=payout_date, value=value)
        statement = statement.on_conflict_do_update(
            index_elements=[DividendModel.secid, DividendModel.date],
            set_={"value": value},
        )
        await session.execute(statement)


    @classmethod
    async def _upsert_metric(
        cls,
        session: AsyncSession,
        secid: str,
        price_date: date,
        price: float,
        return_1y: float,
        return_5y: float,
        div_yield: float,
        score: float,
    ) -> None:
        """Insert or update latest ETF metric row."""
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


    @classmethod
    async def update_etf_data(cls) -> tuple[int, int]:
        """Refresh ETF cache from MOEX and recalculate all ranking metrics."""
        today = date.today()
        one_year_date = today - timedelta(days=365)
        five_year_date = today - timedelta(days=365 * 5)

        total_etfs = 0
        updated_records = 0

        async with new_session() as session:
            try:
                async with MoexApiClient() as api_client:
                    etf_list = await api_client.fetch_etf_list()
                    total_etfs = len(etf_list)

                    for etf in etf_list:
                        await cls._upsert_etf(session, etf)

                        price_date, current_price = await api_client.fetch_current_price(etf.secid, etf.board)
                        if current_price is None or current_price <= 0:
                            if etf.prevprice is None or etf.prevprice <= 0:
                                continue

                            current_price = etf.prevprice
                            price_date = today

                        await cls._upsert_price(session, etf.secid, price_date, current_price)

                        close_1y_data = await api_client.fetch_close_near_date(etf.secid, etf.board, one_year_date)
                        close_5y_data = await api_client.fetch_close_near_date(etf.secid, etf.board, five_year_date)

                        historical_1y = close_1y_data[1] if close_1y_data is not None else None
                        historical_5y = close_5y_data[1] if close_5y_data is not None else None

                        if close_1y_data is not None:
                            await cls._upsert_price(session, etf.secid, close_1y_data[0], close_1y_data[1])

                        if close_5y_data is not None:
                            await cls._upsert_price(session, etf.secid, close_5y_data[0], close_5y_data[1])

                        dividends = await api_client.fetch_dividends_last_12m(etf.secid, one_year_date)

                        total_dividends = 0.0
                        for payout_date, payout_value in dividends:
                            total_dividends += payout_value
                            await cls._upsert_dividend(session, etf.secid, payout_date, payout_value)

                        return_1y = calculate_return_percent(current_price, historical_1y)
                        return_5y = calculate_return_percent(current_price, historical_5y)
                        div_yield = calculate_dividend_yield_percent(total_dividends, current_price)
                        score = calculate_score(div_yield, return_1y, return_5y)

                        await cls._upsert_metric(
                            session=session,
                            secid=etf.secid,
                            price_date=price_date,
                            price=current_price,
                            return_1y=return_1y,
                            return_5y=return_5y,
                            div_yield=div_yield,
                            score=score,
                        )

                        updated_records += 1

                await session.commit()
            except Exception:
                await session.rollback()
                raise

        return total_etfs, updated_records


    @classmethod
    async def add_allowed_user(cls, username: str) -> None:
        """Insert allowed username if it does not already exist."""
        normalized = username.strip().lower()
        if not normalized:
            return

        if not normalized.startswith("@"):
            normalized = f"@{normalized}"

        async with new_session() as session:
            statement = insert(AllowedUserModel).values(username=normalized)
            statement = statement.on_conflict_do_nothing(index_elements=[AllowedUserModel.username])
            await session.execute(statement)
            await session.commit()


    @classmethod
    async def is_user_allowed(cls, username: str) -> bool:
        """Check whether provided username can trigger update endpoint."""
        normalized = username.strip().lower()
        if not normalized:
            return False

        if not normalized.startswith("@"):
            normalized = f"@{normalized}"

        async with new_session() as session:
            statement: Select[tuple[int]] = (
                select(AllowedUserModel.id)
                .where(AllowedUserModel.username.ilike(normalized))
                .limit(1)
            )
            result = await session.execute(statement)
            return result.scalar_one_or_none() is not None
