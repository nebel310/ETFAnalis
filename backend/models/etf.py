from datetime import date
from datetime import datetime

from sqlalchemy import Date
from sqlalchemy import DateTime
from sqlalchemy import Float
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import UniqueConstraint
from sqlalchemy import func
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column

from database import Model




class EtfModel(Model):
    """Store static ETF instrument attributes."""

    __tablename__ = "etfs"

    secid: Mapped[str] = mapped_column(String(32), primary_key=True)
    shortname: Mapped[str | None] = mapped_column(String(255), nullable=True)
    isin: Mapped[str | None] = mapped_column(String(32), nullable=True)
    currency: Mapped[str | None] = mapped_column(String(16), nullable=True)
    lotsize: Mapped[int | None] = mapped_column(Integer, nullable=True)
    prevprice: Mapped[float | None] = mapped_column(Float, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )




class PriceModel(Model):
    """Store cached historical ETF close prices."""

    __tablename__ = "prices"
    __table_args__ = (UniqueConstraint("secid", "date", name="uq_prices_secid_date"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    secid: Mapped[str] = mapped_column(
        String(32),
        ForeignKey("etfs.secid", ondelete="CASCADE"),
        nullable=False,
    )
    date: Mapped[date] = mapped_column(Date, nullable=False)
    close: Mapped[float] = mapped_column(Float, nullable=False)




class DividendModel(Model):
    """Store cached ETF dividend payouts."""

    __tablename__ = "dividends"
    __table_args__ = (UniqueConstraint("secid", "date", name="uq_dividends_secid_date"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    secid: Mapped[str] = mapped_column(
        String(32),
        ForeignKey("etfs.secid", ondelete="CASCADE"),
        nullable=False,
    )
    date: Mapped[date] = mapped_column(Date, nullable=False)
    value: Mapped[float] = mapped_column(Float, nullable=False)




class EtfMetricModel(Model):
    """Store latest calculated ETF ranking metrics."""

    __tablename__ = "etf_metrics"

    secid: Mapped[str] = mapped_column(
        String(32),
        ForeignKey("etfs.secid", ondelete="CASCADE"),
        primary_key=True,
    )
    price_date: Mapped[date] = mapped_column(Date, nullable=False)
    price: Mapped[float] = mapped_column(Float, nullable=False)
    return_1y: Mapped[float] = mapped_column(Float, nullable=False)
    return_5y: Mapped[float] = mapped_column(Float, nullable=False)
    div_yield: Mapped[float] = mapped_column(Float, nullable=False)
    score: Mapped[float] = mapped_column(Float, nullable=False)
    calculated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )




class AllowedUserModel(Model):
    """Store usernames allowed to trigger full update."""

    __tablename__ = "allowed_users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    username: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
