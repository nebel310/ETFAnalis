from collections.abc import AsyncIterator

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import declarative_base

from config import settings




Base = declarative_base()



engine: AsyncEngine = create_async_engine(
    settings.database_url,
    echo=False,
    future=True,
    pool_pre_ping=True,
)



SessionFactory = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
)



async def init_db() -> None:
    """Create all database tables if they do not exist."""
    from models import AllowedUserModel, DividendModel, EtfMetricModel, EtfModel, PriceModel

    _ = (EtfModel, PriceModel, DividendModel, EtfMetricModel, AllowedUserModel)

    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)



async def get_session() -> AsyncIterator[AsyncSession]:
    """Provide an async database session generator."""
    async with SessionFactory() as session:
        yield session
