import asyncio

from aiogram import Bot, Dispatcher
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from config import settings
from database import SessionFactory, init_db
from handlers import router
from models import AllowedUserModel




ALLOWED_UPDATE_USER = "@vlados7529"



async def seed_allowed_users() -> None:
    """Seed hardcoded usernames that can run update command."""
    async with SessionFactory() as session:
        statement = insert(AllowedUserModel).values(username=ALLOWED_UPDATE_USER)
        statement = statement.on_conflict_do_nothing(index_elements=[AllowedUserModel.username])
        await session.execute(statement)
        await session.commit()



async def validate_seed() -> None:
    """Validate that at least one allowed user exists after seeding."""
    async with SessionFactory() as session:
        query = select(AllowedUserModel.id).limit(1)
        result = await session.execute(query)
        if result.scalar_one_or_none() is None:
            raise RuntimeError("Allowed users seed failed.")



async def main() -> None:
    """Initialize database and start Telegram bot polling."""
    await init_db()
    await seed_allowed_users()
    await validate_seed()

    bot = Bot(token=settings.bot_token)
    dispatcher = Dispatcher()
    dispatcher.include_router(router)

    await dispatcher.start_polling(bot)



if __name__ == "__main__":
    asyncio.run(main())
