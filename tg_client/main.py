import asyncio

from aiogram import Bot
from aiogram import Dispatcher

from config import settings
from handlers import router




async def main() -> None:
    """Start Telegram bot polling with configured router."""
    bot = Bot(token=settings.bot_token)
    dispatcher = Dispatcher()
    dispatcher.include_router(router)

    await dispatcher.start_polling(bot)



if __name__ == "__main__":
    asyncio.run(main())
