import asyncio

import aiohttp
from aiogram import Router
from aiogram.exceptions import TelegramNetworkError
from aiogram.filters import Command
from aiogram.filters.command import CommandObject
from aiogram.types import Message
from sqlalchemy.exc import SQLAlchemyError

from database import SessionFactory
from utils import format_percent, format_price, get_info_query, get_top_metrics_query, is_user_allowed, refresh_etf_cache




router = Router()
SEND_RETRIES = 3
SEND_RETRY_DELAY_SECONDS = 1.0
NO_DATA_TEXT = "нет данных"



async def safe_answer(message: Message, text: str) -> bool:
    """Send a Telegram message with retries on transient network failures."""
    for attempt in range(1, SEND_RETRIES + 1):
        try:
            await message.answer(text)
            return True
        except (TelegramNetworkError, asyncio.TimeoutError, aiohttp.ClientError):
            if attempt == SEND_RETRIES:
                return False
            await asyncio.sleep(SEND_RETRY_DELAY_SECONDS * attempt)

    return False



async def handle_start(message: Message) -> None:
    """Send welcome message and available command list."""
    text = (
        "Добро пожаловать в ETF помощника.\n"
        "Доступные команды:\n"
        "/start - Показать это сообщение\n"
        "/top [N] - Показать топ фондов по скору\n"
        "/info SECID - Показать подробную информацию по фонду\n"
        "/update - Обновить данные фондов с MOEX"
    )
    await safe_answer(message, text)



async def handle_top(message: Message, command: CommandObject) -> None:
    """Show top ETFs using cached metrics sorted by score."""
    limit = 10
    if command.args:
        try:
            parsed_limit = int(command.args.strip())
            if parsed_limit > 0:
                limit = min(parsed_limit, 50)
        except ValueError:
            await safe_answer(message, "N должен быть положительным числом. Пример: /top 10")
            return

    try:
        async with SessionFactory() as session:
            query = get_top_metrics_query(limit)
            result = await session.execute(query)
            rows = result.all()

        if not rows:
            await safe_answer(message, "Данные в кэше не найдены. Сначала выполните /update.")
            return

        lines = [f"Топ {limit} фондов:", ""]
        for index, (metric, etf) in enumerate(rows, start=1):
            lines.append(f"{index}. {metric.secid}")
            lines.append(f"- Название: {etf.shortname or NO_DATA_TEXT}")
            lines.append(f"- Доходность за 1 год: {format_percent(metric.return_1y)}")
            lines.append(f"- Доходность за 5 лет: {format_percent(metric.return_5y)}")
            lines.append(f"- Дивиденды: {format_percent(metric.div_yield)}")
            lines.append(f"- Скор: {metric.score:.2f}")
            lines.append("")

        text = "\n".join(lines).rstrip()
        await safe_answer(message, text)
    except SQLAlchemyError:
        await safe_answer(message, "Ошибка базы данных при загрузке топа фондов. Попробуйте позже.")



async def handle_info(message: Message, command: CommandObject) -> None:
    """Show detailed cached information for an ETF by SECID."""
    if not command.args:
        await safe_answer(message, "Укажите SECID. Пример: /info FXUS")
        return

    secid = command.args.strip().upper()
    if " " in secid:
        secid = secid.split()[0]

    try:
        async with SessionFactory() as session:
            query = get_info_query(secid)
            result = await session.execute(query)
            row = result.first()

        if row is None:
            await safe_answer(message, "Фонд не найден в кэше. Выполните /update или проверьте SECID.")
            return

        metric, etf = row
        issue_link = f"https://www.moex.com/ru/issue.aspx?code={metric.secid}"

        text = (
            f"SECID: {metric.secid}\n"
            f"Название: {etf.shortname or NO_DATA_TEXT}\n"
            f"ISIN: {etf.isin or NO_DATA_TEXT}\n"
            f"Валюта: {etf.currency or NO_DATA_TEXT}\n"
            f"Размер лота: {etf.lotsize if etf.lotsize is not None else NO_DATA_TEXT}\n"
            f"Текущая цена: {format_price(metric.price)}\n"
            f"Дата цены: {metric.price_date.isoformat()}\n"
            f"Доходность за 1 год: {format_percent(metric.return_1y)}\n"
            f"Доходность за 5 лет: {format_percent(metric.return_5y)}\n"
            f"Дивидендная доходность: {format_percent(metric.div_yield)}\n"
            f"Скор: {metric.score:.2f}\n"
            f"Страница на MOEX: {issue_link}"
        )
        await safe_answer(message, text)
    except SQLAlchemyError:
        await safe_answer(message, "Ошибка базы данных при загрузке информации о фонде. Попробуйте позже.")



async def handle_update(message: Message) -> None:
    """Refresh ETF data from MOEX for authorized users only."""
    username = message.from_user.username if message.from_user else None
    formatted_username = f"@{username}" if username else None

    try:
        async with SessionFactory() as session:
            if not await is_user_allowed(session, formatted_username):
                await safe_answer(message, "Доступ запрещен. У вас нет прав для /update.")
                return

        await safe_answer(message, "Обновление данных запущено. Это может занять некоторое время.")

        async with SessionFactory() as session:
            total_etfs, updated_metrics = await refresh_etf_cache(session)

        await safe_answer(
            message,
            f"Обновление завершено. Загружено фондов: {total_etfs}. Обновлено метрик: {updated_metrics}.",
        )
    except (aiohttp.ClientError, asyncio.TimeoutError):
        await safe_answer(message, "MOEX API временно недоступен. Попробуйте /update позже.")
    except SQLAlchemyError:
        await safe_answer(message, "Ошибка базы данных во время обновления. Попробуйте позже.")



@router.message(Command("start"))
async def start_command(message: Message) -> None:
    """Handle /start command routing."""
    await handle_start(message)



@router.message(Command("top"))
async def top_command(message: Message, command: CommandObject) -> None:
    """Handle /top command routing."""
    await handle_top(message, command)



@router.message(Command("info"))
async def info_command(message: Message, command: CommandObject) -> None:
    """Handle /info command routing."""
    await handle_info(message, command)



@router.message(Command("update"))
async def update_command(message: Message) -> None:
    """Handle /update command routing."""
    await handle_update(message)
