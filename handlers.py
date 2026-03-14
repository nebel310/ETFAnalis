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
        "Welcome to ETF Helper Bot.\n"
        "Available commands:\n"
        "/start - Show this message\n"
        "/top [N] - Show top ETFs by score\n"
        "/info SECID - Show detailed ETF info\n"
        "/update - Refresh ETF data from MOEX"
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
            await safe_answer(message, "N must be a positive integer. Example: /top 10")
            return

    try:
        async with SessionFactory() as session:
            query = get_top_metrics_query(limit)
            result = await session.execute(query)
            rows = result.all()

        if not rows:
            await safe_answer(message, "No cached ETF data found. Run /update first.")
            return

        lines = [f"Top {limit} ETFs by score:"]
        for index, (metric, etf) in enumerate(rows, start=1):
            line = (
                f"{index}. {metric.secid} | {etf.shortname or 'N/A'} | "
                f"Score: {metric.score:.2f} | "
                f"1Y: {format_percent(metric.return_1y)} | "
                f"5Y: {format_percent(metric.return_5y)} | "
                f"DivYield: {format_percent(metric.div_yield)}"
            )
            lines.append(line)

        await safe_answer(message, "\n".join(lines))
    except SQLAlchemyError:
        await safe_answer(message, "Database error occurred while loading top ETFs. Please try again later.")



async def handle_info(message: Message, command: CommandObject) -> None:
    """Show detailed cached information for an ETF by SECID."""
    if not command.args:
        await safe_answer(message, "Please provide SECID. Example: /info FXUS")
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
            await safe_answer(message, "ETF not found in cache. Run /update first or check SECID.")
            return

        metric, etf = row
        issue_link = f"https://www.moex.com/ru/issue.aspx?code={metric.secid}"

        text = (
            f"SECID: {metric.secid}\n"
            f"Name: {etf.shortname or 'N/A'}\n"
            f"ISIN: {etf.isin or 'N/A'}\n"
            f"Currency: {etf.currency or 'N/A'}\n"
            f"Lot size: {etf.lotsize if etf.lotsize is not None else 'N/A'}\n"
            f"Current price: {format_price(metric.price)}\n"
            f"Price date: {metric.price_date.isoformat()}\n"
            f"Return 1Y: {format_percent(metric.return_1y)}\n"
            f"Return 5Y: {format_percent(metric.return_5y)}\n"
            f"Dividend yield: {format_percent(metric.div_yield)}\n"
            f"Score: {metric.score:.2f}\n"
            f"MOEX page: {issue_link}"
        )
        await safe_answer(message, text)
    except SQLAlchemyError:
        await safe_answer(message, "Database error occurred while loading ETF info. Please try again later.")



async def handle_update(message: Message) -> None:
    """Refresh ETF data from MOEX for authorized users only."""
    username = message.from_user.username if message.from_user else None
    formatted_username = f"@{username}" if username else None

    try:
        async with SessionFactory() as session:
            if not await is_user_allowed(session, formatted_username):
                await safe_answer(message, "Access denied. You are not allowed to run /update.")
                return

        await safe_answer(message, "Data refresh started. This may take a while.")

        async with SessionFactory() as session:
            total_etfs, updated_metrics = await refresh_etf_cache(session)

        await safe_answer(
            message,
            f"Update completed. ETFs fetched: {total_etfs}. Metrics updated: {updated_metrics}.",
        )
    except (aiohttp.ClientError, asyncio.TimeoutError):
        await safe_answer(message, "MOEX API is temporarily unavailable. Please try /update again later.")
    except SQLAlchemyError:
        await safe_answer(message, "Database error occurred during update. Please try again later.")



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
