import asyncio

import aiohttp
from aiogram import Router
from aiogram.exceptions import TelegramNetworkError
from aiogram.filters import Command
from aiogram.filters.command import CommandObject
from aiogram.types import Message

from config import settings




router = Router()
SEND_RETRIES = 3
SEND_RETRY_DELAY_SECONDS = 1.0
REQUEST_TIMEOUT_SECONDS = 20
NO_DATA_TEXT = "нет данных"



async def safe_answer(message: Message, text: str) -> bool:
    """Send Telegram message with retries for transient network errors."""
    for attempt in range(1, SEND_RETRIES + 1):
        try:
            await message.answer(text)
            return True

        except (TelegramNetworkError, asyncio.TimeoutError, aiohttp.ClientError):
            if attempt == SEND_RETRIES:
                return False

            await asyncio.sleep(SEND_RETRY_DELAY_SECONDS * attempt)

    return False



async def _request_backend(
    method: str,
    endpoint: str,
    params: dict[str, str] | None = None,
    payload: dict[str, str] | None = None,
) -> tuple[int, dict | list | str | None]:
    """Execute HTTP request to backend ETF API and return status with parsed body."""
    url = f"{settings.backend_url}{endpoint}"
    headers: dict[str, str] = {}

    if settings.api_key:
        headers["X-API-Key"] = settings.api_key

    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SECONDS)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.request(method, url, params=params, json=payload, headers=headers) as response:
            content_type = response.headers.get("Content-Type", "")
            if "application/json" in content_type:
                body = await response.json()
            else:
                body = await response.text()

            return response.status, body



def _format_percent(value: float) -> str:
    """Format float value as percentage with two decimals."""
    return f"{value:.2f}%"



def _format_price(value: float) -> str:
    """Format float value as price with four decimals."""
    return f"{value:.4f}"



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
    """Load and show top ETFs from backend cached metrics endpoint."""
    limit = 10
    if command.args:
        try:
            parsed_limit = int(command.args.strip())
            if parsed_limit > 0:
                limit = min(parsed_limit, 50)
            else:
                await safe_answer(message, "N должен быть положительным числом. Пример: /top 10")
                return

        except ValueError:
            await safe_answer(message, "N должен быть положительным числом. Пример: /top 10")
            return

    try:
        status_code, body = await _request_backend("GET", "/etf/top", params={"limit": str(limit)})

        if status_code != 200 or not isinstance(body, list):
            await safe_answer(message, "Не удалось получить топ фондов. Попробуйте позже.")
            return

        if not body:
            await safe_answer(message, "Данные в кэше не найдены. Сначала выполните /update.")
            return

        lines = [f"Топ {limit} фондов:", ""]
        for index, item in enumerate(body, start=1):
            secid = str(item.get("secid", NO_DATA_TEXT))
            shortname = str(item.get("shortname") or NO_DATA_TEXT)
            return_1y = float(item.get("return_1y", 0.0))
            return_5y = float(item.get("return_5y", 0.0))
            div_yield = float(item.get("div_yield", 0.0))
            score = float(item.get("score", 0.0))

            lines.append(f"{index}. {secid}")
            lines.append(f"- Название: {shortname}")
            lines.append(f"- Доходность за 1 год: {_format_percent(return_1y)}")
            lines.append(f"- Доходность за 5 лет: {_format_percent(return_5y)}")
            lines.append(f"- Дивиденды: {_format_percent(div_yield)}")
            lines.append(f"- Скор: {score:.2f}")
            lines.append("")

        await safe_answer(message, "\n".join(lines).rstrip())

    except (aiohttp.ClientError, asyncio.TimeoutError):
        await safe_answer(message, "Сервис временно недоступен. Попробуйте позже.")



async def handle_info(message: Message, command: CommandObject) -> None:
    """Load and show ETF detailed data from backend cache by SECID."""
    if not command.args:
        await safe_answer(message, "Укажите SECID. Пример: /info FXUS")
        return

    secid = command.args.strip().upper().split()[0]

    try:
        status_code, body = await _request_backend("GET", f"/etf/info/{secid}")

        if status_code == 404:
            await safe_answer(message, "Фонд не найден в кэше. Выполните /update или проверьте SECID.")
            return

        if status_code != 200 or not isinstance(body, dict):
            await safe_answer(message, "Не удалось получить информацию о фонде. Попробуйте позже.")
            return

        issue_link = f"https://www.moex.com/ru/issue.aspx?code={body.get('secid', secid)}"

        text = (
            f"SECID: {body.get('secid', secid)}\n"
            f"Название: {body.get('shortname') or NO_DATA_TEXT}\n"
            f"ISIN: {body.get('isin') or NO_DATA_TEXT}\n"
            f"Валюта: {body.get('currency') or NO_DATA_TEXT}\n"
            f"Размер лота: {body.get('lotsize') if body.get('lotsize') is not None else NO_DATA_TEXT}\n"
            f"Текущая цена: {_format_price(float(body.get('price', 0.0)))}\n"
            f"Дата цены: {body.get('price_date', NO_DATA_TEXT)}\n"
            f"Доходность за 1 год: {_format_percent(float(body.get('return_1y', 0.0)))}\n"
            f"Доходность за 5 лет: {_format_percent(float(body.get('return_5y', 0.0)))}\n"
            f"Дивидендная доходность: {_format_percent(float(body.get('div_yield', 0.0)))}\n"
            f"Скор: {float(body.get('score', 0.0)):.2f}\n"
            f"Страница на MOEX: {issue_link}"
        )

        await safe_answer(message, text)

    except (aiohttp.ClientError, asyncio.TimeoutError):
        await safe_answer(message, "Сервис временно недоступен. Попробуйте позже.")



async def handle_update(message: Message) -> None:
    """Trigger backend ETF cache refresh with username payload and optional API key."""
    raw_username = message.from_user.username if message.from_user else None
    username = f"@{raw_username}" if raw_username else None

    payload: dict[str, str] = {}
    if username:
        payload["username"] = username

    await safe_answer(message, "Обновление данных запущено. Это может занять некоторое время.")

    try:
        status_code, body = await _request_backend("POST", "/etf/update", payload=payload)

        if status_code == 403:
            await safe_answer(message, "Доступ запрещен. У вас нет прав для /update.")
            return

        if status_code != 200 or not isinstance(body, dict):
            await safe_answer(message, "Ошибка обновления данных. Попробуйте позже.")
            return

        total_etfs = int(body.get("total_etfs", 0))
        updated_records = int(body.get("updated_records", 0))
        await safe_answer(
            message,
            f"Обновление завершено. Загружено фондов: {total_etfs}. Обновлено метрик: {updated_records}.",
        )

    except (aiohttp.ClientError, asyncio.TimeoutError):
        await safe_answer(message, "Сервис временно недоступен. Попробуйте позже.")



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
