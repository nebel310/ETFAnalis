from dataclasses import dataclass
import os

from dotenv import load_dotenv




load_dotenv()



@dataclass(slots=True)
class Settings:
    """Store application settings loaded from environment variables."""

    bot_token: str
    database_url: str



def load_settings() -> Settings:
    """Load and validate required environment variables."""
    bot_token = os.getenv("BOT_TOKEN", "").strip()
    database_url = os.getenv("DATABASE_URL", "").strip()

    if not bot_token:
        raise ValueError("BOT_TOKEN is required in environment variables.")

    if not database_url:
        raise ValueError("DATABASE_URL is required in environment variables.")

    if not database_url.startswith("postgresql+asyncpg://"):
        raise ValueError("DATABASE_URL must use asyncpg format: postgresql+asyncpg://...")

    return Settings(bot_token=bot_token, database_url=database_url)



settings = load_settings()
