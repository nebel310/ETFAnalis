from dataclasses import dataclass
import os

from dotenv import load_dotenv




load_dotenv()



class ConfigError(ValueError):
    """Raise when required Telegram client configuration is missing."""



@dataclass(slots=True)
class Settings:
    """Store Telegram client settings from environment variables."""

    bot_token: str
    backend_url: str
    api_key: str | None



def load_settings() -> Settings:
    """Load and validate Telegram client environment settings."""
    bot_token = os.getenv("BOT_TOKEN", "").strip()
    backend_url = os.getenv("BACKEND_URL", "http://backend:3001").strip().rstrip("/")
    api_key = os.getenv("API_KEY", "").strip() or None

    if not bot_token:
        raise ConfigError("BOT_TOKEN is required for tg_client")

    if not backend_url:
        raise ConfigError("BACKEND_URL must not be empty")

    return Settings(
        bot_token=bot_token,
        backend_url=backend_url,
        api_key=api_key,
    )



settings = load_settings()
