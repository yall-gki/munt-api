from __future__ import annotations

from dataclasses import dataclass, field
import os

from dotenv import load_dotenv
load_dotenv()


def _split_origins(value: str) -> set[str]:
    """Convert comma-separated origins string into a set of origins."""
    return {origin.strip() for origin in value.split(",") if origin.strip()}


@dataclass(frozen=True)
class Settings:
    # Redis URL (must be set in env)
    redis_url: str = os.getenv("REDIS_URL")
    
    # Cache & updater intervals
    cache_ttl_seconds: int = int(os.getenv("CACHE_TTL_SECONDS", "30"))
    updater_interval_seconds: int = int(os.getenv("UPDATER_INTERVAL_SECONDS", "10"))
    websocket_push_seconds: int = int(os.getenv("WS_PUSH_SECONDS", "5"))
    historical_limit: int = int(os.getenv("HISTORICAL_LIMIT", "10000"))

    # Allowed CORS origins
    allowed_origins: set[str] = field(
        default_factory=lambda: _split_origins(
            os.getenv(
                "ALLOWED_ORIGINS",
                "https://munt-xi.vercel.app,http://localhost:3000",
            )
        )
    )

    # Rate limiting
    rate_limit: str = os.getenv("RATE_LIMIT", "10/minute")
    updater_lock_ttl_seconds: int = int(os.getenv("UPDATER_LOCK_TTL_SECONDS", "30"))


# Fail fast if Redis URL is missing
settings = Settings()
if not settings.redis_url:
    raise RuntimeError("REDIS_URL environment variable must be set!")
