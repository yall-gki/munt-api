from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import datetime, timezone

from core.config import settings
from core.redis import get_redis
from models.coins import COINS
from services.prices import fetch_latest_prices, persist_prices

LOCK_KEY = "prices:updater_lock"
LOCK_RELEASE_SCRIPT = """
if redis.call("get", KEYS[1]) == ARGV[1] then
  return redis.call("del", KEYS[1])
else
  return 0
end
"""


async def run_price_updater() -> None:
    redis = await get_redis()
    lock_value = str(uuid.uuid4())

    while True:
        acquired = False
        try:
            acquired = await redis.set(
                LOCK_KEY,
                lock_value,
                nx=True,
                ex=settings.updater_lock_ttl_seconds,
            )
            if acquired:
                timestamp = int(datetime.now(timezone.utc).timestamp())
                prices = await fetch_latest_prices(COINS)
                await persist_prices(redis, prices, timestamp, settings.historical_limit)
                logging.info("Prices updated")
            else:
                logging.debug("Updater lock held by another worker; skipping")
        except Exception as exc:  # pragma: no cover - background loop safety
            logging.exception("Update error: %s", exc)
        finally:
            if acquired:
                try:
                    await redis.eval(LOCK_RELEASE_SCRIPT, 1, LOCK_KEY, lock_value)
                except Exception:
                    logging.warning("Failed to release updater lock")

        await asyncio.sleep(settings.updater_interval_seconds)
