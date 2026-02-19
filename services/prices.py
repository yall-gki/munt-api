from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Iterable

import httpx

LATEST_KEY = "prices:latest"
HISTORY_PREFIX = "prices:history:"


def history_key(coin: str) -> str:
    return f"{HISTORY_PREFIX}{coin}"


def weighted_average(data: list[dict], price_key: str, volume_key: str) -> float:
    total = 0.0
    volume = 0.0
    for item in data:
        price = float(item[price_key])
        qty = float(item[volume_key])
        total += price * qty
        volume += qty
    return round(total / volume, 6) if volume else 0.0


async def fetch_with_retry(
    client: httpx.AsyncClient,
    url: str,
    retries: int = 3,
    timeout: float = 5.0,
) -> httpx.Response:
    last_exc: Exception | None = None
    for i in range(retries):
        try:
            response = await client.get(url, timeout=timeout)
            response.raise_for_status()
            return response
        except Exception as exc:  # pragma: no cover - network errors are environment-dependent
            last_exc = exc
            await asyncio.sleep(2**i)
    raise RuntimeError("Fetch failed") from last_exc


async def fetch_binance_price(client: httpx.AsyncClient, symbol: str) -> float:
    url = f"https://api.binance.com/api/v3/trades?symbol={symbol}&limit=10"
    response = await fetch_with_retry(client, url)
    return weighted_average(response.json(), "price", "qty")


async def fetch_latest_prices(coins: dict[str, str]) -> dict[str, float]:
    items: list[tuple[str, str]] = list(coins.items())
    if not items:
        return {}

    async with httpx.AsyncClient() as client:
        tasks = [fetch_binance_price(client, symbol) for _, symbol in items]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    prices: dict[str, float] = {}
    for (coin, _), result in zip(items, results):
        if isinstance(result, Exception):
            logging.warning("Failed to fetch %s: %s", coin, result)
            continue
        prices[coin] = float(result)

    return prices



async def persist_prices(
    redis,
    prices: dict[str, float],
    timestamp: int,
    history_limit: int,
) -> None:
    if not prices:
        return

    pipe = redis.pipeline()
    pipe.hset(LATEST_KEY, mapping={coin: str(price) for coin, price in prices.items()})
    for coin, price in prices.items():
        entry = json.dumps([timestamp, price], separators=(",", ":"))
        key = history_key(coin)
        pipe.rpush(key, entry)
        pipe.ltrim(key, -history_limit, -1)
    await pipe.execute()


async def get_all_latest_prices(
    redis,
    coins: Iterable[str] | None = None,
) -> dict[str, float]:
    data = await redis.hgetall(LATEST_KEY)
    parsed = {key: float(value) for key, value in data.items()}
    if coins is None:
        return parsed
    return {coin: parsed.get(coin, 0.0) for coin in coins}


async def get_historical_prices(redis, coin: str, days: int) -> list[list[float]]:
    entries = await redis.lrange(history_key(coin), 0, -1)
    if not entries:
        return []

    cutoff = int(datetime.now(timezone.utc).timestamp()) - days * 86400
    prices: list[list[float]] = []

    for entry in entries:
        ts, price = json.loads(entry)
        if ts >= cutoff:
            prices.append([ts * 1000, float(price)])

    return prices
