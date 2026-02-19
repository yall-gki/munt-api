from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, Request

from core.config import settings
from core.rate_limit import limiter
from core.redis import get_redis
from models.coins import COINS
from services.prices import get_all_latest_prices, get_historical_prices

router = APIRouter()


@router.get("/all-prices")
@limiter.limit(settings.rate_limit)
async def all_prices(request: Request):
    redis = await get_redis()
    return await get_all_latest_prices(redis, coins=COINS.keys())


@router.get("/coins/{coin}/market_chart")
@limiter.limit(settings.rate_limit)
async def market_chart(
    request: Request,
    coin: str,
    days: int = Query(10, ge=1),
):
    if coin not in COINS:
        raise HTTPException(status_code=404, detail="Coin not found")

    redis = await get_redis()
    prices = await get_historical_prices(redis, coin, days)
    return {"prices": prices}


@router.get("/prices")
@limiter.limit(settings.rate_limit)
async def prices(
    request: Request,
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=50),
):
    keys = list(COINS.keys())
    start = (page - 1) * page_size
    end = page * page_size
    if start >= len(keys):
        raise HTTPException(status_code=404, detail="Page out of range")

    redis = await get_redis()
    latest = await get_all_latest_prices(redis)
    return {k: latest.get(k, 0.0) for k in keys[start:end]}
