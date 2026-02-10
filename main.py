from fastapi import FastAPI, WebSocket, Request, APIRouter, Query, HTTPException, Path
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.middleware import SlowAPIMiddleware
from slowapi.errors import RateLimitExceeded

import os
import httpx
import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, Tuple

# =====================
# APP SETUP
# =====================

app = FastAPI()
router = APIRouter()

logging.basicConfig(level=logging.INFO)

# =====================
# CORS (FIXED)
# =====================

ALLOWED_ORIGINS = {
    "https://munt-xi.vercel.app",
    "http://localhost:3000",
}

app.add_middleware(
    CORSMiddleware,
    allow_origins=list(ALLOWED_ORIGINS),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =====================
# RATE LIMITING
# =====================

limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_middleware(SlowAPIMiddleware)

@app.exception_handler(RateLimitExceeded)
async def rate_limit_handler(request: Request, exc: RateLimitExceeded):
    return JSONResponse(
        status_code=429,
        content={"detail": "Rate limit exceeded. Please wait before retrying."},
    )

# =====================
# COINS
# =====================

coins = {
    "bitcoin": "BTCUSDT",
    "ethereum": "ETHUSDT",
    "binancecoin": "BNBUSDT",
    "cardano": "ADAUSDT",
    "ripple": "XRPUSDT",
}

coingecko_ids = {
    "bitcoin": "bitcoin",
    "ethereum": "ethereum",
    "binancecoin": "binancecoin",
    "cardano": "cardano",
    "ripple": "ripple",
}

# =====================
# CACHE + STORAGE
# =====================

price_cache: Dict[str, Tuple[float, datetime]] = {}
latest_prices: Dict[str, float] = {}
historical_prices: Dict[str, list] = {c: [] for c in coins}

CACHE_TTL_SECONDS = 30

# =====================
# HELPERS
# =====================

def weighted_average(data, price_key, volume_key):
    total, volume = 0.0, 0.0
    for d in data:
        p, v = float(d[price_key]), float(d[volume_key])
        total += p * v
        volume += v
    return round(total / volume, 6) if volume else 0.0

async def fetch_with_retry(client, url, retries=3):
    for i in range(retries):
        try:
            r = await client.get(url, timeout=5)
            r.raise_for_status()
            return r
        except Exception:
            await asyncio.sleep(2 ** i)
    raise RuntimeError("Fetch failed")

async def fetch_binance(symbol: str) -> float:
    url = f"https://api.binance.com/api/v3/trades?symbol={symbol}&limit=10"
    async with httpx.AsyncClient() as c:
        r = await fetch_with_retry(c, url)
        return weighted_average(r.json(), "price", "qty")

async def get_price(coin: str) -> float:
    now = datetime.utcnow()
    if coin in price_cache and price_cache[coin][1] > now:
        return price_cache[coin][0]

    price = await fetch_binance(coins[coin])
    price_cache[coin] = (price, now + timedelta(seconds=CACHE_TTL_SECONDS))
    return price

# =====================
# BACKGROUND UPDATER (SINGLE, FIXED)
# =====================

async def background_price_updater():
    while True:
        try:
            prices = {}
            now_ts = int(datetime.utcnow().replace(tzinfo=timezone.utc).timestamp())

            for coin in coins:
                price = await get_price(coin)
                prices[coin] = price
                historical_prices[coin].append((now_ts, price))
                historical_prices[coin] = historical_prices[coin][-10_000:]

            global latest_prices
            latest_prices = prices

            logging.info("✅ Prices updated")

        except Exception as e:
            logging.error(f"⛔ Update error: {e}")

        await asyncio.sleep(10)

@app.on_event("startup")
async def startup():
    asyncio.create_task(background_price_updater())

# =====================
# REST API
# =====================

@app.get("/all-prices")
async def all_prices():
    return latest_prices

@router.get("/prices")
@limiter.limit("10/minute")
async def prices(
    request: Request,
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=50),
):
    keys = list(coins.keys())
    start, end = (page - 1) * page_size, page * page_size
    if start >= len(keys):
        raise HTTPException(404, "Page out of range")
    return {k: latest_prices.get(k, 0.0) for k in keys[start:end]}

# =====================
# HISTORICAL
# =====================

@router.get("/coins/{coin}/market_chart")
async def market_chart(coin: str, days: int = Query(10, ge=1)):
    if coin not in historical_prices:
        raise HTTPException(404, "Coin not found")

    cutoff = int(datetime.utcnow().timestamp()) - days * 86400
    return {
        "prices": [[ts * 1000, p] for ts, p in historical_prices[coin] if ts >= cutoff]
    }

# =====================
# WEBSOCKET (SECURED)
# =====================

@app.websocket("/ws/prices")
async def ws_prices(ws: WebSocket):
    origin = ws.headers.get("origin")
    if origin not in ALLOWED_ORIGINS:
        await ws.close(code=1008)
        return

    await ws.accept()

    try:
        while True:
            await ws.send_json(latest_prices)
            await asyncio.sleep(5)
    except Exception:
        pass

# =====================
# REGISTER ROUTER
# =====================

app.include_router(router)

# =====================
# ENTRYPOINT
# =====================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
