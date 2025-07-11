from fastapi import FastAPI, WebSocket, Request, APIRouter, Query, HTTPException
from fastapi.responses import JSONResponse
from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.middleware import SlowAPIMiddleware
from slowapi.errors import RateLimitExceeded
import os

import httpx
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, Tuple

# === FastAPI and Middleware setup ===
app = FastAPI()
router = APIRouter()

limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_middleware(SlowAPIMiddleware)

logging.basicConfig(level=logging.INFO)

# === Rate-limit handler ===
@app.exception_handler(RateLimitExceeded)
async def rate_limit_handler(request: Request, exc: RateLimitExceeded):
    return JSONResponse(
        status_code=429,
        content={"detail": "Rate limit exceeded. Please wait before retrying."}
    )

# === Coin mappings ===
coins = {
    "bitcoin": "BTCUSDT", "ethereum": "ETHUSDT", "binancecoin": "BNBUSDT",
    "cardano": "ADAUSDT", "ripple": "XRPUSDT", "polkadot": "DOTUSDT",
    "uniswap": "UNIUSDT", "chainlink": "LINKUSDT", "litecoin": "LTCUSDT",
    "stellar": "XLMUSDT", "usdc": "USDCUSDT", "dogecoin": "DOGEUSDT",
    "vechain": "VETUSDT", "filecoin": "FILUSDT", "tron": "TRXUSDT",
    "eos": "EOSUSDT", "aave": "AAVEUSDT", "monero": "XMRUSDT",
    "cosmos": "ATOMUSDT", "tezos": "XTZUSDT", "algorand": "ALGOUSDT",
    "nem": "XEMUSDT", "compound": "COMPUSDT", "kusama": "KSMUSDT",
    "zilliqa": "ZILUSDT", "neo": "NEOUSDT", "sushiswap": "SUSHIUSDT",
    "maker": "MKRUSDT", "dash": "DASHUSDT", "elrond": "EGLDUSDT"
}

coingecko_ids = {k: k if k != "usdc" else "usd-coin" for k in coins.keys()}
coingecko_ids["compound"] = "compound-governance-token"
coingecko_ids["sushiswap"] = "sushi"

# === In-memory cache ===
price_cache: Dict[str, Tuple[float, datetime]] = {}
latest_prices: Dict[str, float] = {}
CACHE_TTL_SECONDS = 30

# === Weighted average ===
def weighted_average(data, price_key, volume_key):
    total, volume_sum = 0.0, 0.0
    for trade in data:
        price = float(trade[price_key])
        volume = float(trade[volume_key])
        total += price * volume
        volume_sum += volume
    return round(total / volume_sum, 6) if volume_sum > 0 else 0.0

# === Retry logic ===
async def fetch_with_retry(client, url, retries=3, backoff=1):
    for attempt in range(retries):
        try:
            res = await client.get(url, timeout=5)
            res.raise_for_status()
            return res
        except Exception as e:
            logging.warning(f"Fetch failed ({url}): {e}")
            if attempt < retries - 1:
                await asyncio.sleep(backoff * 2**attempt)
    raise Exception(f"All retries failed for {url}")

# === Exchange fetchers ===
async def fetch_binance(symbol: str) -> float:
    url = f"https://api.binance.com/api/v3/trades?symbol={symbol}&limit=10"
    async with httpx.AsyncClient() as client:
        res = await fetch_with_retry(client, url)
        return weighted_average(res.json(), "price", "qty")

async def fetch_kraken(symbol: str) -> float:
    kraken_map = {
        "BTCUSDT": "XBTUSD", "ETHUSDT": "ETHUSD", "BNBUSDT": "BNBUSD",
        "ADAUSDT": "ADAUSD", "XRPUSDT": "XRPUSD", "DOTUSDT": "DOTUSD",
        "UNIUSDT": "UNIUSD", "LINKUSDT": "LINKUSD", "LTCUSDT": "LTCUSD",
        "XLMUSDT": "XLMUSD", "DOGEUSDT": "DOGEUSD", "VETUSDT": "VETUSD",
        "FILUSDT": "FILUSD", "TRXUSDT": "TRXUSD", "EOSUSDT": "EOSUSD",
        "AAVEUSDT": "AAVEUSD", "XMRUSDT": "XMRUSD", "ATOMUSDT": "ATOMUSD",
        "XTZUSDT": "XTZUSD", "ALGOUSDT": "ALGOUSD", "XEMUSDT": "XEMUSD",
        "COMPUSDT": "COMPUSD", "KSMUSDT": "KSMUSD", "ZILUSDT": "ZILUSD",
        "NEOUSDT": "NEOUSD", "SUSHIUSDT": "SUSHIUSD", "MKRUSDT": "MKRUSD",
        "DASHUSDT": "DASHUSD", "EGLDUSDT": "EGLDUSD"
    }
    pair = kraken_map.get(symbol)
    url = f"https://api.kraken.com/0/public/Trades?pair={pair}"
    async with httpx.AsyncClient() as client:
        res = await fetch_with_retry(client, url)
        trades = list(res.json()["result"].values())[0][:10]
        return weighted_average(trades, 0, 1)

async def fetch_coinbase(symbol: str) -> float:
    symbol_map = {
        k: v.replace("USDT", "-USD") for k, v in coins.items()
    }
    pair = symbol_map.get(symbol)
    url = f"https://api.exchange.coinbase.com/products/{pair}/trades"
    async with httpx.AsyncClient() as client:
        res = await fetch_with_retry(client, url)
        return weighted_average(res.json()[:10], "price", "size")

# === Parallel price fetch with caching ===
async def get_price(coin: str) -> float:
    now = datetime.utcnow()
    if coin in price_cache and price_cache[coin][1] > now:
        return price_cache[coin][0]

    symbol = coins[coin]
    fetchers = [fetch_binance, fetch_coinbase, fetch_kraken]

    async def try_fetch(fetcher):
        try:
            return await fetcher(symbol)
        except Exception as e:
            logging.warning(f"{fetcher.__name__} failed for {coin}: {e}")
            return None

    prices = await asyncio.gather(*(try_fetch(f) for f in fetchers))
    price = next((p for p in prices if p is not None and p > 0), 0.0)
    price_cache[coin] = (price, now + timedelta(seconds=CACHE_TTL_SECONDS))
    return price

# === CoinGecko verification ===
async def verify_prices_with_coingecko(prices: Dict[str, float]) -> Dict[str, bool]:
    ids_str = ",".join(coingecko_ids.values())
    url = f"https://api.coingecko.com/api/v3/simple/price?ids={ids_str}&vs_currencies=usd"
    async with httpx.AsyncClient() as client:
        try:
            res = await client.get(url, timeout=10)
            cg_data = res.json()
            result = {}
            for coin, price in prices.items():
                cg_price = cg_data.get(coingecko_ids[coin], {}).get("usd")
                if not cg_price:
                    result[coin] = True
                    continue
                diff = abs(price - cg_price) / cg_price
                result[coin] = diff <= 0.05
            return result
        except Exception as e:
            logging.warning(f"CoinGecko verification failed: {e}")
            return {coin: True for coin in prices}

# === Background update task ===
async def background_price_updater():
    while True:
        try:
            tasks = [get_price(coin) for coin in coins]
            prices_list = await asyncio.gather(*tasks)
            prices = dict(zip(coins.keys(), prices_list))
            verified = await verify_prices_with_coingecko(prices)
            for coin, is_valid in verified.items():
                if not is_valid:
                    prices[coin] = await get_price(coin)
            global latest_prices
            latest_prices = prices
            logging.info("Prices updated")
        except Exception as e:
            logging.error(f"Background error: {e}")
        await asyncio.sleep(10)

# === REST API endpoint with pagination ===
@router.get("/prices")
@limiter.limit("10/minute")
async def prices(request: Request, page: int = Query(1, ge=1), page_size: int = Query(10, ge=1, le=50)):
    start = (page - 1) * page_size
    end = start + page_size
    keys = list(coins.keys())
    if start >= len(keys):
        raise HTTPException(status_code=404, detail="Page out of range")
    return {k: latest_prices.get(k, 0.0) for k in keys[start:end]}

# === WebSocket for real-time price updates ===
@app.websocket("/ws/prices")
async def websocket_prices(websocket: WebSocket):
    await websocket.accept()
    while True:
        try:
            await websocket.send_json(latest_prices)
            await asyncio.sleep(5)
        except Exception:
            break

# === Start background job on startup ===
@app.on_event("startup")
async def startup_event():
    asyncio.create_task(background_price_updater())

# === Register the router ===
app.include_router(router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",  # main is the filename without .py
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 8000)),
        reload=False
    )