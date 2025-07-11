from fastapi import FastAPI, WebSocket, APIRouter, Query, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import httpx
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, Tuple
from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.middleware import SlowAPIMiddleware

app = FastAPI()
router = APIRouter()
logging.basicConfig(level=logging.INFO)

# === CORS Middleware: Restrict origins to your frontend domain ===
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://your-nextjs-domain.vercel.app"],  # change this to your Next.js frontend URL
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# === Rate Limiter ===
limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_middleware(SlowAPIMiddleware)

# === Coins and mappings ===
coins = {
    "bitcoin": "BTCUSDT",
    "ethereum": "ETHUSDT",
    "binancecoin": "BNBUSDT",
    "cardano": "ADAUSDT",
    "ripple": "XRPUSDT",
    "polkadot": "DOTUSDT",
    "uniswap": "UNIUSDT",
    "chainlink": "LINKUSDT",
    "litecoin": "LTCUSDT",
    "stellar": "XLMUSDT",
    "usdc": "USDCUSDT",
    "dogecoin": "DOGEUSDT",
    "vechain": "VETUSDT",
    "filecoin": "FILUSDT",
    "tron": "TRXUSDT",
    "eos": "EOSUSDT",
    "aave": "AAVEUSDT",
    "monero": "XMRUSDT",
    "cosmos": "ATOMUSDT",
    "tezos": "XTZUSDT",
    "algorand": "ALGOUSDT",
    "nem": "XEMUSDT",
    "compound": "COMPUSDT",
    "kusama": "KSMUSDT",
    "zilliqa": "ZILUSDT",
    "neo": "NEOUSDT",
    "sushiswap": "SUSHIUSDT",
    "maker": "MKRUSDT",
    "dash": "DASHUSDT",
    "elrond": "EGLDUSDT",
}

coingecko_ids = {
    "bitcoin": "bitcoin",
    "ethereum": "ethereum",
    "binancecoin": "binancecoin",
    "cardano": "cardano",
    "ripple": "ripple",
    "polkadot": "polkadot",
    "uniswap": "uniswap",
    "chainlink": "chainlink",
    "litecoin": "litecoin",
    "stellar": "stellar",
    "usdc": "usd-coin",
    "dogecoin": "dogecoin",
    "vechain": "vechain",
    "filecoin": "filecoin",
    "tron": "tron",
    "eos": "eos",
    "aave": "aave",
    "monero": "monero",
    "cosmos": "cosmos",
    "tezos": "tezos",
    "algorand": "algorand",
    "nem": "nem",
    "compound": "compound-governance-token",
    "kusama": "kusama",
    "zilliqa": "zilliqa",
    "neo": "neo",
    "sushiswap": "sushi",
    "maker": "maker",
    "dash": "dash",
    "elrond": "elrond",
}

# === In-memory cache store ===
price_cache: Dict[str, Tuple[float, datetime]] = {}
CACHE_TTL_SECONDS = 30

# === Weighted average helper ===
def weighted_average(data, price_key, volume_key):
    total, volume_sum = 0.0, 0.0
    for trade in data:
        price = float(trade[price_key]) if isinstance(price_key, str) else float(trade[price_key])
        volume = float(trade[volume_key]) if isinstance(volume_key, str) else float(trade[volume_key])
        total += price * volume
        volume_sum += volume
    return round(total / volume_sum, 6) if volume_sum > 0 else 0.0

# === Rate-limit / retry helper with exponential backoff ===
async def fetch_with_retry(client, url, retries=3, backoff_in_sec=1):
    for attempt in range(1, retries + 1):
        try:
            resp = await client.get(url, timeout=5)
            resp.raise_for_status()
            return resp
        except (httpx.HTTPError, httpx.TimeoutException) as e:
            logging.warning(f"Request to {url} failed (attempt {attempt}): {e}")
            if attempt == retries:
                raise
            await asyncio.sleep(backoff_in_sec * 2 ** (attempt - 1))

# === Exchange fetchers ===
async def fetch_binance(symbol: str) -> float:
    url = f"https://api.binance.com/api/v3/trades?symbol={symbol}&limit=10"
    async with httpx.AsyncClient() as client:
        res = await fetch_with_retry(client, url)
        trades = res.json()
        return weighted_average(trades, "price", "qty")

async def fetch_kraken(symbol: str) -> float:
    kraken_map = {
        "BTCUSDT": "XBTUSD", "ETHUSDT": "ETHUSD", "BNBUSDT": "BNBUSD", "ADAUSDT": "ADAUSD",
        "XRPUSDT": "XRPUSD", "DOTUSDT": "DOTUSD", "UNIUSDT": "UNIUSD", "LINKUSDT": "LINKUSD",
        "LTCUSDT": "LTCUSD", "XLMUSDT": "XLMUSD", "DOGEUSDT": "DOGEUSD", "VETUSDT": "VETUSD",
        "FILUSDT": "FILUSD", "TRXUSDT": "TRXUSD", "EOSUSDT": "EOSUSD", "AAVEUSDT": "AAVEUSD",
        "XMRUSDT": "XMRUSD", "ATOMUSDT": "ATOMUSD", "XTZUSDT": "XTZUSD", "ALGOUSDT": "ALGOUSD",
        "XEMUSDT": "XEMUSD", "COMPUSDT": "COMPUSD", "KSMUSDT": "KSMUSD", "ZILUSDT": "ZILUSD",
        "NEOUSDT": "NEOUSD", "SUSHIUSDT": "SUSHIUSD", "MKRUSDT": "MKRUSD", "DASHUSDT": "DASHUSD",
        "EGLDUSDT": "EGLDUSD",
    }
    pair = kraken_map.get(symbol)
    if not pair:
        raise ValueError(f"Unsupported Kraken symbol {symbol}")
    url = f"https://api.kraken.com/0/public/Trades?pair={pair}"
    async with httpx.AsyncClient() as client:
        res = await fetch_with_retry(client, url)
        trades = list(res.json()["result"].values())[0][:10]
        return weighted_average(trades, 0, 1)

async def fetch_coinbase(symbol: str) -> float:
    symbol_map = {
        "BTCUSDT": "BTC-USD", "ETHUSDT": "ETH-USD", "BNBUSDT": "BNB-USD", "ADAUSDT": "ADA-USD",
        "XRPUSDT": "XRP-USD", "DOTUSDT": "DOT-USD", "UNIUSDT": "UNI-USD", "LINKUSDT": "LINK-USD",
        "LTCUSDT": "LTC-USD", "XLMUSDT": "XLM-USD", "DOGEUSDT": "DOGE-USD", "VETUSDT": "VET-USD",
        "FILUSDT": "FIL-USD", "TRXUSDT": "TRX-USD", "EOSUSDT": "EOS-USD", "AAVEUSDT": "AAVE-USD",
        "XMRUSDT": "XMR-USD", "ATOMUSDT": "ATOM-USD", "XTZUSDT": "XTZ-USD", "ALGOUSDT": "ALGO-USD",
        "XEMUSDT": "XEM-USD", "COMPUSDT": "COMP-USD", "KSMUSDT": "KSM-USD", "ZILUSDT": "ZIL-USD",
        "NEOUSDT": "NEO-USD", "SUSHIUSDT": "SUSHI-USD", "MKRUSDT": "MKR-USD", "DASHUSDT": "DASH-USD",
        "EGLDUSDT": "EGLD-USD",
    }
    pair = symbol_map.get(symbol)
    if not pair:
        raise ValueError(f"Unsupported Coinbase symbol {symbol}")
    url = f"https://api.exchange.coinbase.com/products/{pair}/trades"
    async with httpx.AsyncClient() as client:
        res = await fetch_with_retry(client, url)
        trades = res.json()[:10]
        return weighted_average(trades, "price", "size")

# === Get price with parallel fetch and cache ===
async def get_price(coin: str) -> float:
    now = datetime.utcnow()
    cached = price_cache.get(coin)
    if cached and cached[1] > now:
        return cached[0]

    symbol = coins[coin]
    fetchers = [fetch_binance, fetch_coinbase, fetch_kraken]

    async def try_fetch(fetcher):
        try:
            price = await fetcher(symbol)
            logging.info(f"Got price {price} for {coin} from {fetcher.__name__}")
            return price
        except Exception as e:
            logging.warning(f"{fetcher.__name__} failed for {coin}: {e}")
            return None

    results = await asyncio.gather(*(try_fetch(f) for f in fetchers))

    price = next((p for p in results if p is not None and p > 0), 0.0)
    price_cache[coin] = (price, now + timedelta(seconds=CACHE_TTL_SECONDS))
    return price

# === Verify prices with batch CoinGecko call ===
async def verify_prices_with_coingecko(prices: Dict[str, float]) -> Dict[str, bool]:
    ids_str = ",".join(coingecko_ids.values())
    url = f"https://api.coingecko.com/api/v3/simple/price?ids={ids_str}&vs_currencies=usd"
    verified = {}
    async with httpx.AsyncClient() as client:
        try:
            res = await client.get(url, timeout=10)
            res.raise_for_status()
            cg_data = res.json()
            for coin, price in prices.items():
                id = coingecko_ids[coin]
                cg_price = cg_data.get(id, {}).get("usd")
                if cg_price is None:
                    verified[coin] = True
                    continue
                diff = abs(price - cg_price) / cg_price
                verified[coin] = diff <= 0.05
        except Exception as e:
            logging.warning(f"CoinGecko batch verification failed: {e}")
            for coin in prices:
                verified[coin] = True
    return verified

# === Store all latest prices here updated by background ===
latest_prices: Dict[str, float] = {}

# === Background updater task ===
async def background_price_updater():
    while True:
        try:
            tasks = [get_price(coin) for coin in coins]
            prices_list = await asyncio.gather(*tasks)
            prices = dict(zip(coins.keys(), prices_list))
            verified = await verify_prices_with_coingecko(prices)
            for coin, ok in verified.items():
                if not ok:
                    logging.warning(f"Verification failed for {coin}, retrying price fetch")
                    prices[coin] = await get_price(coin)
            global latest_prices
            latest_prices = prices
            logging.info("Background price update completed")
        except Exception as e:
            logging.error(f"Background price update error: {e}")
        await asyncio.sleep(10)

# === Paginated prices endpoint with rate limiting ===
@router.get("/prices")
@limiter.limit("10/minute")  # Limit to 10 requests per minute per IP
async def prices(page: int = Query(1, ge=1), page_size: int = Query(10, ge=1, le=50)):
    start = (page - 1) * page_size
    end = start + page_size
    coins_list = list(coins.keys())
    if start >= len(coins_list):
        raise HTTPException(status_code=404, detail="Page out of range")
    page_coins = coins_list[start:end]
    return {coin: latest_prices.get(coin, 0.0) for coin in page_coins}

# === WebSocket endpoint pushing prices every 5 seconds ===
@app.websocket("/ws/prices")
async def websocket_prices(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            await websocket.send_json(latest_prices)
            await asyncio.sleep(5)
    except Exception as e:
        logging.error(f"WebSocket error: {e}")
        await websocket.close()

app.include_router(router)

# === Global exception handler for unhandled errors ===
@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception):
    logging.error(f"Unhandled error: {exc}")
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal Server Error"},
    )

# === Startup event to launch background updater ===
@app.on_event("startup")
async def startup_event():
    asyncio.create_task(background_price_updater())
