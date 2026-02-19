# Coin API

FastAPI service that fetches crypto prices, stores the latest and historical data in Redis, and serves REST and WebSocket endpoints with rate limiting and CORS controls.

**Features**
- Background updater that fetches prices on an interval and persists them to Redis.
- REST endpoints for latest prices, paginated results, and historical charts.
- WebSocket stream for realtime price pushes.
- Rate limiting via SlowAPI and CORS allow-listing.

**Architecture**
- App entrypoint: `main.py`
- REST routes: `api/routes.py`
- WebSocket routes: `api/ws.py`
- Redis client: `core/redis.py`
- Background updater: `services/updater.py`
- Price fetching + storage: `services/prices.py`
- Supported coins list: `models/coins.py`

**Requirements**
- Python 3.9+
- Redis (local or hosted)

**Setup**
1. Create a virtual environment and install dependencies.
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```
2. Create a `.env` file with at least `REDIS_URL`.
```bash
REDIS_URL=rediss://user:password@host:6379
ALLOWED_ORIGINS=http://localhost:3000
CACHE_TTL_SECONDS=30
UPDATER_INTERVAL_SECONDS=10
WS_PUSH_SECONDS=5
HISTORICAL_LIMIT=10000
RATE_LIMIT=10/minute
UPDATER_LOCK_TTL_SECONDS=30
```

**Run**
1. Start the API.
```bash
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```
2. Optionally, run the simple smoke test (requires the API to be running).
```bash
python test.py
```

**API**
Method | Path | Description | Params
--- | --- | --- | ---
GET | `/test-redis` | Writes and reads a value from Redis | None
GET | `/all-prices` | Latest prices for all coins | None
GET | `/prices` | Paginated latest prices | `page` (default 1), `page_size` (default 10, max 50)
GET | `/coins/{coin}/market_chart` | Historical prices for a coin | `days` (default 10)
WS | `/ws/prices` | Realtime price stream | Requires `Origin` in `ALLOWED_ORIGINS`

**Configuration**
- `REDIS_URL` (required): Redis connection URL.
- `ALLOWED_ORIGINS` (default `https://munt-xi.vercel.app,http://localhost:3000`): Comma-separated list for CORS and WebSocket origin checks.
- `CACHE_TTL_SECONDS` (default `30`): Reserved for cache TTL.
- `UPDATER_INTERVAL_SECONDS` (default `10`): Price fetch interval.
- `WS_PUSH_SECONDS` (default `5`): WebSocket push interval.
- `HISTORICAL_LIMIT` (default `10000`): Max history entries per coin.
- `RATE_LIMIT` (default `10/minute`): SlowAPI rate limit string.
- `UPDATER_LOCK_TTL_SECONDS` (default `30`): Redis lock TTL for the updater.

**Notes**
- Prices are fetched from Binance trades and stored in Redis.
- Latest prices are stored in the hash key `prices:latest`.
- Historical prices are stored in list keys `prices:history:{coin}` as `[timestamp, price]` JSON entries.
