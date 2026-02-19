from __future__ import annotations

import asyncio
import contextlib
import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware

from api.routes import router as api_router
from api.ws import router as ws_router
from core.config import settings
from core.rate_limit import limiter, rate_limit_handler
from core.redis import close_redis, get_redis
from services.updater import run_price_updater

logging.basicConfig(level=logging.INFO)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # ─── Startup ─────────────────────────────────────────
    await get_redis()
    app.state.updater_task = asyncio.create_task(run_price_updater())

    yield

    # ─── Shutdown ────────────────────────────────────────
    task = getattr(app.state, "updater_task", None)
    if task:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    await close_redis()


app = FastAPI(lifespan=lifespan)

# ─── Rate limiting ───────────────────────────────────────
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, rate_limit_handler)
app.add_middleware(SlowAPIMiddleware)

# ─── CORS ────────────────────────────────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=list(settings.allowed_origins),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─── Test endpoint ───────────────────────────────────────
@app.get("/test-redis")
async def test_redis():
    redis = await get_redis()
    await redis.set("foo", "bar")
    value = await redis.get("foo")
    return {"value": value}

# ─── Routers ─────────────────────────────────────────────
app.include_router(api_router)
app.include_router(ws_router)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", 8000)),
        reload=True,
    )
