from __future__ import annotations

import asyncio
import logging

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from core.config import settings
from core.redis import get_redis
from models.coins import COINS
from services.prices import get_all_latest_prices

router = APIRouter()


@router.websocket("/ws/prices")
async def ws_prices(ws: WebSocket) -> None:
    origin = ws.headers.get("origin")
    if origin not in settings.allowed_origins:
        await ws.close(code=1008)
        return

    await ws.accept()
    redis = await get_redis()

    try:
        while True:
            prices = await get_all_latest_prices(redis, coins=COINS.keys())
            await ws.send_json(prices)
            await asyncio.sleep(settings.websocket_push_seconds)
    except WebSocketDisconnect:
        pass
    except Exception as exc:  # pragma: no cover - safety net
        logging.warning("WebSocket error: %s", exc)
