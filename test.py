import asyncio
import httpx
import websockets
import json

REST_URL = "http://localhost:8000/prices?page=1&page_size=10"
WS_URL = "ws://localhost:8000/ws/prices"

async def test_rest():
    async with httpx.AsyncClient() as client:
        response = await client.get(REST_URL)
        print("REST /prices response:")
        print(response.json())

async def test_ws():
    async with websockets.connect(WS_URL) as websocket:
        print("WebSocket connected, waiting for messages...")
        for _ in range(3):  # receive 3 messages then stop
            msg = await websocket.recv()
            data = json.loads(msg)
            print("WebSocket message:")
            print(data)
        print("WebSocket test complete")

async def main():
    await test_rest()
    await test_ws()

if __name__ == "__main__":
    asyncio.run(main())
