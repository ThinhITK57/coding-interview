"""
===============================================================================
 🔌 BÀI TẬP THỰC HÀNH ASYNC PYTHON WEBSOCKET & REDIS PUB/SUB
===============================================================================
File này chứa mã nguồn thực hành Async Python WebSocket Server hoàn chỉnh:
  - Khởi tạo Async WebSocket Server với `websockets` & `asyncio`.
  - Quản lý tập hợp Client Connections (Join / Disconnect / Reconnect).
  - Lắng nghe Redis Pub/Sub bằng `redis.asyncio` và Broadcast Push Events.
  - Tự động đồng bộ Initial State từ Redis khi Client mới kết nối.

Usage:
  python3 09_WEBSOCKET_ASYNC_PRACTICE.py
===============================================================================
"""

import asyncio
import json
import logging
import sys
import websockets
import redis.asyncio as aioredis

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

CLIENTS = set()
REDIS_HOST = "master"
REDIS_PORT = 6379
PUBSUB_CHANNEL = "channel:report-updates"


async def get_redis_client():
    return aioredis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)


async def fetch_redis_initial_state(redis_client) -> dict:
    """Đọc dữ liệu báo cáo mới nhất từ Redis String Keys."""
    try:
        val = await redis_client.get("report:agg:monthly")
        return json.loads(val) if val else {"status": "NO_DATA_YET", "total_revenue": 0.0}
    except Exception as e:
        logging.warning(f"Redis fetch warning: {e}")
        return {"status": "MOCK_INITIAL_DATA", "total_revenue": 17050000.0}


async def ws_handler(websocket, redis_client):
    """Xử lý vòng đời kết nối của WebSocket Client."""
    client_addr = websocket.remote_address
    logging.info(f"🔌 Client connected from {client_addr}")
    CLIENTS.add(websocket)

    try:
        # 1. Initial State Sync
        init_data = await fetch_redis_initial_state(redis_client)
        init_payload = json.dumps({"type": "INITIAL_STATE", "data": init_data})
        await websocket.send(init_payload)
        logging.info(f"📤 Sent INITIAL_STATE to {client_addr}")

        # 2. Ping-Pong Heartbeat Listener
        async for message in websocket:
            logging.info(f"📩 Msg from {client_addr}: {message}")
            if message == "ping":
                await websocket.send(json.dumps({"type": "pong"}))

    except websockets.exceptions.ConnectionClosed:
        logging.info(f"🔌 Client {client_addr} disconnected.")
    finally:
        CLIENTS.remove(websocket)


async def redis_pubsub_broadcaster(redis_client):
    """Lắng nghe Redis Pub/Sub và Broadcast tin nhắn Push tới tất cả WebSockets."""
    pubsub = redis_client.pubsub()
    await pubsub.subscribe(PUBSUB_CHANNEL)
    logging.info(f"📡 Redis Pub/Sub subscribed to '{PUBSUB_CHANNEL}'. Ready for Push events.")

    try:
        async for msg in pubsub.listen():
            if msg["type"] == "message":
                data = msg["data"]
                logging.info(f"⚡ Redis Event Received: {data}")

                broadcast_payload = json.dumps({
                    "type": "REPORT_UPDATE",
                    "event": "REPORT_UPDATED",
                    "data": json.loads(data) if isinstance(data, str) and data.startswith("{") else data
                })

                if CLIENTS:
                    logging.info(f"📢 PUSHING to {len(CLIENTS)} connected WebSocket clients...")
                    websockets.broadcast(CLIENTS, broadcast_payload)
    except Exception as e:
        logging.error(f"Redis PubSub Error: {e}")


async def main():
    logging.info("=" * 60)
    logging.info(" Fleet Platform — Async WebSocket Serving Practice Server")
    logging.info(" Port: 8765 | Redis Channel: channel:report-updates")
    logging.info("=" * 60)

    redis_client = await get_redis_client()

    # Chạy Redis PubSub listener dưới dạng Task chạy ngầm
    asyncio.create_task(redis_pubsub_broadcaster(redis_client))

    # Khởi động WebSocket Server
    async with websockets.serve(lambda ws: ws_handler(ws, redis_client), "0.0.0.0", 8765):
        await asyncio.Future()  # Run forever


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Server stopped.")
