"""
Fleet Platform — WebSocket Push-Based Serving Backend
=====================================================
Backend Async Python đóng vai trò cầu nối Push-based giữa Redis và Dashboard UI:
  1. Subscribes vào Redis Pub/Sub channel `channel:report-updates`.
  2. Khi Airflow / Spark batch job hoàn tất và phát tín hiệu lên Redis Pub/Sub, backend tự động đọc dữ liệu mới nhất từ Redis String Keys (`report:agg:*`) và **PUSH (chủ động đẩy)** xuống tất cả Dashboard clients đang kết nối qua WebSocket.
  3. Khi một Dashboard client mới kết nối (hoặc Reconnect), Backend ngay lập tức gửi Dữ liệu Trạng thái Hiện tại (Initial State Sync) để Dashboard hiển thị ngay lập tức mà không phải chờ event tiếp theo.

Usage:
  python3 websocket_serving_backend.py [--port 8765] [--redis-host master]
"""

import asyncio
import argparse
import json
import logging
import os
import sys
import websockets
import redis.asyncio as aioredis

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

# Store all active websocket client connections
CONNECTED_CLIENTS = set()

REDIS_HOST = os.getenv("REDIS_HOST", "master")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
PUBSUB_CHANNEL = "channel:report-updates"


async def fetch_current_state(redis_client) -> dict:
    """Lấy dữ liệu báo cáo mới nhất từ tất cả các Redis String Keys."""
    states = {}
    for gran in ["weekly", "monthly", "quarterly", "fiscal_year"]:
        key = f"report:agg:{gran}"
        val = await redis_client.get(key)
        if val:
            try:
                states[gran] = json.loads(val)
            except Exception:
                states[gran] = {"raw": val}
    return states


async def handle_client_connection(websocket, redis_client):
    """Xử lý vòng đời kết nối của từng WebSocket client (Dashboard)."""
    client_addr = websocket.remote_address
    logging.info(f"🔌 New Dashboard WebSocket Client connected from {client_addr}")
    CONNECTED_CLIENTS.add(websocket)

    try:
        # 1. Immediate Initial State Sync
        current_state = await fetch_current_state(redis_client)
        init_payload = {
            "type": "INITIAL_STATE",
            "timestamp": asyncio.get_event_loop().time(),
            "data": current_state
        }
        await websocket.send(json.dumps(init_payload))
        logging.info(f"📤 Sent INITIAL_STATE to client {client_addr}")

        # 2. Keep-alive listener for incoming messages / ping from client
        async for message in websocket:
            logging.info(f"📩 Received message from {client_addr}: {message}")
            if message == "ping":
                await websocket.send(json.dumps({"type": "pong"}))

    except websockets.exceptions.ConnectionClosed:
        logging.info(f"🔌 Client {client_addr} disconnected gracefully.")
    except Exception as e:
        logging.error(f"⚠ Error handling client {client_addr}: {e}")
    finally:
        CONNECTED_CLIENTS.remove(websocket)


async def redis_pubsub_listener(redis_client):
    """Lắng nghe liên tục Redis Pub/Sub channel và Broadcast message tới tất cả WebSockets."""
    pubsub = redis_client.pubsub()
    await pubsub.subscribe(PUBSUB_CHANNEL)
    logging.info(f"📡 Redis Pub/Sub listener subscribed to '{PUBSUB_CHANNEL}'. Waiting for events...")

    async for message in pubsub.listen():
        if message["type"] == "message":
            raw_data = message["data"]
            logging.info(f"⚡ Redis Pub/Sub Trigger Received: {raw_data}")

            try:
                event_info = json.loads(raw_data)
                granularity = event_info.get("granularity", "monthly")

                # Fetch updated full data from Redis
                updated_val = await redis_client.get(f"report:agg:{granularity}")
                updated_data = json.loads(updated_val) if updated_val else event_info

                broadcast_payload = json.dumps({
                    "type": "REPORT_UPDATE",
                    "granularity": granularity,
                    "event": event_info.get("event"),
                    "data": updated_data
                })

                # Broadcast to all connected websocket clients
                if CONNECTED_CLIENTS:
                    logging.info(f"📢 Broadcasting PUSH update to {len(CONNECTED_CLIENTS)} WebSocket clients...")
                    websockets.broadcast(CONNECTED_CLIENTS, broadcast_payload)
                else:
                    logging.info("ℹ No WebSocket clients currently connected. Metric saved in Redis.")

            except Exception as e:
                logging.error(f"⚠ Failed to parse/broadcast Pub/Sub message: {e}")


async def main():
    parser = argparse.ArgumentParser(description="Fleet Platform — Push-Based WebSocket Serving Backend")
    parser.add_argument("--port", type=int, default=8765, help="WebSocket Server Port (default: 8765)")
    parser.add_argument("--host", type=str, default="0.0.0.0", help="WebSocket Bind Host (default: 0.0.0.0)")
    args = parser.parse_args()

    logging.info("=" * 65)
    logging.info(" Fleet Platform — WebSocket Serving Backend")
    logging.info(f" Listening on : ws://{args.host}:{args.port}")
    logging.info(f" Redis Host   : {REDIS_HOST}:{REDIS_PORT}")
    logging.info("=" * 65)

    redis_client = aioredis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)

    # Test Redis connection
    try:
        await redis_client.ping()
        logging.info("✅ Connected to Redis successfully.")
    except Exception as e:
        logging.error(f"❌ Cannot connect to Redis: {e}")
        sys.exit(1)

    # Start Redis Pub/Sub listener as background task
    asyncio.create_task(redis_pubsub_listener(redis_client))

    # Start WebSocket Server
    async with websockets.serve(lambda ws: handle_client_connection(ws, redis_client), args.host, args.port):
        await asyncio.Future()  # Run forever


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("\n Server stopped by user.")
