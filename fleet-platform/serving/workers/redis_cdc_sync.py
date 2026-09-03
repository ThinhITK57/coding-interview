"""
Fleet Platform — Redis CDC Sync Worker
======================================
Worker này chạy liên tục tiêu thụ CDC events từ Kafka CDC topics:
  1. `fleet-cdc.public.heads`          → Sync vào Redis GEO (`geo:heads`) & Redis Hash (`head:info:{id}`)
  2. `fleet-cdc.public.parts_inventory` → Sync vào Redis Hash (`head:parts:{head_id}`)

Nhờ REPLICA IDENTITY FULL từ Postgres:
  - Khi DELETE row trong `heads`: có `before.longitude` & `before.latitude` để xóa khỏi Redis GEO (`ZREM geo:heads head_{id}`).
  - Khi DELETE/UPDATE row trong `parts_inventory`: có `before` state để xóa/cập nhật chính xác.

Usage:
  python3 redis_cdc_sync.py
"""

import json
import sys
import os
import time
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import redis

# Config
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "master:9092,slave1:9092,slave2:9092").split(",")
REDIS_HOST = os.getenv("REDIS_HOST", "master")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

CDC_TOPICS = [
    "fleet-cdc.public.heads",
    "fleet-cdc.public.parts_inventory",
]


def get_redis_client():
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)


def process_head_event(r: redis.Redis, op: str, before: dict, after: dict):
    """Xử lý CDC event cho bảng `heads`."""
    if op in ("c", "u", "r"):  # create, update, snapshot read
        head_id = after["id"]
        lat = float(after["latitude"])
        lng = float(after["longitude"])
        name = after["name"]
        member_key = f"head_{head_id}"

        # 1. Update Redis GEO
        r.geoadd("geo:heads", (lng, lat, member_key))

        # 2. Update Redis Hash for Head info
        head_hash_key = f"head:info:{head_id}"
        r.hset(head_hash_key, mapping={
            "id": str(head_id),
            "name": name,
            "address": after.get("address", ""),
            "city": after.get("city", ""),
            "latitude": str(lat),
            "longitude": str(lng),
            "capacity_slots": str(after.get("capacity_slots", 4)),
            "is_active": "1" if after.get("is_active", True) else "0",
        })
        print(f"  [HEAD SYNC] {op.upper()} head_{head_id} ({name}) → GEO & Hash updated.")

    elif op == "d":  # delete
        head_id = before["id"]
        member_key = f"head_{head_id}"

        # Delete from GEO
        r.zrem("geo:heads", member_key)

        # Delete Head info Hash
        r.delete(f"head:info:{head_id}")
        # Delete parts Hash
        r.delete(f"head:parts:{head_id}")
        print(f"  [HEAD SYNC] DELETE head_{head_id} → Removed from Redis.")


def process_parts_inventory_event(r: redis.Redis, op: str, before: dict, after: dict):
    """Xử lý CDC event cho bảng `parts_inventory`."""
    if op in ("c", "u", "r"):
        head_id = after["head_id"]
        comp_id = after["component_id"]
        qty = int(after["quantity"])
        unit_price = float(after["unit_price"])

        parts_hash_key = f"head:parts:{head_id}"
        field_name = f"comp_{comp_id}"

        parts_data = json.dumps({
            "component_id": comp_id,
            "quantity": qty,
            "unit_price": unit_price,
            "updated_at": after.get("updated_at", str(int(time.time())))
        })

        r.hset(parts_hash_key, field_name, parts_data)
        print(f"  [PARTS SYNC] {op.upper()} Head #{head_id}, Comp #{comp_id} → Qty: {qty}, Price: {unit_price:,.0f}đ")

    elif op == "d":
        head_id = before["head_id"]
        comp_id = before["component_id"]
        parts_hash_key = f"head:parts:{head_id}"
        field_name = f"comp_{comp_id}"

        r.hdel(parts_hash_key, field_name)
        print(f"  [PARTS SYNC] DELETE Head #{head_id}, Comp #{comp_id} → Field removed.")


def main():
    print("=" * 60)
    print(" Fleet Platform — Redis CDC Sync Worker")
    print(f" Kafka Brokers : {BOOTSTRAP_SERVERS}")
    print(f" Redis Server  : {REDIS_HOST}:{REDIS_PORT}")
    print("=" * 60)

    r = get_redis_client()
    try:
        r.ping()
        print("✅ Redis connection established.")
    except Exception as e:
        print(f"❌ Cannot connect to Redis at {REDIS_HOST}:{REDIS_PORT}: {e}")
        sys.exit(1)

    try:
        consumer = KafkaConsumer(
            *CDC_TOPICS,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="redis-cdc-sync-group",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")) if m else None,
        )
    except NoBrokersAvailable:
        print("❌ Cannot connect to Kafka brokers.")
        sys.exit(1)

    print("✅ Subscribed to CDC topics. Starting sync loop...\n")

    for msg in consumer:
        val = msg.value
        if not val:
            continue

        payload = val.get("payload", val)
        op = payload.get("op")
        before = payload.get("before")
        after = payload.get("after")

        topic = msg.topic
        if topic == "fleet-cdc.public.heads":
            process_head_event(r, op, before, after)
        elif topic == "fleet-cdc.public.parts_inventory":
            process_parts_inventory_event(r, op, before, after)


if __name__ == "__main__":
    main()
