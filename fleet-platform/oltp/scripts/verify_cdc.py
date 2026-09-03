"""
Fleet Platform — CDC Verification Script
=========================================
Đọc CDC events từ Kafka topics, verify:
  1. Debezium connector đang chạy
  2. CDC events có đúng format (before/after images)
  3. REPLICA IDENTITY FULL → before image có đầy đủ columns
  4. Đếm số events per topic

Usage: python3 verify_cdc.py [--timeout 30]
Chạy trên: master (hoặc node có kafka-python)
"""

import argparse
import json
import sys
import time

try:
    from kafka import KafkaConsumer, KafkaAdminClient
    from kafka.errors import NoBrokersAvailable
except ImportError:
    print("Cần cài kafka-python: pip install kafka-python")
    sys.exit(1)

try:
    import requests
except ImportError:
    print("Cần cài requests: pip install requests")
    sys.exit(1)


BOOTSTRAP_SERVERS = "master:9092,slave1:9092,slave2:9092"
CONNECT_URL = "http://master:8083"

CDC_TOPICS = [
    "fleet-cdc.public.heads",
    "fleet-cdc.public.parts_inventory",
    "fleet-cdc.public.customers",
    "fleet-cdc.public.work_orders",
    "fleet-cdc.public.invoices",
    "fleet-cdc.public.components",
    "fleet-cdc.public.work_order_items",
]

PASS = 0
FAIL = 0


def check(name, condition):
    global PASS, FAIL
    if condition:
        print(f"  ✓ {name}")
        PASS += 1
    else:
        print(f"  ✗ {name}")
        FAIL += 1


def verify_connector():
    """Kiểm tra Debezium connector đang RUNNING."""
    print("\n[1/4] Debezium Connector Status")
    try:
        r = requests.get(f"{CONNECT_URL}/connectors/fleet-cdc-connector/status", timeout=5)
        if r.status_code == 200:
            data = r.json()
            state = data.get("connector", {}).get("state", "UNKNOWN")
            check(f"Connector state: {state}", state == "RUNNING")

            tasks = data.get("tasks", [])
            for task in tasks:
                task_state = task.get("state", "UNKNOWN")
                task_id = task.get("id", "?")
                check(f"Task {task_id} state: {task_state}", task_state == "RUNNING")
        else:
            check(f"Connector API response: {r.status_code}", False)
    except Exception as e:
        check(f"Connect to Kafka Connect REST: {e}", False)


def verify_topics():
    """Kiểm tra CDC topics tồn tại trên Kafka."""
    print("\n[2/4] CDC Topics on Kafka")
    try:
        admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
        existing = admin.list_topics()
        for topic in CDC_TOPICS:
            check(f"Topic exists: {topic}", topic in existing)
        admin.close()
    except NoBrokersAvailable:
        check("Connect to Kafka brokers", False)


def verify_events(timeout_seconds):
    """Đọc CDC events, kiểm tra format và before/after images."""
    print(f"\n[3/4] CDC Event Format (consuming for {timeout_seconds}s)")

    try:
        consumer = KafkaConsumer(
            *CDC_TOPICS,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            auto_offset_reset="earliest",
            consumer_timeout_ms=timeout_seconds * 1000,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")) if m else None,
            key_deserializer=lambda m: json.loads(m.decode("utf-8")) if m else None,
            group_id="fleet-cdc-verify-" + str(int(time.time())),
        )
    except NoBrokersAvailable:
        check("Connect to Kafka for consuming", False)
        return {}

    topic_counts = {t: {"total": 0, "insert": 0, "update": 0, "delete": 0} for t in CDC_TOPICS}
    sample_update = None

    for msg in consumer:
        topic = msg.topic
        if topic not in topic_counts:
            continue

        value = msg.value
        if value is None:
            continue

        # Debezium envelope có: schema + payload
        payload = value.get("payload", value)
        op = payload.get("op", "?")

        topic_counts[topic]["total"] += 1
        if op == "c":
            topic_counts[topic]["insert"] += 1
        elif op == "u":
            topic_counts[topic]["update"] += 1
            if sample_update is None:
                sample_update = {"topic": topic, "payload": payload}
        elif op == "d":
            topic_counts[topic]["delete"] += 1

    consumer.close()

    # Verify có events
    total_all = sum(v["total"] for v in topic_counts.values())
    check(f"Total CDC events received: {total_all}", total_all > 0)

    # Verify REPLICA IDENTITY FULL — before image phải có nhiều hơn chỉ PK
    if sample_update:
        before = sample_update["payload"].get("before", {})
        after = sample_update["payload"].get("after", {})
        before_keys = len(before) if before else 0
        after_keys = len(after) if after else 0

        check(
            f"REPLICA IDENTITY FULL — before image has {before_keys} fields (topic: {sample_update['topic']})",
            before_keys > 1  # > 1 nghĩa là có nhiều hơn chỉ PK
        )
        check(
            f"After image has {after_keys} fields",
            after_keys > 1
        )

        print(f"\n  Sample UPDATE event from {sample_update['topic']}:")
        print(f"    before keys: {list(before.keys()) if before else 'null'}")
        print(f"    after keys:  {list(after.keys()) if after else 'null'}")
    else:
        print("  ⚠ No UPDATE events found — run data_producer.py first to generate UPDATEs")

    return topic_counts


def print_summary(topic_counts):
    """In bảng tổng hợp."""
    print("\n[4/4] Event Count Summary")
    print(f"  {'Topic':<40} {'Total':>6} {'INSERT':>7} {'UPDATE':>7} {'DELETE':>7}")
    print("  " + "-" * 70)
    for topic in CDC_TOPICS:
        c = topic_counts.get(topic, {"total": 0, "insert": 0, "update": 0, "delete": 0})
        print(f"  {topic:<40} {c['total']:>6} {c['insert']:>7} {c['update']:>7} {c['delete']:>7}")


def main():
    parser = argparse.ArgumentParser(description="Fleet Platform — CDC Verification")
    parser.add_argument("--timeout", type=int, default=30, help="Seconds to consume events (default: 30)")
    args = parser.parse_args()

    print("=" * 60)
    print(" Fleet Platform — CDC Pipeline Verification")
    print("=" * 60)

    verify_connector()
    verify_topics()
    topic_counts = verify_events(args.timeout)
    print_summary(topic_counts)

    print("\n" + "=" * 60)
    print(f" Results: {PASS} passed, {FAIL} failed")
    print("=" * 60)

    if FAIL > 0:
        print(" ⚠ Some checks FAILED. Review output above.")
        sys.exit(1)
    else:
        print(" ✅ CDC pipeline is working correctly.")
        sys.exit(0)


if __name__ == "__main__":
    main()
