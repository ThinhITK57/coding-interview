"""
Fleet Platform — Telemetry & Repair Request Mock Producer
===========================================================
Mô phỏng dữ liệu cảm biến phát ra từ đội xe tải 50+ chiếc đang chạy trên đường
(tọa độ GPS thực tế ở TP.HCM, Bình Dương, Đồng Nai, Long An) và gửi vào Kafka:
  - Kafka topic `truck-telemetry`: Phát dữ liệu telemetry mỗi N giây (tốc độ, nhiệt độ động cơ, DTC codes,...)
  - Kafka topic `repair-request`: Ngẫu nhiên phát yêu cầu sửa chữa khi phát hiện sự cố (DTC codes nguy hiểm hoặc nút bấm từ lái xe)

Usage:
  python3 telemetry_mock_producer.py [--interval 2] [--trucks 30] [--rounds 100]
Chạy trên: Master hoặc bất kỳ node nào có kafka-python.
"""

import argparse
import json
import random
import sys
import time
from datetime import datetime

try:
    from kafka import KafkaProducer
    from kafka.errors import NoBrokersAvailable
except ImportError:
    print("Cần cài kafka-python: pip install kafka-python")
    sys.exit(1)


BOOTSTRAP_SERVERS = ["master:9092", "slave1:9092", "slave2:9092"]

# Danh sách biển số xe mẫu
TRUCK_PLATES = [
    "51C-123.45", "51C-123.46", "51C-123.47", "51D-456.78", "51D-456.79",
    "60C-789.01", "60C-789.02", "51H-234.56", "51H-234.57", "51C-567.89",
    "51F-890.12", "62C-111.22", "62C-333.44", "51C-999.01", "51C-999.02",
    "60C-777.04", "51H-666.05", "62C-555.06", "51F-444.07", "51C-333.08",
]

# Tọa độ trung tâm các khu vực (với bán kính dao động ngẫu nhiên)
LOCATIONS = [
    {"name": "Bình Tân (HCM)",    "lat": 10.7513900, "lng": 106.6063900},
    {"name": "Thủ Đức (HCM)",     "lat": 10.8485600, "lng": 106.7717200},
    {"name": "Quận 7 (HCM)",      "lat": 10.7375500, "lng": 106.7218400},
    {"name": "Thuận An (BD)",     "lat": 10.9596100, "lng": 106.6528600},
    {"name": "Bến Lức (Long An)", "lat": 10.6498200, "lng": 106.4865100},
    {"name": "Biên Hòa (Đồng Nai)","lat": 10.9431100, "lng": 106.8382800},
]

# Mã lỗi OBD-II DTC (Diagnostic Trouble Codes)
DTC_CATALOG = [
    {"code": "P0300", "category": "Động cơ",        "desc": "Bỏ lửa động cơ ngẫu nhiên (Engine Misfire)", "urgency": "HIGH"},
    {"code": "P0420", "category": "Khí thải",       "desc": "Hiệu suất bộ chuyển đổi xúc tác thấp",      "urgency": "MEDIUM"},
    {"code": "BRK-01", "category": "Phanh",          "desc": "Áp suất phanh giảm đột ngột",              "urgency": "CRITICAL"},
    {"code": "P0217", "category": "Hệ thống làm mát","desc": "Nhiệt độ nước làm mát quá cao (>105°C)",   "urgency": "HIGH"},
    {"code": "P0562", "category": "Hệ thống điện",  "desc": "Điện áp hệ thống điện thấp (<21V)",         "urgency": "MEDIUM"},
    {"code": "TIR-02", "category": "Lốp",            "desc": "Cảnh báo áp suất lốp thấp",                 "urgency": "MEDIUM"},
]


def create_producer():
    try:
        return KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",
            retries=3,
        )
    except NoBrokersAvailable:
        print(f"❌ Không thể kết nối tới Kafka brokers: {BOOTSTRAP_SERVERS}")
        sys.exit(1)


def generate_telemetry(truck_plate, base_loc):
    # Dao động vị trí GPS (~ 2-5km)
    lat = base_loc["lat"] + random.uniform(-0.03, 0.03)
    lng = base_loc["lng"] + random.uniform(-0.03, 0.03)

    # Động cơ & tốc độ
    speed = round(random.uniform(20.0, 85.0), 1)
    rpm = random.randint(1200, 2400)
    engine_temp = round(random.uniform(85.0, 102.0), 1)

    # 10% cơ hội phát hiện lỗi DTC
    has_dtc = random.random() < 0.10
    dtc_codes = []
    status = "NORMAL"

    if has_dtc:
        dtc_obj = random.choice(DTC_CATALOG)
        dtc_codes.append(dtc_obj["code"])
        status = "WARNING" if dtc_obj["urgency"] != "CRITICAL" else "ALERT"

    payload = {
        "truck_id": f"TRK-{truck_plate.replace('.', '').replace('-', '')}",
        "truck_plate": truck_plate,
        "driver_id": f"DRV-{random.randint(1000, 1050)}",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "location": {
            "latitude": round(lat, 7),
            "longitude": round(lng, 7),
            "altitude_m": round(random.uniform(5.0, 25.0), 1),
            "heading_deg": round(random.uniform(0.0, 360.0), 1),
        },
        "metrics": {
            "speed_kmh": speed,
            "engine_rpm": rpm,
            "engine_temp_c": engine_temp,
            "fuel_level_pct": round(random.uniform(20.0, 95.0), 1),
            "oil_pressure_psi": round(random.uniform(35.0, 50.0), 1),
            "battery_voltage": round(random.uniform(23.5, 25.5), 1),
            "odometer_km": round(random.uniform(50000.0, 250000.0), 1),
        },
        "dtc_codes": dtc_codes,
        "status": status,
    }

    return payload, has_dtc, dtc_codes


def generate_repair_request(truck_plate, base_loc, dtc_code=None):
    lat = base_loc["lat"] + random.uniform(-0.01, 0.01)
    lng = base_loc["lng"] + random.uniform(-0.01, 0.01)

    if dtc_code:
        dtc_info = next((d for d in DTC_CATALOG if d["code"] == dtc_code), DTC_CATALOG[0])
        category = dtc_info["category"]
        desc = dtc_info["desc"]
        urgency = dtc_info["urgency"]
        dtcs = [dtc_code]
    else:
        category = random.choice(["Phanh", "Lốp", "Động cơ", "Hệ thống điện"])
        desc = f"Lái xe báo sự cố {category.lower()} cần kiểm tra gấp"
        urgency = random.choice(["MEDIUM", "HIGH", "CRITICAL"])
        dtcs = []

    payload = {
        "request_id": f"REQ-{datetime.now().strftime('%Y%m%d%H%M%S')}-{random.randint(100, 999)}",
        "customer_id": random.randint(1, 8),
        "truck_plate": truck_plate,
        "requested_at": datetime.utcnow().isoformat() + "Z",
        "location": {
            "latitude": round(lat, 7),
            "longitude": round(lng, 7),
        },
        "issue_category": category,
        "issue_description": desc,
        "urgency": urgency,
        "dtc_codes": dtcs,
    }

    return payload


def main():
    parser = argparse.ArgumentParser(description="Fleet Platform — Telemetry & Repair Mock Producer")
    parser.add_argument("--interval", type=float, default=2.0, help="Thời gian chờ giữa các round (giây)")
    parser.add_argument("--trucks", type=int, default=15, help="Số lượng xe phát telemetry mỗi round")
    parser.add_argument("--rounds", type=int, default=100, help="Số round chạy (default: 100)")
    args = parser.parse_args()

    print("=" * 65)
    print(" Fleet Platform — Telemetry & Repair Request Mock Producer")
    print(f" Kafka Brokers : {BOOTSTRAP_SERVERS}")
    print(f" Rounds        : {args.rounds} (Interval: {args.interval}s, Trucks/round: {args.trucks})")
    print("=" * 65)

    producer = create_producer()
    truck_pool = TRUCK_PLATES[:min(args.trucks, len(TRUCK_PLATES))]

    telemetry_count = 0
    repair_count = 0

    try:
        for r in range(1, args.rounds + 1):
            print(f"\n--- Round {r}/{args.rounds} [{datetime.now().strftime('%H:%M:%S')}] ---")

            for plate in truck_pool:
                base_loc = random.choice(LOCATIONS)
                t_payload, has_dtc, dtc_codes = generate_telemetry(plate, base_loc)

                # Send telemetry
                producer.send("truck-telemetry", key=plate, value=t_payload)
                telemetry_count += 1

                # If warning/alert DTC detected or random 5% chance, also generate repair-request
                if has_dtc or random.random() < 0.05:
                    dtc_code = dtc_codes[0] if dtc_codes else None
                    r_payload = generate_repair_request(plate, base_loc, dtc_code)
                    producer.send("repair-request", key=plate, value=r_payload)
                    repair_count += 1
                    print(f"  [REPAIR REQ] {plate} ({base_loc['name']}) → Category: {r_payload['issue_category']} | Urgency: {r_payload['urgency']}")

            producer.flush()
            print(f"  [TELEMETRY] Sent {len(truck_pool)} telemetry messages. (Total sent: {telemetry_count})")

            if r < args.rounds:
                time.sleep(args.interval)

    except KeyboardInterrupt:
        print("\n Stopped by user.")
    finally:
        producer.close()

    print("\n" + "=" * 65)
    print(f" Summary: Sent {telemetry_count} telemetry messages, {repair_count} repair requests.")
    print("=" * 65)


if __name__ == "__main__":
    main()
