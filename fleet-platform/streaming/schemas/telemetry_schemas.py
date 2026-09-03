"""
Fleet Platform — Streaming Schemas
==================================
Định nghĩa PySpark Schema cho JSON payloads từ Kafka topics:
  1. `truck-telemetry`: Dữ liệu cảm biến xe tải theo thời gian thực (GPS, tốc độ, OBD-II DTC codes,...)
  2. `repair-request`: Yêu cầu báo hỏng / đặt lịch từ app mobile
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    LongType,
    TimestampType,
    ArrayType,
)

# =============================================================
# 1. Telemetry Schema (Kafka topic: truck-telemetry)
# =============================================================
# Sample JSON payload:
# {
#   "truck_id": "TRK-51C-12345",
#   "truck_plate": "51C-123.45",
#   "driver_id": "DRV-1029",
#   "timestamp": "2024-07-29T21:45:00.123Z",
#   "location": {
#     "latitude": 10.7513900,
#     "longitude": 106.6063900,
#     "altitude_m": 12.5,
#     "heading_deg": 180.0
#   },
#   "metrics": {
#     "speed_kmh": 45.5,
#     "engine_rpm": 1800,
#     "engine_temp_c": 92.0,
#     "fuel_level_pct": 65.0,
#     "oil_pressure_psi": 40.0,
#     "battery_voltage": 24.2,
#     "odometer_km": 125430.5
#   },
#   "dtc_codes": ["P0300", "P0420"],
#   "status": "NORMAL"
# }

TELEMETRY_SCHEMA = StructType([
    StructField("truck_id", StringType(), True),
    StructField("truck_plate", StringType(), True),
    StructField("driver_id", StringType(), True),
    StructField("timestamp", StringType(), True),  # Raw string to be parsed as TimestampType
    StructField("location", StructType([
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("altitude_m", DoubleType(), True),
        StructField("heading_deg", DoubleType(), True),
    ]), True),
    StructField("metrics", StructType([
        StructField("speed_kmh", DoubleType(), True),
        StructField("engine_rpm", IntegerType(), True),
        StructField("engine_temp_c", DoubleType(), True),
        StructField("fuel_level_pct", DoubleType(), True),
        StructField("oil_pressure_psi", DoubleType(), True),
        StructField("battery_voltage", DoubleType(), True),
        StructField("odometer_km", DoubleType(), True),
    ]), True),
    StructField("dtc_codes", ArrayType(StringType()), True),
    StructField("status", StringType(), True),
])


# =============================================================
# 2. Repair Request Schema (Kafka topic: repair-request)
# =============================================================
# Sample JSON payload:
# {
#   "request_id": "REQ-20240729-0012",
#   "customer_id": 1,
#   "truck_plate": "51C-123.45",
#   "requested_at": "2024-07-29T21:45:00.000Z",
#   "location": {
#     "latitude": 10.7513900,
#     "longitude": 106.6063900
#   },
#   "issue_category": "Phanh",
#   "issue_description": "Phanh phát ra tiếng kêu lạ khi rẽ",
#   "urgency": "HIGH",
#   "dtc_codes": ["P0300"]
# }

REPAIR_REQUEST_SCHEMA = StructType([
    StructField("request_id", StringType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("truck_plate", StringType(), True),
    StructField("requested_at", StringType(), True),
    StructField("location", StructType([
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
    ]), True),
    StructField("issue_category", StringType(), True),
    StructField("issue_description", StringType(), True),
    StructField("urgency", StringType(), True),
    StructField("dtc_codes", ArrayType(StringType()), True),
])
