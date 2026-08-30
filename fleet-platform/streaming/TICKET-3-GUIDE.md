# Ticket 3 — Real-Time Telemetry & Repair Request Streaming (Spark Structured Streaming)

> **Mục tiêu**: Tiêu thụ dữ liệu streaming thời gian thực từ 2 Kafka topics (`truck-telemetry` và `repair-request`), parse JSON theo schema chuẩn hóa, và ghi dữ liệu dưới dạng **Parquet partitioned theo `year/month/day`** xuống **HDFS Data Lake** (`hdfs:///fleet-datalake/raw/...`) với cơ chế Checkpointing đảm bảo tính chịu lỗi.

---

## Cấu trúc thư mục Ticket 3

```
fleet-platform/
└── streaming/
    ├── schemas/
    │   └── telemetry_schemas.py           ← PySpark StructType schemas cho Telemetry & Repair
    ├── jobs/
    │   └── streaming_telemetry_ingestion.py ← PySpark Structured Streaming Job (Ghi HDFS)
    ├── producers/
    │   └── telemetry_mock_producer.py      ← Mock producer đẩy dữ liệu GPS/Telemetry vào Kafka
    └── TICKET-3-GUIDE.md                   ← Hướng dẫn chạy & verify
```

---

## 1. Yêu cầu môi trường (Pre-requisites)

1. **HDFS Multinode** đang chạy và đường dẫn `/fleet-datalake` đã tồn tại trên HDFS:
   ```bash
   hdfs dfs -mkdir -p /fleet-datalake/raw/telemetry
   hdfs dfs -mkdir -p /fleet-datalake/raw/repair_requests
   hdfs dfs -mkdir -p /fleet-datalake/checkpoints
   ```
2. **Kafka Multi-Broker Cluster** đang chạy (master, slave1, slave2) và topics `truck-telemetry`, `repair-request` đã được tạo.
3. Python package `kafka-python` đã được cài đặt trên node chạy producer/spark submit.

---

## 2. Bước 1: Khởi động Telemetry Mock Producer

Trên máy `master` (hoặc máy ảo có kết nối Kafka):

```bash
cd /path/to/fleet-platform/streaming/producers

# Cài kafka-python nếu chưa có
pip install kafka-python

# Chạy producer trong background hoặc ở terminal 1:
python3 telemetry_mock_producer.py --interval 2 --trucks 20 --rounds 200
```

**Output mong đợi:**
```
--- Round 1/200 [22:15:00] ---
  [REPAIR REQ] 51C-123.45 (Bình Tân (HCM)) → Category: Phanh | Urgency: HIGH
  [TELEMETRY] Sent 20 telemetry messages. (Total sent: 20)
```

---

## 3. Bước 2: Chạy Spark Structured Streaming Ingestion Job

Trên máy `master`:

```bash
cd /path/to/fleet-platform/streaming

# Submit PySpark job lên Spark Standalone Cluster (hoặc local mode)
spark-submit \
  --master spark://master:7077 \
  --deploy-mode client \
  --driver-memory 1g \
  --executor-memory 1g \
  --executor-cores 1 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  jobs/streaming_telemetry_ingestion.py
```

> **Lưu ý về Package Kafka**:
> PySpark cần package `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1` (phù hợp với Spark 3.5.x & Scala 2.12). Spark sẽ tự tải package này từ Maven Central trong lần chạy đầu tiên.

---

## 4. Bước 3: Kiểm tra & Verify Dữ liệu trên HDFS Data Lake

Mở terminal mới trên `master`:

### 4.1 Kiểm tra cấu trúc thư mục Partitioning trên HDFS

```bash
# Kiểm tra thư mục telemetry
hdfs dfs -ls /fleet-datalake/raw/telemetry

# Output sẽ hiển thị dạng:
# drwxr-xr-x   - aiguystory supergroup          0 2026-07-29 22:16 /fleet-datalake/raw/telemetry/year=2026

# Kiểm tra đến level day:
hdfs dfs -ls -R /fleet-datalake/raw/telemetry
```

### 4.2 Kiểm tra file Parquet & Checkpoints

```bash
# Kiểm tra file parquet đã ghi
hdfs dfs -ls /fleet-datalake/raw/telemetry/year=2026/month=07/day=29/

# Output hiển thị file Snappy Parquet:
# -rw-r--r--   3 aiguystory supergroup      15420 2026-07-29 22:16 /fleet-datalake/raw/telemetry/year=2026/month=07/day=29/part-00000-....snappy.parquet

# Kiểm tra Checkpoint directory
hdfs dfs -ls /fleet-datalake/checkpoints/telemetry
```

### 4.3 Đọc thử nội dung Parquet từ PySpark Shell

```bash
pyspark --master local[2]
```

Trong PySpark Shell:
```python
# Đọc raw telemetry data từ HDFS
df = spark.read.parquet("hdfs://master:9000/fleet-datalake/raw/telemetry")
df.printSchema()
df.show(5, truncate=False)

# Đếm tổng số bản ghi telemetry đã lưu
print("Total Telemetry Rows:", df.count())

# Đọc raw repair requests
df_repair = spark.read.parquet("hdfs://master:9000/fleet-datalake/raw/repair_requests")
df_repair.show(5, truncate=False)
```
