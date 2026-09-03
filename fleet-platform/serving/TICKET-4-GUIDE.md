# Ticket 4 — Multi-Role Redis Sync & Matching Engine (GEO + Hash + Atomic Reservation)

> **Mục tiêu**:
> 1. **Redis CDC Sync Worker**: Đồng bộ liên tục dữ liệu Master Data từ Kafka CDC topics (`heads`, `parts_inventory`) vào Redis Data Structure (GEO `geo:heads`, Hash `head:info:{id}`, Hash `head:parts:{head_id}`).
> 2. **Head Matching Engine**: Truy vấn vị trí trạm sửa chữa gần nhất trong bán kính R km (dùng `GEOSEARCH`) + kiểm tra linh kiện còn tồn kho (Hash) trong thời gian **sub-millisecond (< 2ms)** mà KHÔNG chạm vào PostgreSQL OLTP.
> 3. **Atomic Slot Reservation**: Kiểm soát tranh chấp chỗ (Concurrency Control) khi nhiều lái xe cùng đặt 1 trạm/giờ bằng **Redis Lua Script** đảm bảo 100% không bị Overbooking.

---

## Cấu trúc thư mục Ticket 4

```
fleet-platform/
└── serving/
    ├── workers/
    │   └── redis_cdc_sync.py        ← CDC Worker sync data từ Kafka CDC -> Redis
    ├── services/
    │   ├── head_matching_service.py ← Matching Engine (Redis GEOSEARCH + Hash)
    │   └── slot_reservation.py      ← Concurrency Control (Lua Script + TTL)
    ├── tests/
    │   └── test_redis_matching.py   ← Benchmark Latency & Test 50 Concurrent Threads
    └── TICKET-4-GUIDE.md             ← Hướng dẫn chạy & Interview Q&A
```

---

## 1. Yêu cầu môi trường (Pre-requisites)

1. **Redis Server** đang chạy trên máy `master` (port 6379):
   ```bash
   redis-cli -h master ping
   # Trả về: PONG
   ```
2. Python package `redis` và `kafka-python` đã cài đặt:
   ```bash
   pip install redis kafka-python
   ```
3. Ticket 2 (CDC Pipeline) đang hoạt động và đẩy CDC events vào Kafka.

---

## 2. Bước 1: Khởi động Redis CDC Sync Worker

Trên máy `master`:

```bash
cd /path/to/fleet-platform/serving/workers

# Chạy worker trong background hoặc terminal riêng:
python3 redis_cdc_sync.py
```

**Output mong đợi khi CDC events đến:**
```
============================================================
 Fleet Platform — Redis CDC Sync Worker
 Kafka Brokers : ['master:9092', 'slave1:9092', 'slave2:9092']
 Redis Server  : master:6379
============================================================
✅ Redis connection established.
✅ Subscribed to CDC topics. Starting sync loop...

  [HEAD SYNC] R head_1 (Trạm Bình Tân) → GEO & Hash updated.
  [PARTS SYNC] R Head #1, Comp #1 → Qty: 20, Price: 850,000đ
  ...
```

---

## 3. Bước 2: Kiểm tra dữ liệu trên Redis qua `redis-cli`

Mở terminal mới trên `master`:

```bash
redis-cli -h master
```

```redis
# 1. Kiểm tra danh sách Head trong Redis GEO
GEOSEARCH geo:heads FROMLONLAT 106.6063900 10.7513900 BYRADIUS 15 km WITHDIST

# 2. Kiểm tra thông tin chi tiết Head #1 trong Hash
HGETALL head:info:1

# 3. Kiểm tra tồn kho linh kiện phanh (comp_1) tại Head #1
HGET head:parts:1 comp_1
```

---

## 4. Bước 4: Chạy Unit & Concurrency Test Suite

Chạy test suite kiểm tra hiệu năng matching & tính năng chống overbooking:

```bash
cd /path/to/fleet-platform/serving/tests
python3 test_redis_matching.py
```

**Output mong đợi:**
```
============================================================
 Fleet Platform — Serving Layer Tests
============================================================

[Test 1] Head Matching Latency Benchmark
  ✓ Matching Executed in 0.845 ms
  ✓ Candidate Heads Found: 3
  ✅ Latency Benchmark PASSED (< 5ms)

[Test 2] Concurrency Overbooking Prevention (50 Parallel Requests)
  Total Requests Executed : 50
  Successful Reservations  : 4 (Capacity limit: 4)
  Rejected Requests        : 46
  ✅ Concurrency Control Test PASSED: Zero Overbooking Guaranteed!

============================================================
 All Serving Layer Tests Completed Successfully.
============================================================
```
