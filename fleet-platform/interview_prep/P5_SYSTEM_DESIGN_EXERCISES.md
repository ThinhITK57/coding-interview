# 🏗️ Pillar 5: System Design Exercises — Ngày 8-9

> **Mục tiêu**: Thành thạo khung trả lời System Design trong 45 phút, áp dụng vào 3 bài toán
> thực tế của Data Engineer. Mỗi bài đều đi từ estimation → architecture → deep-dive → trade-off.

---

## Phần mở đầu: Back-of-Envelope Estimation Cheat Sheet

### 📐 Latency Numbers (cần thuộc lòng)

| Thao tác                    | Latency       | Ghi nhớ                          |
|-----------------------------|---------------|-----------------------------------|
| L1 cache reference          | **1 ns**      | Nhanh nhất                       |
| L2 cache reference          | ~4 ns         |                                   |
| RAM reference               | **100 ns**    | ~100× chậm hơn L1               |
| SSD random read             | **150 μs**    | ~1500× chậm hơn RAM             |
| HDD seek                    | **10 ms**     | ~67× chậm hơn SSD               |
| Network trong cùng DC       | **0.5 ms**    | Round-trip TCP                    |
| Network cross-region        | **100 ms**    | US-East ↔ EU                     |
| Kafka publish (ack=1)       | **2-5 ms**    | Broker ghi WAL                   |
| Redis GET                   | **0.1 ms**    | In-memory, sub-millisecond       |
| PostgreSQL simple query     | **1-5 ms**    | Index lookup                      |

> [!TIP]
> **Quy tắc nhớ nhanh**: Mỗi bậc storage chậm hơn bậc trước khoảng **100-1000×**.
> `L1 (1ns) → RAM (100ns) → SSD (150μs) → HDD (10ms) → Network (100ms)`

### 📊 Throughput Numbers

| Hệ thống            | Throughput                   | Ghi chú                              |
|----------------------|------------------------------|---------------------------------------|
| Kafka                | **1M msg/s/broker**          | Với batch, compression               |
| Spark processing     | **100-500 MB/s/core**        | Tuỳ workload (ETL vs ML)             |
| HDFS write           | **100 MB/s**                 | Per DataNode, 3× replication         |
| Redis                | **100K ops/s**               | Single thread, pipeline tăng 10×     |
| PostgreSQL           | **10K TPS**                  | OLTP, tuned                          |
| Flink                | **1-5M events/s/TaskManager**| Stateful processing                  |
| Elasticsearch ingest | **10-50K docs/s**            | Tuỳ mapping complexity               |

### 💾 Storage Rules of Thumb

```
1 triệu rows  × 100 bytes/row  =  100 MB
10 triệu rows × 100 bytes/row  =    1 GB
1 tỷ rows     × 100 bytes/row  =  100 GB
1 tỷ rows     × 1 KB/row       =    1 TB
```

> [!IMPORTANT]
> **Luôn estimate cả 3 chiều**: Volume (bao nhiêu dữ liệu), Velocity (bao nhiêu/giây),
> Variety (bao nhiêu loại schema). Thiếu 1 trong 3 = ước lượng sai.

---

### 🧮 Bài tập Estimation #1: Fleet CDC Pipeline

**Đề bài**: Ước lượng storage cho CDC pipeline: 7 bảng, mỗi bảng ~1000 changes/phút,
message trung bình 500 bytes, lưu 30 ngày.

```
Bước 1: Messages/ngày
  = 7 tables × 1,000 changes/min × 60 min × 24 hr
  = 7 × 1,000 × 1,440
  = 10,080,000 msg/ngày  (≈ 10M msg/ngày)

Bước 2: Raw size/ngày
  = 10M × 500 bytes
  = 5,000,000,000 bytes
  = 5 GB/ngày (raw)

Bước 3: Với Kafka replication factor = 3
  = 5 GB × 3 = 15 GB/ngày trên Kafka cluster

Bước 4: 30 ngày retention
  Kafka: 15 GB × 30 = 450 GB
  HDFS (raw + compressed ~3:1): 5 GB × 30 / 3 = 50 GB
  HDFS (với replication 3): 50 GB × 3 = 150 GB

Bước 5: Tổng cộng
  ┌────────────────────────────────┐
  │ Kafka cluster:     ~450 GB    │
  │ HDFS (compressed): ~150 GB    │
  │ Tổng:              ~600 GB    │
  └────────────────────────────────┘
```

### 🧮 Bài tập Estimation #2: Clickstream

**Đề bài**: 5 triệu users, mỗi user 10 events/ngày, mỗi event 200 bytes. Ước lượng
throughput và storage cho 1 năm.

```
Bước 1: Events/ngày
  = 5,000,000 users × 10 events/user
  = 50,000,000 events/ngày  (50M events/ngày)

Bước 2: Peak throughput (giả sử peak = 3× average, 8 giờ cao điểm)
  Average: 50M / 86,400s ≈ 580 events/s
  Peak:    580 × 3 ≈ 1,740 events/s
  → Kafka single partition dư sức (1 broker xử lý 1M msg/s)

Bước 3: Storage/ngày
  = 50M × 200 bytes = 10 GB/ngày (raw)

Bước 4: 1 năm storage
  Raw:        10 GB × 365 = 3.65 TB
  Compressed: 3.65 TB / 3 ≈ 1.2 TB (Parquet/Snappy)
  HDFS (×3):  1.2 TB × 3  = 3.6 TB

  ┌────────────────────────────────────┐
  │ Throughput peak:  ~1,740 events/s  │
  │ Storage 1 năm:   ~3.6 TB (HDFS)   │
  │ Kafka partitions: 3-6 là đủ       │
  └────────────────────────────────────┘
```

---

## Khung trả lời chuẩn (5 bước — 45 phút)

> **Phân bổ thời gian gợi ý**: 5' → 5' → 15' → 15' → 5'

```
┌──────────────────────────────────────────────────────────┐
│           SYSTEM DESIGN FRAMEWORK (5 BƯỚC)               │
├──────────────────────────────────────────────────────────┤
│                                                          │
│  ┌─────────────────────────────────────────────────┐     │
│  │ BƯỚC 1: Clarify Requirements (5 phút)           │     │
│  │  • Functional: hệ thống CẦN làm gì?            │     │
│  │  • Non-functional: latency, throughput, SLA      │     │
│  │  • Scope: build cái gì, KHÔNG build cái gì?     │     │
│  └──────────────────┬──────────────────────────────┘     │
│                     ▼                                    │
│  ┌─────────────────────────────────────────────────┐     │
│  │ BƯỚC 2: Back-of-Envelope Estimation (5 phút)    │     │
│  │  • QPS / Events per second                      │     │
│  │  • Storage (ngày / tháng / năm)                 │     │
│  │  • Bandwidth / Network                          │     │
│  └──────────────────┬──────────────────────────────┘     │
│                     ▼                                    │
│  ┌─────────────────────────────────────────────────┐     │
│  │ BƯỚC 3: High-Level Architecture (15 phút)       │     │
│  │  Source → Ingest → Process → Store → Serve      │     │
│  │  • Vẽ diagram cho từng component                │     │
│  │  • Chọn technology cho mỗi layer               │     │
│  └──────────────────┬──────────────────────────────┘     │
│                     ▼                                    │
│  ┌─────────────────────────────────────────────────┐     │
│  │ BƯỚC 4: Deep-dive 1-2 Components (15 phút)      │     │
│  │  • Component phức tạp nhất hoặc rủi ro nhất    │     │
│  │  • Schema design, partitioning, replication     │     │
│  │  • Failure handling, exactly-once semantics     │     │
│  └──────────────────┬──────────────────────────────┘     │
│                     ▼                                    │
│  ┌─────────────────────────────────────────────────┐     │
│  │ BƯỚC 5: Trade-offs & Failure Modes (5 phút)     │     │
│  │  • Consistency vs Availability                  │     │
│  │  • Cost vs Performance                          │     │
│  │  • Failure scenarios + mitigation               │     │
│  └─────────────────────────────────────────────────┘     │
│                                                          │
└──────────────────────────────────────────────────────────┘
```

### Mẹo phỏng vấn

- **Luôn hỏi ngược interviewer** trước khi vẽ diagram. Ví dụ:
  - "Latency requirement là real-time (< 1s) hay near-real-time (< 5 min)?"
  - "Data có cần exactly-once hay at-least-once đã đủ?"
  - "Budget constraint? On-prem hay cloud?"
- **Đừng nhảy vào technology** quá sớm. Nói trước kiến trúc logic, sau đó mới map technology.
- **Nêu trade-off chủ động** — interviewer đánh giá tư duy, không phải đáp án "đúng".

---

## Đề 1: Redesign Fleet CDC Pipeline cho quy mô ×10

### 📋 Bước 1 — Clarify Requirements

**Bối cảnh hiện tại**:
- 7 bảng CDC, ~50 service heads (đầu xe)
- Debezium → Kafka → Spark batch → HDFS → Hive

**Yêu cầu mới (scale ×10)**:
- **200 bảng** CDC (từ 7 bảng)
- **500 service heads** (từ 50)
- Latency: dữ liệu available trong HDFS ≤ 15 phút
- Schema evolution: thêm/đổi cột không làm gãy pipeline
- SLA: 99.9% uptime

**Câu hỏi cần hỏi interviewer**:

| # | Câu hỏi                                         | Giả định nếu không hỏi     |
|---|--------------------------------------------------|-----------------------------|
| 1 | Có bảng nào high-volume đặc biệt không?         | Phân bố đều ~1K changes/min |
| 2 | Schema changes tần suất bao lâu?                 | ~2 lần/tuần                 |
| 3 | Downstream consumers là ai?                      | Hive queries + Dashboard    |
| 4 | Budget cho Kafka cluster?                         | On-prem, cần tối ưu cost   |

### 📐 Bước 2 — Estimation

```
Messages/ngày (scale ×10):
  = 200 tables × 1,000 changes/min × 1,440 min/day
  = 288,000,000 msg/ngày  (≈ 288M msg/ngày)
  = 288M / 86,400 ≈ 3,333 msg/s (average)
  Peak (3×): ≈ 10,000 msg/s

Storage/ngày (raw):
  = 288M × 500 bytes = 144 GB/ngày

Kafka (30 ngày, RF=3):
  = 144 GB × 30 × 3 = 12.96 TB ≈ 13 TB

HDFS (1 năm, compressed 3:1, RF=3):
  = 144 GB × 365 / 3 × 3 = 52.56 TB ≈ 53 TB

Kafka brokers needed:
  = 10K msg/s peak → 1 broker dư sức về throughput
  = 13 TB storage → 3-5 brokers (3-4 TB SSD mỗi broker)
  → Chọn 5 brokers (headroom)

  ┌───────────────────────────────────────────┐
  │ Peak throughput:  ~10K msg/s              │
  │ Kafka storage:    ~13 TB (30 ngày)        │
  │ HDFS storage:     ~53 TB/năm              │
  │ Kafka brokers:    5 (với headroom)        │
  │ Kafka partitions: 200 (1 per table)       │
  └───────────────────────────────────────────┘
```

### 🏗️ Bước 3 — High-Level Architecture

```
  ┌─────────────┐     ┌──────────────┐     ┌───────────────────┐
  │  200 Tables  │     │   Debezium   │     │  Schema Registry  │
  │  (MySQL/PG)  │────▶│  Connectors  │────▶│   (Confluent)     │
  │  500 heads   │     │  (per-DB)    │     │                   │
  └─────────────┘     └──────┬───────┘     └────────┬──────────┘
                             │                      │
                             ▼                      │ schema
                    ┌────────────────┐              │ validation
                    │     KAFKA      │◄─────────────┘
                    │  (5 brokers)   │
                    │                │
                    │ Topic Strategy:│
                    │ 1 topic/table  │
                    │ = 200 topics   │
                    └───┬────────┬───┘
                        │        │
              ┌─────────┘        └──────────┐
              ▼                             ▼
  ┌───────────────────┐         ┌───────────────────┐
  │   Spark Streaming │         │  Kafka Connect     │
  │  (micro-batch     │         │  (HDFS Sink)       │
  │   5-min windows)  │         │  [backup path]     │
  │                   │         │                    │
  │  • Dedup          │         └────────┬───────────┘
  │  • Schema enforce │                  │
  │  • Partitioning   │                  │
  └────────┬──────────┘                  │
           │                             │
           ▼                             ▼
  ┌─────────────────────────────────────────────┐
  │                  HDFS / S3                   │
  │                                             │
  │  /data/cdc/{table_name}/                    │
  │      year={yyyy}/month={MM}/day={dd}/       │
  │      hour={HH}/                             │
  │          part-00000.parquet                  │
  │                                             │
  │  Partition strategy: table → date → hour    │
  └──────────────────┬──────────────────────────┘
                     │
                     ▼
  ┌─────────────────────────────────────────────┐
  │              Hive Metastore                  │
  │  • External tables trỏ vào HDFS             │
  │  • Auto partition repair (MSCK)             │
  │  • Schema evolution via ALTER TABLE          │
  └──────────────────┬──────────────────────────┘
                     │
                     ▼
  ┌─────────────────────────────────────────────┐
  │          Downstream Consumers                │
  │  • BI Dashboard (Superset/Metabase)         │
  │  • Ad-hoc queries (Presto/Trino)            │
  │  • ML Feature Store                         │
  └─────────────────────────────────────────────┘
```

### 🔍 Bước 4 — Deep-dive: Schema Registry + Topic Strategy

#### Deep-dive A: Schema Registry

**Vấn đề**: 200 bảng, schema thay đổi ~2 lần/tuần → ~400 schema changes/tuần.
Nếu không quản lý, consumer sẽ crash khi gặp schema mới.

```
Luồng hoạt động:

  Producer (Debezium)           Schema Registry          Consumer (Spark)
       │                              │                        │
       │  1. Register schema v2       │                        │
       │─────────────────────────────▶│                        │
       │                              │                        │
       │  2. Return schema_id=42      │                        │
       │◄─────────────────────────────│                        │
       │                              │                        │
       │  3. Send message             │                        │
       │     [magic_byte|schema_id=42|│avro_payload]           │
       │─────────────────────────────────────────────────────▶ │
       │                              │                        │
       │                              │  4. Lookup schema 42   │
       │                              │◄───────────────────────│
       │                              │                        │
       │                              │  5. Return schema v2   │
       │                              │───────────────────────▶│
       │                              │                        │
       │                              │  6. Deserialize OK     │
       │                              │                        │
```

**Compatibility mode**: Chọn `BACKWARD` compatibility
- Producer thêm cột mới (có default) → Consumer cũ vẫn đọc được
- Producer xoá cột → Consumer cũ ignore
- Producer đổi type cột → **BỊ REJECT** → cần migration

```
Ví dụ schema evolution:

  v1: { vehicle_id: long, speed: float, ts: long }
          │
          │  Thêm cột "fuel_level" có default = -1
          ▼
  v2: { vehicle_id: long, speed: float, ts: long, fuel_level: float = -1 }
          │
          │  ✅ BACKWARD compatible
          │  Consumer v1 bỏ qua fuel_level
          │  Consumer v2 đọc fuel_level
```

#### Deep-dive B: Topic Strategy — 1 topic/table vs Single topic

| Tiêu chí              | 1 topic/table (200 topics)    | Single topic               |
|------------------------|-------------------------------|----------------------------|
| **Consumer isolation** | ✅ Consumer chỉ đọc table cần | ❌ Phải filter tất cả      |
| **Throughput**         | ✅ Scale partition riêng      | ⚠️ Bottleneck 1 topic      |
| **Ordering**           | ✅ Order per table partition  | ⚠️ Không guarantee cross   |
| **Ops complexity**     | ⚠️ 200 topics cần monitor    | ✅ Đơn giản                |
| **Schema Registry**    | ✅ Schema per topic = rõ ràng | ❌ Schema conflict          |
| **Kafka overhead**     | ⚠️ Nhiều partition metadata  | ✅ Ít metadata             |

> **Quyết định**: Chọn **1 topic/table** vì consumer isolation và schema clarity
> quan trọng hơn ops simplicity ở quy mô 200 bảng.

### ⚖️ Bước 5 — Trade-offs & Failure Modes

#### Cái gì VỠ trước khi scale ×10?

```
 Mức độ nghiêm trọng:  🔴 Critical  🟡 Warning  🟢 OK

 ┌─────────────────────────────────────────────────────────┐
 │ Component        │ Hiện tại (×1)   │ Scale ×10         │
 ├──────────────────┼─────────────────┼───────────────────┤
 │ Debezium         │ 🟢 7 tables     │ 🟡 200 tables     │
 │                  │                 │ Cần nhiều task    │
 ├──────────────────┼─────────────────┼───────────────────┤
 │ Kafka            │ 🟢 10M msg/day  │ 🟢 288M msg/day   │
 │                  │                 │ 5 brokers đủ     │
 ├──────────────────┼─────────────────┼───────────────────┤
 │ Spark batch      │ 🟢 5 GB/day     │ 🔴 144 GB/day     │
 │                  │                 │ Cần autoscaling   │
 ├──────────────────┼─────────────────┼───────────────────┤
 │ HDFS NameNode    │ 🟢 ~100 files   │ 🟡 Small files!   │
 │                  │                 │ Cần compaction    │
 ├──────────────────┼─────────────────┼───────────────────┤
 │ Schema mgmt      │ 🟢 Manual       │ 🔴 400 changes/wk │
 │                  │                 │ Cần Registry      │
 └─────────────────────────────────────────────────────────┘
```

**Top 3 Failure Scenarios**:

| # | Failure                              | Impact                         | Mitigation                                    |
|---|--------------------------------------|--------------------------------|-----------------------------------------------|
| 1 | Debezium connector crash             | Mất CDC events                 | Kafka Connect restart policy + monitoring     |
| 2 | Schema incompatible change deployed  | Consumer crash hàng loạt       | Schema Registry BACKWARD mode + CI/CD check   |
| 3 | HDFS small files (200 tables × 24h)  | NameNode memory OOM            | Hourly compaction job + Spark coalesce         |

---

## Đề 2: Real-time Clickstream Analytics

### 📋 Bước 1 — Clarify Requirements

**Functional**:
- Thu thập click events từ web/mobile (5M users)
- Dashboard hiển thị: page views, unique visitors, top pages, conversion funnel
- Real-time: dashboard cập nhật < 1 phút
- Historical: query dữ liệu 90 ngày gần nhất

**Non-functional**:
- Throughput: 50M events/ngày, peak ~1,740 events/s
- Latency: end-to-end < 60 giây (event xảy ra → hiển thị trên dashboard)
- Availability: 99.9%
- Data loss tolerance: at-least-once OK (click events không cần exactly-once)

### 📐 Bước 2 — Estimation

```
(Đã tính ở phần Estimation #2 phía trên)

  Events/ngày:    50M
  Peak:           ~1,740 events/s
  Storage/ngày:   10 GB (raw)
  Storage/90 ngày: 900 GB raw → ~300 GB compressed

Thêm estimation cho speed layer (Redis):
  Active aggregations = top pages (1000) + hourly UV (24) + funnels (10)
  ≈ 1,034 keys × 8 bytes avg value = ~8 KB
  → Redis memory: trivial (< 1 MB cho aggregation)

  ┌──────────────────────────────────────┐
  │ Kafka: 3 brokers (dư sức)           │
  │ Flink TaskManagers: 2-3             │
  │ Redis: 1 instance (< 1 MB data)     │
  │ HDFS: ~1 TB/năm (compressed, ×3)    │
  └──────────────────────────────────────┘
```

### 🏗️ Bước 3 — High-Level Architecture (Lambda)

```
                        ┌─────────────────────────────────────────┐
                        │          CLICKSTREAM ANALYTICS           │
                        └─────────────────────────────────────────┘

  ┌──────────┐    ┌──────────┐    ┌─────────────────────────────────────┐
  │   Web    │    │  Mobile  │    │         Event Schema (JSON)         │
  │   SDK    │    │   SDK    │    │  {                                  │
  │  (JS)    │    │ (iOS/    │    │    "user_id": "u123",               │
  └────┬─────┘    │ Android) │    │    "event": "page_view",            │
       │          └────┬─────┘    │    "page": "/checkout",             │
       │               │          │    "ts": 1691234567000,             │
       │               │          │    "session_id": "s456",            │
       ▼               ▼          │    "device": "mobile",              │
  ┌──────────────────────────┐    │    "properties": {...}              │
  │    API Gateway / LB      │    │  }                                  │
  │   (rate limit, auth)     │    └─────────────────────────────────────┘
  └────────────┬─────────────┘
               │
               ▼
  ┌──────────────────────────┐
  │         KAFKA            │
  │   topic: clickstream     │
  │   partitions: 6          │
  │   key: user_id           │
  │   retention: 7 ngày      │
  └─────┬──────────┬─────────┘
        │          │
        │          │
   ─────┘          └──────────────────────────
   │ SPEED LAYER (real-time)                  │ BATCH LAYER
   ▼                                          ▼
  ┌──────────────────────────┐    ┌──────────────────────────┐
  │   Flink / Spark          │    │      Spark Batch         │
  │   Streaming              │    │    (hourly job)          │
  │                          │    │                          │
  │  • Sliding window 1 min  │    │  • Daily aggregation     │
  │  • Count page views      │    │  • UV dedup (HyperLogLog)│
  │  • Approx UV (HLL)       │    │  • Funnel analysis       │
  │  • Top-K pages           │    │  • Sessionization        │
  └────────────┬─────────────┘    └──────────┬───────────────┘
               │                             │
               ▼                             ▼
  ┌──────────────────────────┐    ┌──────────────────────────┐
  │        REDIS             │    │     HDFS / S3            │
  │   (speed layer store)    │    │   (batch layer store)    │
  │                          │    │                          │
  │  • realtime:pv:1min      │    │  /clickstream/           │
  │  • realtime:uv:1min      │    │    year=2026/month=08/   │
  │  • realtime:top_pages    │    │      day=12/             │
  │  • TTL = 24h             │    │        events.parquet    │
  └────────────┬─────────────┘    └──────────┬───────────────┘
               │                             │
               └──────────┬──────────────────┘
                          │
                          ▼
              ┌──────────────────────────┐
              │    SERVING LAYER         │
              │                          │
              │  • Merge: Redis (recent) │
              │    + HDFS (historical)   │
              │  • API endpoint          │
              │  • Cache (Redis)         │
              └──────────┬───────────────┘
                         │
                         ▼
              ┌──────────────────────────┐
              │       DASHBOARD          │
              │    (Grafana/Superset)    │
              │                          │
              │  • Page views (real-time)│
              │  • UV (real-time + batch)│
              │  • Top pages             │
              │  • Conversion funnel     │
              └──────────────────────────┘
```

### 🔍 Bước 4 — Deep-dive: Lambda vs Kappa Architecture

#### So sánh Lambda vs Kappa

```
LAMBDA Architecture:
  ┌──────────┐    ┌──────────────────────────┐
  │          │───▶│ Speed Layer (Flink)       │──▶ Redis (real-time view)
  │  Kafka   │    └──────────────────────────┘            │
  │          │    ┌──────────────────────────┐            │
  │          │───▶│ Batch Layer (Spark)       │──▶ HDFS   ├──▶ Merge ──▶ Dashboard
  └──────────┘    └──────────────────────────┘            │
                  2 codebases, 2 processing paths ────────┘

KAPPA Architecture:
  ┌──────────┐    ┌──────────────────────────┐
  │  Kafka   │───▶│ Stream Layer (Flink)     │──▶ Store ──▶ Dashboard
  │ (replay) │    └──────────────────────────┘
  └──────────┘    1 codebase, replay khi cần reprocess
```

| Tiêu chí                  | Lambda                         | Kappa                            |
|---------------------------|--------------------------------|----------------------------------|
| **Code duplication**      | ❌ 2 codebases (batch + stream)| ✅ 1 codebase                    |
| **Complexity**            | ❌ Cao (merge logic)           | ✅ Thấp hơn                     |
| **Correctness**           | ✅ Batch sửa lỗi stream       | ⚠️ Phụ thuộc replay quality     |
| **Late data handling**    | ✅ Batch recompute dễ          | ⚠️ Cần watermark phức tạp       |
| **Historical reprocess**  | ✅ Batch layer sẵn sàng        | ⚠️ Replay toàn bộ Kafka (chậm)  |
| **Operational cost**      | ❌ 2 systems = 2× ops          | ✅ 1 system                      |
| **Phù hợp khi**          | Cần correctness guarantee      | Team nhỏ, ưu tiên simplicity   |

> **Quyết định cho bài này**: Chọn **Lambda** vì:
> 1. Clickstream cần historical accuracy (funnel analysis 90 ngày)
> 2. Batch layer dùng exact count, speed layer dùng approximate (HyperLogLog)
> 3. Team đã có Spark batch expertise

#### Deep-dive: HyperLogLog cho Unique Visitors

```
Vấn đề: Đếm UV real-time cho 5M users
  - Exact: HashSet → 5M × 8 bytes = 40 MB per window → tốn RAM
  - HyperLogLog: 12 KB fixed memory, sai số ±0.81%

Flink pseudo-code:
  stream
    .keyBy(event -> event.page)
    .window(TumblingEventTimeWindows.of(Time.minutes(1)))
    .aggregate(new HyperLogLogAggregator())
    .addSink(new RedisSink("realtime:uv:{page}:{window}"))
```

### ⚖️ Bước 5 — Trade-offs & Failure Modes

| Trade-off                       | Chọn                    | Lý do                                         |
|---------------------------------|-------------------------|------------------------------------------------|
| Exact vs Approximate UV         | Approximate (HLL)       | Real-time ±1% OK, batch sửa exact sau         |
| user_id vs session_id partition | user_id                 | Cùng user vào cùng partition → session ordering|
| JSON vs Avro event format       | Avro                    | Compact hơn 3-4×, schema validation           |
| Flink vs Spark Streaming        | Flink (speed layer)     | True streaming, lower latency                  |

**Failure Scenarios**:

| Failure                  | Impact                          | Mitigation                              |
|--------------------------|---------------------------------|-----------------------------------------|
| SDK offline/timeout      | Mất events                      | Local buffer + retry + batch upload     |
| Kafka broker down        | Ingestion pause                 | RF=3, min.insync.replicas=2             |
| Flink checkpoint fail    | Duplicate counts                | At-least-once OK cho clicks             |
| Redis OOM                | Dashboard stale                 | TTL 24h + eviction policy allkeys-lru   |

---

## Đề 3: Redesign OCR Invoice Pipeline — 40K → 4M invoices/ngày

### 📋 Bước 1 — Clarify Requirements

**Hiện tại**:
- 40K invoices/ngày, single OCR worker, sequential processing
- Processing time: ~2 giây/invoice
- Error rate: ~5% cần manual review

**Yêu cầu mới (scale ×100)**:
- **4M invoices/ngày**
- Latency: invoice → structured data ≤ 5 phút
- Accuracy: ≥ 95% (giảm manual review xuống ≤ 5%)
- Support nhiều format: PDF, image (JPEG/PNG), scan quality thấp

**Câu hỏi cần hỏi interviewer**:

| # | Câu hỏi                                         | Giả định nếu không hỏi     |
|---|--------------------------------------------------|-----------------------------|
| 1 | Peak invoice time? (cuối tháng?)                 | Peak = 3× average           |
| 2 | GPU available hay chỉ CPU?                       | GPU available (cloud)        |
| 3 | Invoice template đa dạng hay chuẩn?              | ~50 templates chính          |
| 4 | Output format?                                    | JSON structured data         |

### 📐 Bước 2 — Estimation

```
Throughput:
  Average: 4,000,000 / 86,400 ≈ 46 invoices/s
  Peak (3×): ≈ 140 invoices/s

Processing capacity per worker:
  GPU worker: ~0.5s/invoice → 2 invoices/s/worker
  CPU worker: ~2s/invoice  → 0.5 invoices/s/worker

Workers needed (peak):
  GPU: 140 / 2 = 70 workers
  CPU: 140 / 0.5 = 280 workers
  → Chọn GPU: 70 workers + 20% headroom = 84 workers
  → Hoặc mix: 50 GPU + 40 CPU (cost optimize)

Storage:
  Average invoice size: 500 KB (PDF/image)
  Raw input/ngày: 4M × 500 KB = 2 TB/ngày
  Output JSON/ngày: 4M × 2 KB = 8 GB/ngày
  30 ngày retention: 60 TB input + 240 GB output

Queue sizing (Kafka):
  140 msg/s peak × 500 KB = 70 MB/s
  → 3-5 Kafka brokers (hoặc SQS nếu cloud)

  ┌──────────────────────────────────────────┐
  │ Peak throughput:  140 invoices/s         │
  │ GPU workers:      ~84 (with headroom)    │
  │ Input storage:    ~2 TB/ngày             │
  │ Queue throughput: ~70 MB/s               │
  └──────────────────────────────────────────┘
```

### 🏗️ Bước 3 — High-Level Architecture

```
  ┌─────────────────────────────────────────────────────────────────┐
  │                   OCR INVOICE PIPELINE (4M/ngày)                │
  └─────────────────────────────────────────────────────────────────┘

  ┌──────────────┐     ┌──────────────┐     ┌──────────────────────┐
  │  Upload API  │     │  S3/MinIO    │     │  Kafka / SQS         │
  │  (REST)      │────▶│  (raw store) │────▶│  (job queue)         │
  │              │     │              │     │                      │
  │ • Auth       │     │ • 2 TB/day   │     │ • topic: ocr-jobs    │
  │ • Validate   │     │ • Lifecycle  │     │ • topic: ocr-results │
  │ • Rate limit │     │   policy     │     │ • topic: ocr-dlq     │
  └──────────────┘     └──────────────┘     └───┬──────────────────┘
                                                │
                          ┌─────────────────────┘
                          │
                          ▼
  ┌───────────────────────────────────────────────────────────┐
  │                    WORKER POOL                             │
  │                                                           │
  │  ┌─────────┐ ┌─────────┐ ┌─────────┐      ┌─────────┐   │
  │  │ GPU     │ │ GPU     │ │ GPU     │ ...  │ GPU     │   │
  │  │ Worker 1│ │ Worker 2│ │ Worker 3│      │ Worker N│   │
  │  └────┬────┘ └────┬────┘ └────┬────┘      └────┬────┘   │
  │       │           │           │                 │        │
  │       └─────────┬─┴───────────┴─────────────────┘        │
  │                 │                                        │
  │  Pipeline per invoice:                                   │
  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌─────────┐  │
  │  │ Download │─▶│ Preproc  │─▶│   OCR    │─▶│  Post   │  │
  │  │ from S3  │  │ (deskew, │  │ (Tesser- │  │ process │  │
  │  │          │  │  denoise,│  │  act/    │  │ (JSON   │  │
  │  │          │  │  resize) │  │  PaddleOCR)│ │  struct)│  │
  │  └──────────┘  └──────────┘  └──────────┘  └────┬────┘  │
  │                                                  │       │
  └──────────────────────────────────────────────────┼───────┘
                                                     │
                              ┌───────────────────────┤
                              │                       │
                    ┌─────────▼────────┐    ┌────────▼─────────┐
                    │  Confidence ≥ 80%│    │ Confidence < 80% │
                    │                  │    │                   │
                    │  → ocr-results   │    │  → ocr-dlq       │
                    │    topic         │    │    (Dead Letter)  │
                    └─────────┬────────┘    └────────┬─────────┘
                              │                      │
                              ▼                      ▼
                    ┌──────────────────┐    ┌──────────────────┐
                    │  Result Store    │    │  Manual Review   │
                    │  (PostgreSQL)    │    │  Queue           │
                    │                  │    │  (UI for humans) │
                    │  • invoice_id    │    │                  │
                    │  • json_data     │    │  • Re-OCR        │
                    │  • confidence    │    │  • Manual correct│
                    │  • processed_at  │    │  • Train feedback│
                    └──────────────────┘    └──────────────────┘
```

### 🔍 Bước 4 — Deep-dive: Queue Design + Retry Strategy

#### Retry Strategy với Exponential Backoff

```
                    ┌────────────┐
                    │  ocr-jobs  │
                    │   (main)   │
                    └─────┬──────┘
                          │
                   ┌──────▼──────┐
                   │   Worker    │
                   │  processes  │
                   └──────┬──────┘
                          │
                ┌─────────┴─────────┐
                │                   │
          ┌─────▼─────┐      ┌─────▼─────┐
          │  Success  │      │   Fail    │
          │           │      │           │
          │ → results │      │ retry < 3?│
          └───────────┘      └─────┬─────┘
                                   │
                          ┌────────┴────────┐
                          │                 │
                    ┌─────▼─────┐     ┌─────▼─────┐
                    │  Yes:     │     │  No:      │
                    │  Re-queue │     │  → DLQ    │
                    │  with     │     │           │
                    │  backoff  │     │  Manual   │
                    │           │     │  review   │
                    │  Delay:   │     └───────────┘
                    │  1s, 4s,  │
                    │  16s      │
                    └───────────┘

  Retry delays: 2^(attempt×2) seconds
    Attempt 1: 1s    (transient error, retry nhanh)
    Attempt 2: 4s    (có thể service dependency down)
    Attempt 3: 16s   (cho thời gian recovery)
    After 3:   → DLQ (cần human intervention)
```

#### Batch Size Optimization

```
Throughput vs Latency trade-off:

  Batch Size │  Throughput    │  Latency/invoice  │  GPU Utilization
  ───────────┼────────────────┼───────────────────┼─────────────────
      1      │  2 inv/s       │  0.5s             │  30%
      4      │  6 inv/s       │  0.7s             │  65%
      8      │  10 inv/s      │  0.8s             │  85%    ← sweet spot
      16     │  12 inv/s      │  1.3s             │  90%
      32     │  13 inv/s      │  2.5s             │  92%

  → Chọn batch_size = 8:
    • GPU utilization 85% (cost-effective)
    • Latency 0.8s/invoice (đạt SLA 5 phút)
    • Workers needed: 140/10 = 14 GPU workers (thay vì 70!)

  ┌──────────────────────────────────────────────┐
  │ Với batch_size=8, chỉ cần 14 GPU workers    │
  │ thay vì 84 workers khi batch_size=1          │
  │ → Tiết kiệm ~83% cost GPU                   │
  └──────────────────────────────────────────────┘
```

#### GPU vs CPU Trade-off

| Tiêu chí          | GPU Workers                | CPU Workers                   |
|--------------------|---------------------------|-------------------------------|
| **Speed**          | 0.5s/invoice              | 2s/invoice                    |
| **Cost/worker**    | $2.5/h (p3.xlarge)       | $0.5/h (c5.2xlarge)          |
| **Batch benefit**  | ✅ 5× với batching        | ⚠️ 1.5× với batching         |
| **Scale down**     | ⚠️ GPU instances ít sẵn   | ✅ Dễ scale                   |
| **Best for**       | High-volume sustained     | Burst / off-peak              |

> **Quyết định**: Mixed fleet — **GPU cho peak hours**, **CPU cho off-peak + burst**.
> Autoscaling dựa trên queue depth.

### ⚖️ Bước 5 — Trade-offs & Failure Modes

```
Autoscaling Logic:

  Queue Depth    │  GPU Workers  │  CPU Workers  │  Total Capacity
  ───────────────┼───────────────┼───────────────┼─────────────────
  < 1,000        │  5            │  0            │  50 inv/s
  1,000-10,000   │  10           │  10           │  105 inv/s
  10,000-50,000  │  14           │  20           │  150 inv/s
  > 50,000       │  14           │  40           │  160 inv/s
                 │               │               │  + alert on-call
```

**Failure Scenarios**:

| # | Failure                        | Impact                       | Mitigation                                |
|---|--------------------------------|------------------------------|-------------------------------------------|
| 1 | GPU worker OOM                 | Invoice stuck                | Per-invoice memory limit + kill + requeue |
| 2 | S3 throttling                  | Download bottleneck          | Exponential backoff + multi-part download |
| 3 | Corrupt PDF/image              | OCR crash                    | Prevalidation + timeout 30s + → DLQ      |
| 4 | Queue backlog > 100K           | SLA breach                   | Alert + auto-scale + priority queue       |
| 5 | Model accuracy degradation     | Tăng DLQ rate                | Monitor confidence distribution + retrain |

---

## 📝 System Design Checklist

Dùng checklist này trước khi kết thúc mỗi bài system design:

### Trước khi trả lời

- [ ] Đã hỏi ít nhất 3 clarifying questions
- [ ] Đã xác định functional vs non-functional requirements
- [ ] Đã set scope rõ ràng (build gì, KHÔNG build gì)

### Estimation

- [ ] Đã tính QPS / throughput (average + peak)
- [ ] Đã tính storage (ngày / tháng / năm)
- [ ] Đã xác định bottleneck resource (CPU / memory / disk / network)
- [ ] Số liệu hợp lý (không bịa số, dùng cheat sheet)

### Architecture

- [ ] Diagram có đủ 5 layers: Source → Ingest → Process → Store → Serve
- [ ] Mỗi component có technology cụ thể
- [ ] Data flow rõ ràng (mũi tên 1 chiều, không vòng lặp ẩn)
- [ ] Có xử lý failure path (DLQ, retry, fallback)

### Deep-dive

- [ ] Đã deep-dive ít nhất 1 component phức tạp nhất
- [ ] Đã giải thích WHY chọn technology, không chỉ WHAT
- [ ] Đã đề cập schema / data model
- [ ] Đã đề cập partitioning / sharding strategy

### Trade-offs

- [ ] Đã nêu ít nhất 3 trade-offs với lý do chọn
- [ ] Đã nêu ít nhất 3 failure scenarios + mitigation
- [ ] Đã đề cập monitoring / alerting
- [ ] Đã thể hiện awareness về cost vs performance

### Kỹ năng mềm

- [ ] Cấu trúc trả lời rõ ràng, đi theo framework
- [ ] Chủ động hỏi interviewer, không monologue
- [ ] Dùng whiteboard/diagram hiệu quả
- [ ] Tự tin nêu trade-off, không cố tìm "đáp án đúng duy nhất"

---

> [!NOTE]
> **Lịch ôn tập gợi ý**:
> - **Ngày 8 sáng**: Thuộc Cheat Sheet + Framework 5 bước. Làm Đề 1 (Fleet CDC ×10).
> - **Ngày 8 chiều**: Làm Đề 2 (Clickstream). Focus vào Lambda vs Kappa.
> - **Ngày 9 sáng**: Làm Đề 3 (OCR Pipeline). Focus vào queue + scaling.
> - **Ngày 9 chiều**: Mock interview 45 phút với bạn/ChatGPT. Dùng checklist tự đánh giá.
