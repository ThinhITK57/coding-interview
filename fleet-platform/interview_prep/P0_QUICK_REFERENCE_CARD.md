# P0 — QUICK REFERENCE CARD (IN RA ÔN TRƯỚC PHỎNG VẤN)
*Tóm tắt mọi con số, công thức, pattern quan trọng nhất trên 1 tài liệu duy nhất.*

---

## 1. LATENCY NUMBERS EVERY DE SHOULD KNOW

| Thao tác | Thời gian | Ghi nhớ |
|---|---|---|
| L1 Cache reference | 1 ns | |
| L2 Cache reference | 4 ns | |
| RAM reference | 100 ns | 0.1 μs |
| SSD random read | 150 μs | 150,000 ns |
| HDD seek | 10 ms | 10,000,000 ns — **chậm hơn SSD 67x** |
| Network round-trip (same DC) | 0.5 ms | |
| Network round-trip (cross-region) | 100 ms | |
| Kafka publish 1 message | 2–5 ms | |
| Redis GET | 0.1 ms | **< 1ms — lý do dùng Redis serving** |
| PostgreSQL simple query | 1–5 ms | |
| PostgreSQL complex JOIN | 20–100 ms | **Lý do cần Materialized View** |
| Spark micro-batch trigger | 100 ms – 30s | Cấu hình `trigger(processingTime)` |

---

## 2. THROUGHPUT BENCHMARKS

| Hệ thống | Throughput | Điều kiện |
|---|---|---|
| Kafka | **1M messages/s/broker** | Message size < 1KB, 3 brokers, RF=3 |
| Spark read Parquet | **100–500 MB/s/core** | Columnar + partition pruning |
| HDFS sequential write | **100 MB/s** | 128MB block, RF=3 |
| Redis operations | **100K ops/s** | Single thread, pipeline mode |
| PostgreSQL TPS | **5K–15K TPS** | Simple INSERT/UPDATE, SSD |

---

## 3. STORAGE QUICK MATH

```
1 triệu rows × 100 Bytes/row  = 100 MB
10 triệu rows × 100 Bytes/row = 1 GB
1 tỷ rows × 100 Bytes/row     = 100 GB
10 tỷ rows × 100 Bytes/row    = 1 TB
```

**Parquet 128MB chứa được:**
- Bảng hẹp (Telemetry, 10 cột số): **~10 triệu dòng**
- Bảng trung bình (Fact DWH, 25 cột): **~1.5 triệu dòng**
- Bảng rộng (Customer + text, 50 cột): **~300,000 dòng**

---

## 4. FLEET ARCHITECTURE DIAGRAM

```
┌──────────────────┐     ┌──────────────────┐     ┌──────────────────┐
│ PostgreSQL Odoo  │────▶│ Debezium CDC     │────▶│ Kafka (3 brokers)│
│ (OLTP, 3NF)     │ WAL │ (Log-based)      │     │ RF=3, ISR=2      │
│ REPLICA IDENTITY │     │ confirmed_flush  │     │                  │
│ FULL             │     │ _lsn             │     │                  │
└──────────────────┘     └──────────────────┘     └────────┬─────────┘
                                                           │
                              ┌─────────────────────────────┤
                              │                             │
                              ▼                             ▼
                   ┌──────────────────┐          ┌──────────────────┐
                   │ Spark Structured │          │ Spark Batch      │
                   │ Streaming        │          │ (Airflow DAGs)   │
                   │ Trigger: 30s     │          │ Weekly/Monthly   │
                   └────────┬─────────┘          └────────┬─────────┘
                            │                             │
                            ▼                             ▼
                   ┌──────────────────┐          ┌──────────────────┐
                   │ HDFS Data Lake   │          │ HDFS DWH         │
                   │ (Bronze/Raw)     │          │ (Gold/Star Schema│
                   │ Parquet, part by │          │ 2 Fact + 4 Dim)  │
                   │ year/month/day   │          │                  │
                   └──────────────────┘          └────────┬─────────┘
                                                          │
                                                          ▼
                                                ┌──────────────────┐
                                                │ Redis 7+         │
                                                │ • GEOSEARCH <1ms │
                                                │ • Lua atomic slot│
                                                │ • Pub/Sub notify │
                                                └────────┬─────────┘
                                                         │
                                                         ▼
                                                ┌──────────────────┐
                                                │ WebSocket Server │
                                                │ Push < 15ms      │
                                                │ Executive Dashbd │
                                                └──────────────────┘
```

---

## 5. SCD TYPE 2 MERGE — 5 BƯỚC (THUỘC LÒNG)

```
Bước 1: Giữ nguyên 100% bản ghi lịch sử đã đóng (is_current = FALSE)
Bước 2: LEFT JOIN active records với CDC batch mới
Bước 3: ĐÓNG records active bị đổi (SET is_current=FALSE, expiration_date=today)
Bước 4: Giữ nguyên records active KHÔNG đổi (skip)
Bước 5: INSERT records mới (version mới + khách mới) với surrogate key MD5
```

**Surrogate Key**: `MD5(customer_id || '_' || effective_date || '_' || status)`
**Điều kiện bắt buộc**: `ALTER TABLE customers REPLICA IDENTITY FULL;`

---

## 6. DATA QUALITY GATE — 5 TẦNG

```
Tầng 1: pg_replication_slots → Debezium active? WAL lag < 5GB?
Tầng 2: Kafka consumer lag → LAG < 100K messages?
Tầng 3: Freshness → MAX(loaded_at) < 4 hours ago?
Tầng 4: Volume → today_count > avg_7d - 2σ?
Tầng 5: Reconciliation → source_rows == target_rows?

Gate: Airflow ShortCircuitOperator → Nếu ANY fail → CHẶN publish xuống Redis
```

---

## 7. BIG-O CHEAT SHEET

| Cấu trúc DL | Access | Search | Insert | Delete | Ghi nhớ |
|---|---|---|---|---|---|
| Array | O(1) | O(n) | O(n) | O(n) | Index trực tiếp |
| HashMap | — | O(1)* | O(1)* | O(1)* | *Amortized, worst O(n) |
| Set | — | O(1)* | O(1)* | O(1)* | Dedupe tool |
| Linked List | O(n) | O(n) | O(1) | O(1) | Insert/Delete nhanh |
| BST (balanced) | — | O(log n) | O(log n) | O(log n) | Sorted data |
| Min/Max Heap | O(1) top | O(n) | O(log n) | O(log n) | Top-K problems |
| Stack/Queue | O(1) top | O(n) | O(1) | O(1) | LIFO/FIFO |

| Thuật toán Sort | Best | Average | Worst | Space | Stable? |
|---|---|---|---|---|---|
| QuickSort | O(n log n) | O(n log n) | **O(n²)** | O(log n) | ❌ |
| MergeSort | O(n log n) | O(n log n) | O(n log n) | **O(n)** | ✅ |
| HeapSort | O(n log n) | O(n log n) | O(n log n) | O(1) | ❌ |
| TimSort (Python) | O(n) | O(n log n) | O(n log n) | O(n) | ✅ |

---

## 8. STAR SCHEMA FLEET DWH

```
Fact Tables:
  fact_repair_service_revenue (date_key, head_key, customer_key, service_amount, parts_amount, total_amount)
  fact_parts_sales (date_key, head_key, component_key, quantity, total_parts_amount)

Dimension Tables:
  dim_date       (date_key INT YYYYMMDD, calendar_year, fiscal_year, fiscal_quarter)
  dim_customer   (customer_key MD5, customer_id, status, effective_date, expiration_date, is_current)  ← SCD2
  dim_head       (head_key, head_name, city, lat, lng, capacity)
  dim_component  (component_key, code, category, unit_cost)

Fiscal Year: CASE WHEN month >= 10 THEN year + 1 ELSE year END
```

---

## 9. EXACTLY-ONCE SEMANTICS — 4 TẦNG

| Tầng | Cơ chế |
|---|---|
| Kafka → Spark | Checkpoint lưu offset (`checkpointLocation`) |
| Spark → HDFS | FileStreamSinkLog (`_spark_metadata/batchId`) |
| Spark → Redis | `foreachBatch()` + idempotent `HSET` overwrite |
| HDFS Batch Overwrite | `partitionOverwriteMode=dynamic` — ghi đè partition nguyên tử |

---

## 10. TOOL JUSTIFICATION — 1 CÂU MỖI TOOL

| Tool | 1 câu trả lời "Tại sao?" |
|---|---|
| **Debezium CDC** | 0% query load lên OLTP, bắt DELETE + trạng thái trung gian, latency < 1s |
| **Kafka** | Decouple producer/consumer, replay được (replayable source), throughput 1M msg/s |
| **Spark** | Unified batch + streaming, in-memory DAG nhanh hơn MapReduce 10-100x |
| **HDFS** | Data locality cho Spark batch, 3x replication, hệ sinh thái Hadoop mature |
| **Parquet** | Columnar → column pruning + predicate pushdown, nén 5-10x so với CSV |
| **Star Schema** | Tối thiểu JOIN (1 hop Fact→Dim), tối ưu cho BI query trên Spark SQL |
| **Redis** | In-memory < 1ms, GEOSEARCH, Lua Script atomic, Pub/Sub push notification |
| **Airflow** | DAG-based orchestration, sensor dependencies, backfill built-in |
| **WebSocket** | Push-based (0 polling requests) vs REST polling (360 req/min cho 30 users) |

---

## 11. PHÂN BIỆT NHANH

| Khái niệm A | vs | Khái niệm B | Khác biệt cốt lõi |
|---|---|---|---|
| OLTP | vs | OLAP | Write-optimized (3NF) vs Read-optimized (Star Schema) |
| ETL | vs | ELT | Transform trước/sau Load — ELT phổ biến trên Cloud/Lakehouse |
| Batch | vs | Streaming | Latency > 1h vs < 1 min |
| Star Schema | vs | Snowflake | Denorm Dim (ít JOIN) vs Norm Dim (nhiều JOIN) |
| CDC Log-based | vs | Query Polling | Đọc WAL (0% load DB) vs SELECT (tải DB, mất DELETE) |
| Kafka | vs | RabbitMQ | Log-based replay vs Traditional message queue |
| coalesce(n) | vs | repartition(n) | Gộp KHÔNG shuffle vs Chia đều CÓ shuffle |
| Additive | vs | Semi-Additive | SUM qua mọi dim vs KHÔNG SUM qua dim Time |
