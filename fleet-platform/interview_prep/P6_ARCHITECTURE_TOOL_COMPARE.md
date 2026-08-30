# P6 — ARCHITECTURE DESIGN + TOOL JUSTIFICATION (Trụ cột 6 — Ngày 10-11)
*Trả lời "tại sao X không dùng Y" trong 30 giây cho mọi tool trong CV*

---

## PHẦN A: 7 CẶP CÔNG CỤ SO SÁNH

### 1. HDFS vs S3/MinIO

| Tiêu chí | HDFS | S3/MinIO |
|---|---|---|
| **Kiến trúc** | Storage + Compute **cùng node** (data locality) | Storage tách biệt Compute |
| **Latency đọc** | Rất thấp (đọc local disk) | Cao hơn (network hop) |
| **Scalability** | Scale cả storage + compute cùng lúc (tốn kém) | Scale **độc lập** (linh hoạt, tiết kiệm) |
| **Chi phí vận hành** | Cao (quản lý NameNode, DataNode, JournalNode) | Thấp hơn (managed service hoặc MinIO đơn giản) |
| **Fleet dùng gì** | **HDFS** — bare-metal cluster sẵn có, Spark batch chạy lại lịch sử cần data locality |

**🎤 30 giây**: *"Em chọn HDFS vì team vận hành bare-metal cluster, và Spark batch jobs cần data locality — đọc dữ liệu ngay trên node chứa data, tránh truyền qua mạng. Nếu chuyển cloud, em sẽ dùng S3 + Spark on Kubernetes để tách storage/compute, scale độc lập."*

---

### 2. Kafka vs RabbitMQ

| Tiêu chí | Kafka | RabbitMQ |
|---|---|---|
| **Mô hình** | **Log-based** — message được lưu lại, consumer đọc theo offset | **Queue-based** — message bị xóa sau khi consumer ACK |
| **Replay** | ✅ Replay bất kỳ lúc nào (reset offset) | ❌ Không replay được (đã xóa) |
| **Throughput** | **1M msg/s/broker** | ~50K msg/s |
| **Use case** | Event streaming, CDC, log aggregation | Task queue, RPC, request-reply |
| **Fleet dùng gì** | **Kafka** — CDC events cần replay khi backfill, throughput cao |

**🎤 30 giây**: *"Em chọn Kafka vì CDC pipeline cần 2 tính năng: (1) Replay — khi cần backfill, consumer reset offset về thời điểm bất kỳ để xử lý lại. (2) Multi-consumer — cùng 1 CDC topic được consume bởi cả Spark Streaming (near-real-time) và Spark Batch (nightly) mà không ảnh hưởng lẫn nhau."*

---

### 3. Spark Structured Streaming vs Apache Flink

| Tiêu chí | Spark Structured Streaming | Apache Flink |
|---|---|---|
| **Processing model** | **Micro-batch** (trigger interval: 100ms - 30s) | **True streaming** (event-by-event, latency ms) |
| **Latency** | 100ms - vài giây | **Milliseconds** |
| **Unified API** | ✅ Cùng DataFrame API cho batch + streaming | ✅ DataStream + Table API |
| **Ecosystem** | Rộng hơn (MLlib, GraphX, SparkSQL) | Focused hơn vào streaming |
| **Fleet dùng gì** | **Spark** — unified batch+streaming, team quen, latency vài giây đủ cho use case |

**🎤 30 giây**: *"Fleet không cần latency mili-giây — Dashboard refresh chu kỳ 30 giây là đủ. Spark cho phép dùng cùng DataFrame API cho cả streaming ingestion và batch DWH aggregation, giảm learning curve và maintenance cost. Nếu use case cần sub-millisecond (trading, fraud detection real-time), em sẽ chọn Flink."*

---

### 4. Neo4j vs PostgreSQL cho Graph Queries

| Tiêu chí | Neo4j | PostgreSQL |
|---|---|---|
| **Mô hình lưu trữ** | **Native graph** — pointer trực tiếp giữa nodes | Relational — JOIN qua index |
| **Multi-hop traversal** | **O(1) per hop** (follow pointer) | **O(n log n) per JOIN** (index lookup) |
| **5-hop query** | 5 × O(1) = O(5) ≈ **< 10ms** | 5 JOINs = **cost tăng theo hàm mũ** |
| **Use case** | Social network, recommendation, knowledge graph | OLTP, reporting, ACID transactions |

**🎤 30 giây**: *"Bài toán Student Behavioral cần truy vấn 4-5 bước nhảy quan hệ: Student → Enrollment → Course → Module → Quiz. PostgreSQL cần 4 JOINs lồng nhau, cost tăng theo hàm mũ. Neo4j lưu pointer trực tiếp → mỗi hop O(1), performance không phụ thuộc vào tổng số nodes."*

---

### 5. LanceDB (Dense) vs BM25s (Sparse) trong RAG

| Tiêu chí | LanceDB (Dense Embedding) | BM25s (Sparse / Keyword) |
|---|---|---|
| **Cách tìm kiếm** | Vector similarity (cosine/L2) | Term frequency (TF-IDF) |
| **Thế mạnh** | Hiểu **ngữ nghĩa** ("bộ phanh" ≈ "hệ thống phanh") | Khớp **từ khóa chính xác** (mã số "ABC-12345") |
| **Điểm yếu** | Bỏ sót từ khóa chính xác (tên riêng, mã số) | Không hiểu ngữ nghĩa |
| **Fleet dùng gì** | **Hybrid (cả hai) + RRF fusion** — kết hợp ưu điểm |

**🎤 30 giây**: *"Em dùng Hybrid Retrieval: Dense bắt ngữ nghĩa, Sparse bắt keyword chính xác. Reciprocal Rank Fusion (RRF) kết hợp ranking từ cả hai — document xuất hiện top ở cả 2 hệ thống được ưu tiên cao nhất. Thực nghiệm cho thấy Hybrid tăng +15-20% recall so với Dense-only trên queries chứa mã số kỹ thuật."*

---

### 6. Airflow vs Dagster/Prefect

| Tiêu chí | Airflow | Dagster/Prefect |
|---|---|---|
| **Maturity** | **10+ năm**, cộng đồng lớn nhất | 3-5 năm, đang phát triển |
| **Ecosystem** | 1000+ operators/hooks built-in | Nhỏ hơn nhưng đang mở rộng |
| **Programming model** | Imperative (define tasks + dependencies) | **Declarative** (software-defined assets) |
| **Scaling** | CeleryExecutor, KubernetesExecutor | Tương tự nhưng nhẹ hơn |
| **Fleet dùng gì** | **Airflow** — mature, đầy đủ sensors, backfill built-in |

---

### 7. Redis vs Memcached

| Tiêu chí | Redis | Memcached |
|---|---|---|
| **Data structures** | ✅ String, Hash, List, Set, Sorted Set, **Geo**, Stream, **Lua Script** | ❌ Chỉ key-value string |
| **Persistence** | ✅ RDB snapshot + AOF (durable) | ❌ Volatile only (RAM) |
| **Pub/Sub** | ✅ Built-in | ❌ Không có |
| **Fleet dùng gì** | **Redis** — cần GEOSEARCH, Lua atomic, Pub/Sub cho WebSocket push |

---

## PHẦN B: DECISION FRAMEWORKS

### Batch vs Streaming Decision Tree

```
                    Latency Requirement?
                          │
          ┌───────────────┼───────────────┐
          ▼               ▼               ▼
     > 1 hour       1 min – 1 hour     < 1 minute
          │               │               │
          ▼               ▼               ▼
      ✅ BATCH      ✅ MICRO-BATCH    ✅ TRUE STREAMING
    (Spark Batch     (Spark Structured   (Flink, Kafka
     + Airflow)       Streaming)          Streams)
```

**Hệ thống Fleet dùng cả hai:**
- **Streaming** (micro-batch 30s): Ingest CDC events → HDFS Data Lake
- **Batch** (Airflow schedule): HDFS → Star Schema DWH → Redis → Dashboard

---

### CAP Theorem — Áp dụng thực tế

| Hệ thống | C (Consistency) | A (Availability) | P (Partition Tolerance) | Giải thích |
|---|---|---|---|---|
| PostgreSQL | ✅ | ⚠️ | ✅ | **CP** — ACID đảm bảo consistency, sacrifce availability khi network split |
| Kafka | ⚠️ | ✅ | ✅ | **AP** — `acks=all` + `min.insync.replicas=2` tăng C nhưng vẫn AP cốt lõi |
| Redis | ⚠️ | ✅ | ✅ | **AP** by default — async replication, có thể mất data khi failover |
| HDFS | ✅ | ⚠️ | ✅ | **CP** — NameNode single point of failure (cần HA config) |

---

### Storage Format Decision

| Format | Kiểu | Nén | Schema | Use case |
|---|---|---|---|---|
| **CSV** | Row | ❌ Kém | ❌ Không | Exchange, import/export, small data |
| **JSON** | Row | ❌ Kém | ✅ Flexible | API responses, logs, semi-structured |
| **Avro** | Row | ✅ Tốt | ✅ Embedded | **Kafka messages** (schema evolution, compact) |
| **Parquet** | **Column** | ✅ Rất tốt | ✅ Embedded | **Analytics / DWH** (column pruning, predicate pushdown) |
| **ORC** | **Column** | ✅ Rất tốt | ✅ Embedded | Hive ecosystem (tương đương Parquet) |

**Fleet dùng**: Kafka CDC → **Avro** (nếu có Schema Registry) hoặc JSON, HDFS → **Parquet** (analytics)

---

## PHẦN C: SCRIPT E2E ARCHITECTURE FLEET (3-4 phút)

> *"Pipeline bắt đầu từ **PostgreSQL** — em chọn PostgreSQL vì Odoo ERP chạy trên nó, và PostgreSQL hỗ trợ logical replication (WAL) cần thiết cho CDC.*
>
> *Debezium CDC đọc WAL log — em chọn **Debezium** thay vì query polling vì 0% query load lên OLTP DB, bắt được cả DELETE events và trạng thái trung gian.*
>
> ***Kafka** làm buffer trung gian — em chọn Kafka thay vì RabbitMQ vì cần replay capability khi backfill, và 1 CDC topic được consume bởi cả streaming và batch mà không ảnh hưởng lẫn nhau.*
>
> ***Spark** xử lý cả streaming (micro-batch 30s ingest vào Data Lake) và batch (Airflow schedule tạo DWH). Em chọn Spark thay vì Flink vì unified API cho cả 2 workloads, team đã quen, và latency vài giây đủ cho use case.*
>
> *Data Lake trên **HDFS Parquet** — em chọn HDFS vì bare-metal cluster sẵn có, data locality cho Spark batch. Parquet vì columnar storage cho phép column pruning và predicate pushdown, nén tốt 5-10x so với CSV.*
>
> *DWH thiết kế theo **Star Schema** — em chọn thay vì Snowflake vì trên Spark, chi phí JOIN rất đắt (shuffle), Star Schema giảm xuống tối đa 1 hop JOIN.*
>
> ***Redis 7+** làm serving layer — em chọn thay vì PostgreSQL PostGIS vì GEOSEARCH < 1ms so với 50-100ms, Lua Script đảm bảo atomic slot reservation 100% không overbooking, Pub/Sub kích hoạt WebSocket push.*
>
> *Dashboard dùng **WebSocket** push — em chọn thay vì REST polling vì 30 executives polling = 360 requests/phút tạo tải vô ích. WebSocket: 0 requests, push chỉ khi có data mới, latency < 15ms."*

---

## PHẦN D: 10 CÂU "WHY X NOT Y" PHỔ BIẾN NHẤT

| # | Câu hỏi | Đáp án 30 giây |
|---|---|---|
| 1 | Tại sao CDC mà không Polling? | 0% load DB, bắt DELETE + trạng thái trung gian, latency < 1s |
| 2 | Tại sao Kafka mà không RabbitMQ? | Log-based replay cho backfill, multi-consumer cùng topic, 1M msg/s |
| 3 | Tại sao HDFS mà không S3? | Data locality cho Spark batch, bare-metal cluster sẵn có |
| 4 | Tại sao Star Schema mà không Snowflake? | Ít JOIN nhất trên Spark (shuffle đắt), BI tools tương thích tốt |
| 5 | Tại sao Spark mà không Flink? | Unified batch+streaming API, team quen, latency vài giây đủ |
| 6 | Tại sao Redis mà không PostgreSQL serving? | In-memory < 1ms, GEOSEARCH, Lua atomic, Pub/Sub push |
| 7 | Tại sao WebSocket mà không REST polling? | 0 requests/phút vs 360, push khi có data mới, latency < 15ms |
| 8 | Tại sao Parquet mà không CSV? | Column pruning, predicate pushdown, nén 5-10x, schema embedded |
| 9 | Tại sao SCD2 mà không SCD1? | Giữ lịch sử trạng thái tại thời điểm hóa đơn, SCD1 mất hết |
| 10 | Tại sao REPLICA IDENTITY FULL? | Cần before image đầy đủ để so sánh trạng thái cũ/mới cho SCD2 merge |

---

## 📋 CHECKLIST ÔN TẬP TRỤ CỘT 6

- [ ] Nói được trade-off 7 cặp công cụ trong 30 giây/cặp
- [ ] Vẽ được Batch vs Streaming Decision Tree
- [ ] Biết CAP cho từng tool (Kafka=AP, PostgreSQL=CP, Redis=AP, HDFS=CP)
- [ ] Trình bày E2E Architecture Fleet trong 3-4 phút không nhìn tài liệu
- [ ] Trả lời 10 câu "Why X not Y" phản xạ trong 30 giây
