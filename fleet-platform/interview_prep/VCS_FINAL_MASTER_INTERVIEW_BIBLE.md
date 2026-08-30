# VCS FINAL MASTER INTERVIEW BIBLE
## SIÊU TÀI LIỆU CHIẾN LƯỢC PHỎNG VẤN FINAL ROUND TẠI VIETTEL CYBER SECURITY
### Đóng Gói Toàn Diện: Bảo Vệ Dự Án Flagship, Tranh Biện 360 Độ, Xử Lý Sự Cố Thực Chiến, Cấu Hình 7 Frameworks Bare-Metal (No Docker), Kỷ Luật Kỹ Thuật & Quản Trị Source Code

---

```
┌────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ TÔN CHỈ PHỎNG VẤN LEVEL PRINCIPAL / SENIOR DATA ENGINEER:                                              │
│                                                                                                        │
│  "Một kỹ sư giỏi chỉ nhìn thấy ưu điểm trong giải pháp của mình.                                       │
│   Một KỸ SƯ TRƯỞNG XUẤT SẮC nhìn thấy toàn bộ ĐIỂM NGHẼN, RỦI RO, ĐÁNH ĐỔI (TRADE-OFFS)               │
│   và CHỦ ĐỘNG THIẾT LẬP CHỐT CHẶN AN TOÀN (FAILSAFES) cho toàn bộ hệ sinh thái."                       │
└────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

# MỤC LỤC TỔNG QUAN

```
┌────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ PHẦN 1: HỒ SƠ DỰ ÁN FLAGSHIP & BẢN ĐỒ KIẾN TRÚC END-TO-END                                             │
│   1.1 Bối cảnh kinh doanh & 3 Điểm nghẽn nghiêm trọng trước khi triển khai                             │
│   1.2 Sơ đồ dòng chảy dữ liệu Kiến trúc 5 Tầng (Data Flow Architecture)                                │
│   1.3 Bảng thống kê Outcome định lượng (Before vs After)                                               │
├────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ PHẦN 2: ĐẠI CHIẾN TRANH BIỆN 360 ĐỘ (SENIOR ODOO ARCHITECT VS SENIOR DATA ENGINEER)                    │
│   2.1 Vòng 1: Phe Data Engineer chỉ ra 4 giới hạn vật lý của Odoo / Monolith                          │
│   2.2 Vòng 2: Phe Senior Odoo tấn công trực diện 5 tử huyệt của kiến trúc Data Platform               │
│   2.3 Vòng 3: Phe Data Engineer phản công dứt điểm bằng Nguyên lý Kiến trúc Doanh nghiệp              │
├────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ PHẦN 3: BỐN SỰ CỐ PRODUCTION THỰC CHIẾN & RUNBOOK XỬ LÝ GỐC RỄ (RCA)                                   │
│   3.1 Sự cố Kép: Lệch LSN Debezium (`wal_status='lost'`) + Data Skew Partition trên Spark (Task 42)   │
│   3.2 Sự cố 2: Small File Problem làm kiệt quệ RAM NameNode HDFS (150.000 files)                      │
│   3.3 Sự cố 3: Tranh chấp giữ chỗ (Race Condition Overbooking) giữa hàng ngàn tài xế                  │
│   3.4 Sự cố 4: Cơn ác mộng Schema Drift khi Odoo thêm/sửa/xóa cột trong Sprint                       │
├────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ PHẦN 4: TỔNG HỢP TOÀN BỘ CẤU HÌNH BẮT BUỘC ĐỂ CÀI ĐẶT 8 FRAMEWORKS BARE-METAL (NO DOCKER)               │
│   4.1 Bản đồ phân bổ dịch vụ & IP tĩnh trên cụm 3 máy chủ vật lý                                       │
│   4.2 Framework 1: Hadoop HDFS 3.3.6 (Mọi file XML, thư mục & thông số bắt buộc)                       │
│   4.3 Framework 2: Kafka Multi-Broker Cluster 3.7.1 (Zookeeper + Server.properties)                    │
│   4.4 Framework 3: PostgreSQL 14+ Bare-Metal (postgresql.conf + pg_hba.conf + CDC Role)                │
│   4.5 Framework 4: Debezium Distributed (connect-distributed.properties + JSON Connector)             │
│   4.6 Framework 5: Apache Spark on Bare-Metal (spark-env.sh + spark-defaults.conf + History Server)    │
│   4.7 Framework 6: Redis 7+ Systemd (redis.conf + bảo mật + LRU memory)                                │
│   4.8 Framework 7: Apache Hive 3.1.3 (hive-site.xml + PostgreSQL Metastore + HiveServer2 + Beeline)    │
│   4.9 Framework 8: Apache Airflow LocalExecutor (airflow.cfg + Postgres metadata + Systemd units)      │
│   4.10 Kịch bản Kiểm thử tự động 8/8 thành phần hạ tầng qua Bash Script (verify-all-infra.sh)          │
│   4.11 Bảng tổng hợp các "Bẫy cấu hình chết người" cần ghim chặt trong đầu                             │
├────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ PHẦN 5: KỶ LUẬT KỸ THUẬT, QUẢN TRỊ SOURCE CODE & PIPELINE CI/CD NATIVE                                 │
│   5.1 Quy chuẩn Git Flow & Chiến lược phân nhánh an toàn                                               │
│   5.2 Pre-commit Hooks & Quét lộ lọt bảo mật (Gitleaks, Flake8, Black, Sqlfluff)                       │
│   5.3 Chiến lược Kiểm thử tự động (PySpark Unit Test Mocking + Great Expectations Data Quality)        │
│   5.4 Pipeline CI/CD tự động hóa Deployment (GitLab CI / Shell Scripts / Systemd Service Reload)       │
├────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ PHẦN 6: QUẢN TRỊ CÔNG VIỆC AGILE/SCRUM & NGHỆ THUẬT LÀM VIỆC NHÓM LIÊN PHÒNG BAN                      │
│   6.1 Quy trình vận hành Sprint 2 tuần trên Jira                                                       │
│   6.2 Ma trận phối hợp 4 bên: DBA/ERP, BI/Analysts, Hạ Tầng/Bảo Mật, Product Owner                     │
├────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ PHẦN 7: TƯ DUY AN TOÀN & BẢO MẬT DỮ LIỆU ĐẶC THÙ VIETTEL CYBER SECURITY (VCS MINDSET)                 │
│   7.1 Che mờ dữ liệu định danh cá nhân (PII Data Masking)                                              │
│   7.2 Nguyên tắc đặc quyền tối thiểu (Least Privilege & RBAC)                                          │
│   7.3 Quản trị Bí mật (Secrets Management qua HashiCorp Vault / Linux Permissions)                     │
│   7.4 Audit Logging & Dấu vết nguồn gốc dữ liệu (Data Lineage)                                         │
├────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ PHẦN 8: BỘ CÂU HỎI KỸ THUẬT ĐÀO SÂU POSTGRESQL & BIG DATA (TỔNG HỢP VÒNG 1)                            │
├────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ PHẦN 9: KỊCH BẢN TRÌNH BÀY MẪU 10 PHÚT CHUẨN STAR & 2 CÂU HỎI CHIẾN LƯỢC CUỐI BUỔI                     │
└────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

# PHẦN 1: HỒ SƠ DỰ ÁN FLAGSHIP & BẢN ĐỒ KIẾN TRÚC END-TO-END

### 🎯 Tên dự án:
**Hệ Thống Nền Tảng Dữ Liệu Bảo Dưỡng & Sửa Chữa Xe Tải Tập Trung (Fleet Maintenance & Repair Data Platform)**

### 1.1 Bối cảnh kinh doanh & 3 Điểm nghẽn nghiêm trọng:
* **Quy mô**: Chuỗi 50 trạm dịch vụ (Heads) trên toàn quốc, phục vụ $10.000$ xe tải vận hành liên tục.
* **Hai nguồn doanh thu cốt lõi**:
  1. *Doanh thu Dịch vụ Sửa chữa & Bảo dưỡng sau bán xe*.
  2. *Doanh thu Bán Linh kiện & Phụ tùng thay thế*.
* **3 Điểm nghẽn dữ liệu & vận hành trước khi xây dựng Data Platform**:
  1. *Quá tải Database OLTP*: Hệ thống ERP Odoo (PostgreSQL) thường xuyên bị nghẽn I/O và treo màn hình thu ngân vào cuối tháng do các truy vấn báo cáo tài khóa và tính khoảng cách vị trí.
  2. *Độ trễ báo cáo quá lớn (T+30 ngày)*: Ban Giám đốc phải chờ báo cáo Excel thủ công vào cuối tháng, không phát hiện kịp thời trạm sụt giảm doanh thu hay nguy cơ xe hỏng hóc trên đèo.
  3. *Mất dấu vết lịch sử (Data Overwrite)*: Khi khách hàng đổi trạng thái (*Mới $\to$ Thường niên*), Odoo ghi đè `UPDATE` làm sai lệch $100\%$ báo cáo doanh thu tài khóa cũ.

---

### 1.2 Sơ đồ dòng chảy dữ liệu Kiến trúc 5 Tầng:

```
┌────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ KIẾN TRÚC 5 TẦNG DÒNG CHẢY DỮ LIỆU (END-TO-END DATA PLATFORM ARCHITECTURE)                            │
│                                                                                                        │
│ ┌────────────────────────────────────────────────────────────────────────────────────────────────────┐ │
│ │ TẦNG 1: INGESTION (Thu Thập Dữ Liệu Không Gây Khóa)                                                │ │
│ │  [PostgreSQL OLTP] ──(WAL Log)──► [Debezium CDC] ──────┐                                           │ │
│ │  (REPLICA IDENTITY FULL)          (pgoutput plugin)    │                                           │ │
│ │                                                        ├──► [Kafka Cluster (3 Brokers, RF=3, ISR=2)]│
│ │  [10.000 Xe Tải IoT] ──(MQTT/HTTP)──► [IoT Gateway] ───┘    (Topic: cdc.fleet.*, iot.telemetry)    │
│ └───────────────────────────────────────────────────────────────────────────────┬────────────────────┘ │
│                                                                                 │                      │
│ ┌───────────────────────────────────────────────────────────────────────────────┼────────────────────┐ │
│ │ TẦNG 2 & 3: STREAM & BATCH STORAGE (Hadoop Multi-Node Lakehouse)              │                      │
│ │                                                                               ▼                      │
│ │                                                                   [Spark Structured Streaming]       │
│ │                                                                   (Micro-batch 30s + Watermark 10m)  │
│ │                                                                               │                      │
│ │                                                                               ▼                      │
│ │                                                                   [HDFS Data Lake (Bronze Parquet)]  │
│ │                                                                   (Partition: year/month/day/head_id)│
│ │                                                                               │                      │
│ │                                                                               ▼                      │
│ │                                                                   [Airflow Batch Processing (Gold)]  │
│ │                                                                   (Nightly Compaction + SCD2 DWH)    │
│ └───────────────────────────────────────────────────────────────────────────────┬────────────────────┘ │
│                                                                                 │                      │
│ ┌───────────────────────────────────────────────────────────────────────────────┼────────────────────┐ │
│ │ TẦNG 4 & 5: SERVING & GOVERNANCE LAYER (Độ Trễ Thấp & Quản Trị)               ▼                      │
│ │  [Redis 7+ Serving] ◄───────────────────────────────────────────── [Star Schema DWH (Gold)]          │
│ │  (GEOSEARCH <1ms, Lua Script Atomic Booking)                                  │                      │
│ │          │                                                                    ▼                      │
│ │          ▼                                                             [Executive BI / PowerBI]      │
│ │  [FastAPI Backend Push] ──(WebSocket <15ms)──────────────────────────► (Latency T+15m, Freshness)   │
│ └────────────────────────────────────────────────────────────────────────────────────────────────────┘ │
└────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

### 1.3 Bảng thống kê Outcome định lượng (Before vs After):

```
┌───────────────────────────────┬──────────────────────────────┬─────────────────────────────────────────┐
│ Chỉ Số Đo Lường (Metrics)     │ Trước Khi Có Data Platform   │ Sau Khi Triển Khai Data Platform        │
├───────────────────────────────┼──────────────────────────────┼─────────────────────────────────────────┤
│ Độ trễ Báo cáo Doanh thu      │ T+30 ngày (Excel cuối tháng) │ T+15 phút (Cập nhật tự động liên tục)   │
│ Tải Truy vấn trên PostgreSQL  │ Rất cao (Nghẽn bảng cuối quý)│ 0% tải phân tích (Chỉ đọc WAL Log)      │
│ Độ trễ Tìm trạm gần nhất      │ 85 ms (Truy vấn PostGIS)     │ < 1 ms (Redis GEOSEARCH in-memory)      │
│ Tỉ lệ Đặt trùng lịch (Booking)│ Có xảy ra tranh chấp         │ 0% Overbooking (Redis Lua Atomic)       │
│ Thời gian Phát hiện Sự cố CDC │ Mất 2 ngày mới phát hiện     │ < 5 phút (Data Quality Gate 5 tầng)     │
│ Độ chính xác Lịch sử Khách    │ 0% (Bị ghi đè mất dữ liệu)   │ 100% chính xác lịch sử (SCD Type 2)     │
│ Doanh thu Bán chéo Phụ tùng   │ Cơ bản                       │ +22% nhờ Khai phá luật kết hợp Apriori  │
└───────────────────────────────┴──────────────────────────────┴─────────────────────────────────────────┘
```

---

# PHẦN 2: ĐẠI CHIẾN TRANH BIỆN 360 ĐỘ
### (Senior Odoo Architect vs Senior Data Engineer)

```
┌────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ VÒNG 1: PHE DATA ENGINEER CHỈ RA 4 GIỚI HẠN VẬT LÝ CỦA MONOLITH ODOO                                  │
├────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ 1. EAV Table Explosion: Bảng `mail.tracking.value` phình to 270 triệu dòng sau 5 năm. Phép dựng lại    │
│    trạng thái lịch sử bằng Subquery/Window Function tốn $O(N \times M)$ I/O, bóp chết CPU PostgreSQL. │
│ 2. Row-Store I/O Bottleneck: PostgreSQL quét bảng 50 triệu dòng phải nạp toàn bộ cột vào RAM, trong   │
│    khi Parquet Columnar chỉ đọc đúng 2 cột số liệu, nén Snappy 10x, giảm 90% Disk I/O.                │
│ 3. Connection Exhaustion: 5.000 tài xế ping tìm trạm đồng thời sẽ làm cạn kiệt Gunicorn workers của    │
│    Odoo, khiến màn hình bán hàng tại trạm bị xoay vòng tròn.                                           │
│ 4. The Monolith Trap: Cố nhét Telemetry 86 triệu events/ngày vào Odoo sẽ gây bão Autovacuum và sập DB.│
└────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

```
┌────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ VÒNG 2: PHE SENIOR ODOO TẤN CÔNG 5 TỬ HUYỆT CỦA DATA PLATFORM                                          │
├────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ 🥊 Đòn 1 (TCO & Vận hành): "6 công cụ phân tán tốn tiền server, nếu em nghỉ việc ai bảo trì?"         │
│ 🥊 Đòn 2 (WAL Disk Bomb):  "REPLICA IDENTITY FULL làm phình to WAL 5 lần, Debezium treo làm nổ đĩa?"  │
│ 🥊 Đòn 3 (Eventual Lag):   "Dữ liệu trễ 15 phút, Giám đốc nhìn số lệch so với hóa đơn thực tế tại trạm?"│
│ 🥊 Đòn 4 (Schema Drift):   "Odoo sửa cột là Spark sập, team Data cản trở tốc độ release của ERP?"     │
│ 🥊 Đòn 5 (Over-engineering): "180GB/năm chạy PostgreSQL đủ sức, vẽ Hadoop/Spark để làm đẹp CV?"       │
└────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

```
┌────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ VÒNG 3: PHE DATA ENGINEER PHẢN CÔNG DỨT ĐIỂM (KIẾN TRÚC THƯỢNG TẦNG)                                   │
├────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ 🛡️ Phản công 1 (TCO): Tận dụng Bare-metal on-premise + Systemd/Shell native (0đ phí Cloud).            │
│    Observability tự động bằng Prometheus/Grafana giúp 1 kỹ sư quản trị trọn vẹn.                      │
│ 🛡️ Phản công 2 (WAL Safeguard): Chỉ bật FULL trên 2 bảng Master Data (`res_partner`). Thiết lập chốt an│
│    toàn `max_slot_wal_keep_size = 10GB` và Data Quality Gate quét `pg_wal_lsn_diff()` cảnh báo < 5m.  │
│ 🛡️ Phản công 3 (Consistency): Phân định rõ ràng: Giao dịch trạm = 100% ACID Strong Consistency;       │
│    Báo cáo Executive BI = T+15m (có nhãn `Data Freshness: Last updated at HH:mm`).                    │
│ 🛡️ Phản công 4 (Schema Contract): Áp dụng Schema Registry tự động tương thích ngược (Backward          │
│    Compatibility) cho trường mới; quy ước Data Contract cảnh báo trước 1 tuần cho Breaking Changes.   │
│ 🛡️ Phản công 5 (Khối lượng thật): 180GB là dữ liệu ERP; nhưng có thêm **86.4 triệu sự kiện/ngày       │
│    (~11TB/năm) từ Cảm biến Telemetry của 10.000 xe tải**. Bắt buộc phải có cụm Spark phân tán để       │
│    JOIN dữ liệu IoT với ERP phục vụ Bảo trì Dự đoán (Predictive Maintenance).                          │
└────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

# PHẦN 3: BỐN SỰ CỐ PRODUCTION THỰC CHIẾN & RUNBOOK XỬ LÝ GỐC RỄ (RCA)

---

### 🚨 SỰ CỐ 1 (SỰ CỐ KÉP ĐỈNH CAO): LỆCH LSN TRÊN DEBEZIUM + DATA SKEW TRÊN SPARK

```
┌────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ DIỄN BIẾN SỰ CỐ KÉP LÚC 08:30 SÁNG:                                                                    │
│  1. Debezium đứt kết nối mạng LAN → Postgres kích hoạt `max_slot_wal_keep_size` xóa WAL cũ để cứu đĩa.│
│  2. Debezium khởi động lại bị lỗi LSN Mismatch (`wal_status = 'lost'`).                                │
│  3. DE kích hoạt Signal Incremental Snapshot bù dữ liệu → 5 triệu bản ghi dồn dập đổ về Kafka.        │
│  4. Spark Batch Job bị treo ở Stage 3: Task 42 ôm 2.8GB Shuffle Read chạy 45 phút do 1 khách hàng lớn. │
└────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

#### 1. Nhật ký lỗi (Log Trace) trên Debezium:
```text
org.postgresql.util.PSQLException: ERROR: requested WAL segment 00000001000000A200000045 has already been removed
Caused by: io.debezium.DebeziumException: The replication slot 'debezium_fleet_slot' is invalid or WAL segment removed.
```

#### 2. Runbook xử lý Tầng Ingestion (PostgreSQL & Debezium):
```sql
-- Bước 1: Kiểm tra trạng thái slot bị 'lost'
SELECT slot_name, plugin, active, wal_status, restart_lsn, pg_current_wal_lsn() 
FROM pg_replication_slots WHERE slot_name = 'debezium_fleet_slot';

-- Bước 2: Tái tạo Replication Slot tại LSN hiện tại
SELECT pg_drop_replication_slot('debezium_fleet_slot');
SELECT pg_create_logical_replication_slot('debezium_fleet_slot', 'pgoutput');

-- Bước 3: Kích hoạt Incremental Chunked Snapshot bù dữ liệu không gây khóa bảng (DBLog Algorithm)
INSERT INTO debezium_signal (id, type, data) 
VALUES ('sig_01', 'execute-snapshot', '{"data-collections": ["public.invoices", "public.customers"], "type": "INCREMENTAL"}');
```

#### 3. Runbook xử lý Tầng Compute (Triệt tiêu Data Skew trên Spark UI):
* **Phát hiện trên Spark UI**: Task 42 có Duration $45.3\text{ phút}$ (Median $2.0\text{s}$), Shuffle Read $2.8\text{GB}$ (Median $22\text{MB}$). Key bị lệch: `customer_id = 1001` (chiếm $2.1$ triệu hóa đơn).
* **Mã nguồn PySpark Salting 2 giai đoạn giải quyết dứt điểm**:

```python
from pyspark.sql import functions as F

# GIAI ĐOẠN 1: Phân tán key bị lệch bằng 10 Salt Buckets
SALT_BUCKETS = 10
df_salted = df_invoices.withColumn(
    "salt_key", 
    F.concat(F.col("customer_id"), F.lit("_"), F.floor(F.rand() * SALT_BUCKETS))
)

# Chạy Partial Aggregation theo salt_key (Chia đều 2.1 triệu dòng sang 10 Partition)
df_partial_agg = df_salted.groupBy("salt_key").agg(
    F.sum("total_amount").alias("partial_revenue"),
    F.count("id").alias("partial_count")
)

# GIAI ĐOẠN 2: Bỏ đuôi Salt và Aggregate lần cuối trên kết quả nhỏ
df_final = df_partial_agg.withColumn(
    "customer_id", F.split(F.col("salt_key"), "_").getItem(0).cast("int")
).groupBy("customer_id").agg(
    F.sum("partial_revenue").alias("total_revenue"),
    F.sum("partial_count").alias("total_invoices")
)

# Ghi đè vào Gold Layer DWH
df_final.write.mode("overwrite").parquet("hdfs://master:9000/dwh/gold/fact_customer_revenue")
```
* **Kết quả**: Thời gian chạy giảm từ **$45.3\text{ phút} \downharpoonright 1.4\text{ phút}$ (Nhanh hơn $32\times$)**.

---

### 🚨 SỰ CỐ 2: SMALL FILE PROBLEM LÀM KIỆT QUỆ RAM NAMENODE HDFS
* **Hiện tượng**: Spark Streaming micro-batch 30s tạo ra hơn $150.000$ file Parquet nhỏ sau 2 tháng $\to$ NameNode heap memory chạm ngưỡng $85\%$, dính Java GC Pause liên tục làm đơ cụm.
* **Xử lý gốc rễ (RCA)**:
  1. Tăng trigger streaming từ $30\text{s} \to 5\text{ phút}$ cho các topic dữ liệu không cần phản hồi dưới 1 phút.
  2. Viết Airflow DAG chạy **Nightly Compaction Job** lúc 2h sáng:
     ```python
     # Đọc toàn bộ file nhỏ trong partition ngày hôm trước và gộp thành 4 file chuẩn 128MB
     df_daily = spark.read.parquet("hdfs://master:9000/lake/bronze/invoices/year=2026/month=08/day=17")
     df_daily.coalesce(4).write.mode("overwrite").parquet("hdfs://master:9000/lake/bronze/invoices/year=2026/month=08/day=17_compacted")
     ```
  3. **Kết quả**: Giảm **$98\%$ số lượng file**, giải phóng RAM NameNode về mức an toàn $40\%$.

---

### 🚨 SỰ CỐ 3: TRANH CHẤP GIỮ CHỖ (RACE CONDITION OVERBOOKING)
* **Hiện tượng**: Khung giờ vàng 9h sáng tại trạm trung tâm Hà Nội chỉ còn 2 slot sửa chữa. 100 tài xế xe tải cùng bấm đặt chỗ trên App đồng thời $\to$ PostgreSQL bị khóa dòng `SELECT FOR UPDATE` gây nghẽn hàng đợi, 5 tài xế cùng nhận thông báo thành công (Overbooking).
* **Xử lý gốc rễ (RCA)**:
  * Chuyển toàn bộ logic giữ chỗ sang **Redis 7+ Lua Script (Chạy đơn luồng nguyên tử trong RAM)**:
    ```lua
    -- Atomic Slot Reservation Script in Redis (Chạy trong < 0.05ms)
    local head_id = KEYS[1]
    local slot_time = ARGV[1]
    local driver_id = ARGV[2]
    local max_capacity = tonumber(ARGV[3])

    local current_booked = redis.call('SCARD', 'head:' .. head_id .. ':slot:' .. slot_time)
    if current_booked < max_capacity then
        redis.call('SADD', 'head:' .. head_id .. ':slot:' .. slot_time, driver_id)
        return 1 -- Đặt chỗ thành công
    else
        return 0 -- Đã hết chỗ
    end
    ```
  * **Kết quả**: Xử lý $5.000\text{ req/s}$ với độ trễ $< 1\text{ms}$, triệt tiêu **$100\%$ lỗi Overbooking**.

---

### 🚨 SỰ CỐ 4: CƠN ÁC MỘNG SCHEMA DRIFT
* **Hiện tượng**: Team ERP Odoo thêm trường `discount_rate` vào bảng `account_move` nhưng không thông báo $\to$ Debezium đẩy payload JSON mới $\to$ Spark Parquet bị Schema Mismatch Exception làm gãy pipeline đêm.
* **Xử lý gốc rễ (RCA)**:
  1. Triển khai **Schema Registry (Confluent)** kiểm soát tính tương thích ngược (**`BACKWARD_TRANSITIVE`**).
  2. Cấu hình Spark `spark.sql.parquet.mergeSchema = true` để tự động gộp các cột mới mà không làm crash job.
  3. Đưa bài test Schema Drift vào pipeline CI/CD của team ERP.

---

# PHẦN 4: TỔNG HỢP TOÀN BỘ CẤU HÌNH BẮT BUỘC ĐỂ CÀI ĐẶT 7 FRAMEWORKS BARE-METAL (NO DOCKER)

> **Khẳng định Bản lĩnh Thực chiến:**
> *"Để cài đặt và vận hành thành công toàn bộ hệ thống Fleet Platform trên cụm 3 máy chủ Bare-Metal Ubuntu (Master, Slave1, Slave2), dưới đây là **danh mục chính xác 100% các file cấu hình, đường dẫn và các tham số kỹ thuật bắt buộc** phải thiết lập cho từng framework."*

---

### 4.1 Bản Đồ Phân Bổ Dịch Vụ & IP Tĩnh Trên Cụm 3 Máy Chủ Vật Lý:

```
┌────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ BẢN ĐỒ PHÂN BỔ DỊCH VỤ TRÊN CỤM BARE-METAL (3 MÁY CHỦ VẬT LÝ NỐI MẠNG LAN 1Gbps):                      │
├──────────────────────────────────┬──────────────────────────────────┬──────────────────────────────────┤
│ master (192.168.1.10)            │ slave1 (192.168.1.11)            │ slave2 (192.168.1.12)            │
├──────────────────────────────────┼──────────────────────────────────┼──────────────────────────────────┤
│ • Hadoop NameNode (9870/9000)    │ • Hadoop DataNode (9864)         │ • Hadoop DataNode (9864)         │
│ • Hadoop SecondaryNameNode (9868)│ • Kafka Broker 1 (Port 9092)     │ • Kafka Broker 2 (Port 9092)     │
│ • Zookeeper Server (Port 2181)   │ • Spark Worker (Port 8081)       │ • Spark Worker (Port 8081)       │
│ • Kafka Broker 0 (Port 9092)     │                                  │                                  │
│ • Debezium Kafka Connect (8083)  │                                  │                                  │
│ • PostgreSQL 14+ OLTP (5432)     │                                  │                                  │
│ • Redis 7+ Server (6379)         │                                  │                                  │
│ • Spark Master (7077/8080)       │                                  │                                  │
│ • Hive Metastore (Port 9083)     │                                  │                                  │
│ • HiveServer2 Thrift (Port 10000)│                                  │                                  │
│ • Airflow Web (8081) + Scheduler │                                  │                                  │
└──────────────────────────────────┴──────────────────────────────────┴──────────────────────────────────┘
```

---

### 4.2 Framework 1: Hadoop HDFS 3.3.6 Multi-Node (Toàn Bộ File XML & Tham Số Bắt Buộc)

* **Môi trường**: OpenJDK 11, cài tại `/opt/hadoop/`. Thư mục cấu hình: `/opt/hadoop/etc/hadoop/`.

#### 1. File `hadoop-env.sh`:
```bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
```

#### 2. File `core-site.xml` (Cấu hình NameNode endpoint & Thư mục tạm):
```xml
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://master:9000</value>
        <description>Địa chỉ NameNode phục vụ RPC cho Spark và Hadoop clients</description>
    </property>
    <property>
        <name>hadoop.tmp.dir</name>
        <value>/opt/hadoop/data/tmp</value>
        <description>Thư mục tạm lưu trữ dữ liệu runtime của Hadoop</description>
    </property>
</configuration>
```

#### 3. File `hdfs-site.xml` (Cấu hình Replication, Thư mục DataNode & Chốt an toàn ổ đĩa):
```xml
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>2</value>
        <description>Số bản sao dữ liệu (khớp với số lượng 2 DataNodes trên cụm)</description>
    </property>
    <property>
        <name>dfs.namenode.name.dir</name>
        <value>file:///opt/hadoop/data/namenode</value>
        <description>Nơi lưu trữ metadata FsImage và EditLogs của NameNode</description>
    </property>
    <property>
        <name>dfs.datanode.data.dir</name>
        <value>file:///opt/hadoop/data/datanode</value>
        <description>Nơi lưu trữ các Block vật lý trên DataNodes</description>
    </property>
    <property>
        <name>dfs.datanode.du.reserved</name>
        <value>2147483648</value>
        <description>CHỐT AN TOÀN: Giữ lại đúng 2GB dung lượng đĩa trống cho OS Linux!</description>
    </property>
    <property>
        <name>dfs.namenode.http-address</name>
        <value>master:9870</value>
        <description>Cổng Web UI của NameNode</description>
    </property>
    <property>
        <name>dfs.namenode.secondary.http-address</name>
        <value>master:9868</value>
        <description>Cổng Web UI của Secondary NameNode</description>
    </property>
</configuration>
```

#### 4. File `workers` (Khai báo danh sách DataNodes):
```text
slave1
slave2
```

#### 5. Khởi tạo & Vận hành:
* **Khởi tạo (Chỉ chạy 1 LẦN DUY NHẤT)**: `/opt/hadoop/bin/hdfs namenode -format`
* **Khởi động cụm**: `/opt/hadoop/sbin/start-dfs.sh`
* **Kiểm tra tiến trình (`jps`)**:
  * Trên `master`: `NameNode`, `SecondaryNameNode`.
  * Trên `slave1`, `slave2`: `DataNode`.
* **Tạo thư mục Data Lake**: `/opt/hadoop/bin/hdfs dfs -mkdir -p /fleet-datalake /spark-logs`

***
Người phỏng vấn hỏi: "Nếu trong hdfs-site.xml em quên không cấu hình dfs.datanode.data.dir thì chuyện gì sẽ xảy ra?"

Bạn trả lời chuẩn Senior: *"Dạ thưa anh, nếu không khai báo dfs.datanode.data.dir trong hdfs-site.xml, Hadoop sẽ tự động lấy đường dẫn mặc định là ${hadoop.tmp.dir}/dfs/data.

Nếu hadoop.tmp.dir trong core-site.xml lại không được cấu hình nốt, nó sẽ fallback về /tmp/hadoop-${user.name} của Linux.

Đây là thảm họa lớn nhất của người mới cài Hadoop: Vì thư mục /tmp của Linux sẽ tự động bị xóa sạch khi máy chủ reboot 
⟹
⟹ Toàn bộ dữ liệu HDFS Data Lake sẽ bị xóa sổ sau một lần khởi động lại máy!

Vì vậy, trong dự án thực tế, em luôn cấu hình tách bạch rõ ràng:

hadoop.tmp.dir đặt tại /opt/hadoop/data/tmp (dành cho file runtime tạm).
dfs.datanode.data.dir đặt tại file:///opt/hadoop/data/datanode (dành cho lưu trữ block vĩnh viễn), kết hợp chốt an toàn dfs.datanode.du.reserved = 2GB để bảo vệ đĩa OS."*
***
---

### 4.3 Framework 2: Kafka Multi-Broker Cluster 3.7.1 (Toàn Bộ Tham Số Server.properties)

* **Môi trường**: Scala 2.13, cài tại `/opt/kafka/`. Thư mục cấu hình: `/opt/kafka/config/`.

#### 1. File `zookeeper.properties` (Chạy trên `master`):
```properties
dataDir=/opt/kafka/data/zookeeper
clientPort=2181
maxClientCnxns=0
admin.enableServer=false
```

#### 2. File `server.properties` (Cấu hình Broker trên TỪNG MÁY):

| Tham số cấu hình | Giá trị trên `master` | Giá trị trên `slave1` | Giá trị trên `slave2` | Ý nghĩa kỹ thuật |
|---|---|---|---|---|
| `broker.id` | **`0`** | **`1`** | **`2`** | ID định danh duy nhất của từng broker |
| `listeners` | `PLAINTEXT://master:9092` | `PLAINTEXT://slave1:9092` | `PLAINTEXT://slave2:9092` | Cổng mạng socket broker lắng nghe |
| `advertised.listeners` | `PLAINTEXT://master:9092` | `PLAINTEXT://slave1:9092` | `PLAINTEXT://slave2:9092` | Hostname broker công khai cho clients |
| `log.dirs` | `/opt/kafka/data/kafka-logs` | `/opt/kafka/data/kafka-logs` | `/opt/kafka/data/kafka-logs` | Thư mục lưu trữ log partitions trên đĩa |
| `zookeeper.connect` | `master:2181` | `master:2181` | `master:2181` | Địa chỉ kết nối cụm Zookeeper |
| `num.partitions` | `6` | `6` | `6` | Số partition mặc định cho topic mới |
| `default.replication.factor` | `3` | `3` | `3` | Số bản sao dữ liệu trên cả 3 brokers |
| `min.insync.replicas` | `2` | `2` | `2` | Yêu cầu tối thiểu 2 bản sao đồng bộ khi ghi |
| `auto.create.topics.enable` | `false` | `false` | `false` | Chống tự động tạo topic rác |
| `log.retention.hours` | `168` (7 ngày) | `168` | `168` | Thời gian lưu trữ dữ liệu trước khi xóa |

#### 3. Khởi động & Tạo Topic Production:
```bash
# 1. Khởi động Zookeeper trên master
/opt/kafka/bin/zookeeper-server-start.sh -daemon /opt/kafka/config/zookeeper.properties

# 2. Khởi động Broker trên cả 3 máy
/opt/kafka/bin/kafka-server-start.sh -daemon /opt/kafka/config/server.properties

# 3. Tạo Topic chuẩn CDC
/opt/kafka/bin/kafka-topics.sh --create --bootstrap-server master:9092,slave1:9092,slave2:9092 \
  --topic cdc.fleet.invoices --partitions 6 --replication-factor 3 --config min.insync.replicas=2
```

---

### 4.4 Framework 3: PostgreSQL 14+ Bare-Metal (Cấu Hình Logical Replication & Chốt WAL)

* **Môi trường**: PostgreSQL 14 Native Systemd Service trên Ubuntu `master`.

#### 1. File `/etc/postgresql/14/main/postgresql.conf`:
```ini
# Network & Port
listen_addresses = '*'
port = 5432
max_connections = 100

# Tối ưu hóa Bộ nhớ (Server 16GB RAM)
shared_buffers = 4GB                  # 25% tổng dung lượng RAM
work_mem = 64MB                       # Cấp phát per sort/hash
maintenance_work_mem = 1GB            # Dùng cho VACUUM, CREATE INDEX
effective_cache_size = 12GB           # 75% RAM gợi ý cho Query Planner

# Cấu hình BẮT BUỘC cho Logical Replication (CDC)
wal_level = logical                   # Cho phép giải mã WAL sang JSON
max_wal_senders = 10                  # Số tiến trình gửi WAL song song
max_replication_slots = 10            # Số lượng replication slot tối đa
max_slot_wal_keep_size = 10240MB      # CHỐT AN TOÀN: Tối đa 10GB WAL cho slot, chống nổ đĩa!
wal_buffers = 16MB
checkpoint_completion_target = 0.9
```

#### 2. File `/etc/postgresql/14/main/pg_hba.conf`:
```text
# Cho phép user fleet_cdc kết nối Replication từ IP Master
host    fleet_oltp      fleet_cdc       192.168.1.10/32         scram-sha-256
host    replication     fleet_cdc       192.168.1.10/32         scram-sha-256
```

#### 3. Khởi tạo Database, User & Bật REPLICA IDENTITY FULL:
```sql
-- Tạo Database nghiệp vụ & Database lưu Metadata Airflow
CREATE DATABASE fleet_oltp;
CREATE DATABASE airflow_metadata;

-- Tạo User CDC tuân thủ đặc quyền tối thiểu
CREATE USER fleet_cdc WITH REPLICATION PASSWORD 'SecurePass_VCS_2026';
GRANT CONNECT ON DATABASE fleet_oltp TO fleet_cdc;
GRANT USAGE ON SCHEMA public TO fleet_cdc;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO fleet_cdc;

-- Bật REPLICA IDENTITY FULL trên 2 bảng Master Data cần theo dõi SCD2
\c fleet_oltp
ALTER TABLE customers REPLICA IDENTITY FULL;
ALTER TABLE heads REPLICA IDENTITY FULL;
```

---

### 4.5 Framework 4: Debezium Distributed (Kafka Connect REST API Port 8083)

* **Môi trường**: Debezium PostgreSQL Connector 2.5+ đặt tại `/opt/kafka/plugins/`.

#### 1. File `/opt/kafka/config/connect-distributed.properties`:
```properties
bootstrap.servers=master:9092,slave1:9092,slave2:9092
group.id=fleet-connect-cluster

# 3 Topics nội bộ lưu trạng thái cấu hình của Kafka Connect
config.storage.topic=connect-configs
config.storage.replication.factor=3
offset.storage.topic=connect-offsets
offset.storage.replication.factor=3
offset.storage.partitions=25
status.storage.topic=connect-status
status.storage.replication.factor=3
status.storage.partitions=5

# Converter JSON (Lưu cấu trúc Schema trong Value)
key.converter=org.apache.kafka.connect.json.JsonConverter
value.converter=org.apache.kafka.connect.json.JsonConverter
key.converter.schemas.enable=false
value.converter.schemas.enable=true

# Thư mục chứa JAR plugin Debezium
plugin.path=/opt/kafka/plugins

# REST API Endpoint
rest.port=8083
rest.advertised.host.name=master
```

#### 2. File JSON Đăng ký Connector (`fleet-cdc-connector.json`):
```json
{
  "name": "fleet-postgres-cdc",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "tasks.max": "1",
    "plugin.name": "pgoutput",
    "database.hostname": "master",
    "database.port": "5432",
    "database.user": "fleet_cdc",
    "database.password": "SecurePass_VCS_2026",
    "database.dbname": "fleet_oltp",
    "database.server.name": "fleet",
    "table.include.list": "public.invoices,public.work_orders,public.customers,public.heads,public.components,public.parts_inventory",
    "decimal.handling.mode": "double",
    "publication.autocreate.mode": "all_tables",
    "slot.name": "debezium_fleet_slot"
  }
}
```

#### 3. Lệnh Khởi động & Nạp Connector qua cURL:
```bash
# Khởi động Kafka Connect Distributed daemon trên master
/opt/kafka/bin/connect-distributed.sh -daemon /opt/kafka/config/connect-distributed.properties

# Nạp cấu hình connector vào REST API
curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" \
  http://master:8083/connectors/ -d @/opt/debezium/config/fleet-cdc-connector.json
```

---

### 4.6 Framework 5: Apache Spark on Bare-Metal (Cầu Nối Classpath HDFS & History Server)

* **Môi trường**: Apache Spark 3.5.1 cài tại `/opt/spark/`. Thư mục cấu hình: `/opt/spark/conf/`.

#### 1. File `spark-env.sh`:
```bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export SPARK_MASTER_HOST=master
export SPARK_MASTER_PORT=7077
export SPARK_MASTER_WEBUI_PORT=8080
export SPARK_WORKER_CORES=4
export SPARK_WORKER_MEMORY=8g
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop

# BẮT BUỘC: Cầu nối Classpath để Spark đọc/ghi HDFS không bị lỗi ClassNotFoundException
export SPARK_DIST_CLASSPATH=$(/opt/hadoop/bin/hadoop classpath)
```

#### 2. File `spark-defaults.conf`:
```properties
spark.master                     spark://master:7077
spark.eventLog.enabled           true
spark.eventLog.dir               hdfs://master:9000/spark-logs
spark.history.fs.logDirectory    hdfs://master:9000/spark-logs
spark.sql.parquet.compression.codec snappy
spark.sql.adaptive.enabled       true
spark.sql.adaptive.skewJoin.enabled true
spark.serializer                 org.apache.spark.serializer.KryoSerializer
```

#### 3. File `workers`:
```text
slave1
slave2
```

#### 4. Khởi động Master, Workers & History Server:
```bash
/opt/spark/sbin/start-master.sh
/opt/spark/sbin/start-workers.sh
/opt/spark/sbin/start-history-server.sh   # Web UI xem log lịch sử tại port 18080
```

---

### 4.7 Framework 6: Redis 7+ Systemd (Cấu Hình Bảo Mật, LRU & Bền Vững Đĩa)

* **Môi trường**: Redis 7.2 Native Systemd Service trên Ubuntu `master`.

#### File cấu hình `/etc/redis/redis.conf`:
```ini
# Network & Security
bind 0.0.0.0
port 6379
protected-mode yes
requirepass StrongRedisPass_VCS_2026

# Tối ưu hóa Bộ nhớ RAM (Chống tràn RAM OS)
maxmemory 2gb
maxmemory-policy allkeys-lru           # Dọn dẹp key ít dùng nhất khi đầy 2GB

# Bền vững Dữ liệu Đĩa (AOF + RDB Snapshot)
appendonly yes
appendfsync everysec
dir /var/lib/redis
save 900 1
save 300 10
save 60 10000

# Quản lý qua Systemd
supervised systemd
```

#### Lệnh kích hoạt & Kiểm tra:
```bash
sudo systemctl restart redis-server
redis-cli -a StrongRedisPass_VCS_2026 ping      # Trả về PONG
```

---

### 4.8 Framework 7: Apache Hive 3.1.3 (Centralized Metastore Catalog & HiveServer2)

* **Môi trường**: Apache Hive 3.1.3 cài tại `/usr/local/hive` trên máy `master`. Quản lý Metadata tập trung qua **PostgreSQL Backend (`metastore_db`)** và cung cấp Thrift URI cho Spark SQL.

#### 1. File `/usr/local/hive/conf/hive-site.xml`:
```xml
<configuration>
    <!-- Kết nối PostgreSQL Metastore -->
    <property>
        <name>javax.jdo.option.ConnectionURL</name>
        <value>jdbc:postgresql://master:5432/metastore_db</value>
    </property>
    <property>
        <name>javax.jdo.option.ConnectionDriverName</name>
        <value>org.postgresql.Driver</value>
    </property>
    <property>
        <name>javax.jdo.option.ConnectionUserName</name>
        <value>hive</value>
    </property>
    <property>
        <name>javax.jdo.option.ConnectionPassword</name>
        <value>HivePass_VCS_2026</value>
    </property>

    <!-- Thư mục HDFS Warehouse & Scratch -->
    <property>
        <name>hive.metastore.warehouse.dir</name>
        <value>/user/hive/warehouse</value>
    </property>
    <property>
        <name>hive.exec.scratchdir</name>
        <value>/tmp/hive</value>
    </property>

    <!-- Thrift Metastore URI (Port 9083) cho Spark SQL/Trino -->
    <property>
        <name>hive.metastore.uris</name>
        <value>thrift://master:9083</value>
    </property>
    <property>
        <name>hive.metastore.schema.verification</name>
        <value>false</value>
    </property>

    <!-- HiveServer2 Port (Port 10000) -->
    <property>
        <name>hive.server2.thrift.port</name>
        <value>10000</value>
    </property>
    <property>
        <name>hive.server2.thrift.bind.host</name>
        <value>master</value>
    </property>
    <property>
        <name>hive.server2.enable.doAs</name>
        <value>false</value>
    </property>
</configuration>
```

#### 2. Bẫy lỗi sống còn & Khởi tạo Schema với `schematool`:
```bash
# BẮT BUỘC: Fix lỗi xung đột Guava JAR giữa Hadoop 3.x và Hive 3.x
rm /usr/local/hive/lib/guava-19.0.jar
cp /usr/local/hadoop/share/hadoop/common/lib/guava-*.jar /usr/local/hive/lib/

# BẮT BUỘC: Cấp quyền thư mục HDFS
hdfs dfs -mkdir -p /tmp/hive /user/hive/warehouse
hdfs dfs -chmod -R 777 /tmp/hive
hdfs dfs -chmod -R 775 /user/hive/warehouse

# Khởi tạo Schema Metastore trong PostgreSQL (Chỉ chạy 1 LẦN DUY NHẤT)
schematool -dbType postgres -initSchema

# Cầu nối Spark SQL: Copy hive-site.xml sang Spark
cp /usr/local/hive/conf/hive-site.xml /usr/local/spark/conf/
```

#### 3. Quản lý tự động qua Systemd (`hive-metastore.service` & `hive-server2.service`):
```bash
sudo systemctl enable --now hive-metastore    # Lắng nghe port 9083 Thrift
sudo systemctl enable --now hive-server2      # Lắng nghe port 10000 JDBC
```

---

### 4.9 Framework 8: Apache Airflow LocalExecutor (Python Virtualenv & Systemd Units)

* **Môi trường**: Python 3.10 Virtualenv tại `/opt/airflow/venv/`. Home: `/opt/airflow/`.

#### 1. File `/opt/airflow/airflow.cfg`:
```ini
[core]
dags_folder = /opt/airflow/dags
executor = LocalExecutor              # Chạy song song đa tiến trình trên máy Master
sql_alchemy_conn = postgresql+psycopg2://airflow:AirflowPass2026@master:5432/airflow_metadata
load_examples = False
parallelism = 16
max_active_tasks_per_dag = 8
max_active_runs_per_dag = 4
dag_dir_list_interval = 30           # Quét DAG mới mỗi 30 giây

[webserver]
base_url = http://master:8081
web_server_port = 8081
secret_key = SuperSecretKey_VCS_2026
```

#### 2. Tạo 2 Systemd Services quản lý tự động:

* **File `/etc/systemd/system/airflow-webserver.service`**:
```ini
[Unit]
Description=Airflow Webserver Daemon
After=network.target postgresql.service

[Service]
Environment="PATH=/opt/airflow/venv/bin:/usr/local/bin:/usr/bin"
Environment="AIRFLOW_HOME=/opt/airflow"
User=aiguystory
ExecStart=/opt/airflow/venv/bin/airflow webserver --port 8081
Restart=always
RestartSec=5s

[Install]
WantedBy=multi-user.target
```

* **File `/etc/systemd/system/airflow-scheduler.service`**:
```ini
[Unit]
Description=Airflow Scheduler Daemon
After=network.target postgresql.service

[Service]
Environment="PATH=/opt/airflow/venv/bin:/usr/local/bin:/usr/bin"
Environment="AIRFLOW_HOME=/opt/airflow"
User=aiguystory
ExecStart=/opt/airflow/venv/bin/airflow scheduler
Restart=always
RestartSec=5s

[Install]
WantedBy=multi-user.target
```

---

### 4.10 Kịch Bản Kiểm Thử Tự Động Toàn Bộ Hạ Tầng (`verify-all-infra.sh` 8/8)

Bạn tự hào trình bày với Hội đồng rằng bạn đã tự tay viết một **Bash Script tự động hóa kiểm tra sức khỏe của 8/8 thành phần hạ tầng**:

```bash
#!/bin/bash
# Script: /fleet-platform/infra/scripts/verify-all-infra.sh

echo "━━━ [1/8] Hadoop HDFS Multi-Node ━━━"
ssh master jps | grep -q NameNode && echo "  ✓ NameNode (master)"
ssh slave1 jps | grep -q DataNode && echo "  ✓ DataNode (slave1)"
ssh slave2 jps | grep -q DataNode && echo "  ✓ DataNode (slave2)"
hdfs dfsadmin -safemode get | grep -q OFF && echo "  ✓ HDFS Safemode OFF"

echo "━━━ [2/8] Kafka Multi-Broker Cluster ━━━"
/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server master:9092,slave1:9092,slave2:9092 2>&1 | grep -c ':9092' | grep -q 3 && echo "  ✓ All 3 Kafka Brokers Registered"

echo "━━━ [3/8] PostgreSQL Logical CDC & Hive Metastore ━━━"
sudo -u postgres psql -tAc "SHOW wal_level;" | grep -q logical && echo "  ✓ wal_level = logical"
sudo -u postgres psql -tAc "SELECT 1 FROM pg_database WHERE datname='metastore_db';" | grep -q 1 && echo "  ✓ Hive metastore_db OK"

echo "━━━ [4/8] Debezium Connector ━━━"
curl -sf http://master:8083/connectors/fleet-postgres-cdc/status | grep -q RUNNING && echo "  ✓ Debezium Task RUNNING"

echo "━━━ [5/8] Redis GEO Serving ━━━"
redis-cli -a StrongRedisPass_VCS_2026 ping | grep -q PONG && echo "  ✓ Redis PONG"

echo "━━━ [6/8] Spark Cluster ━━━"
ssh master jps | grep -q Master && ssh slave1 jps | grep -q Worker && echo "  ✓ Spark Master & Workers OK"

echo "━━━ [7/8] Apache Hive Metastore & HMS ━━━"
ss -tlnp | grep -q :9083 && echo "  ✓ Hive Metastore (Port 9083)"
ss -tlnp | grep -q :10000 && echo "  ✓ HiveServer2 (Port 10000)"

echo "━━━ [8/8] Airflow Scheduler ━━━"
systemctl is-active --quiet airflow-scheduler && echo "  ✓ Airflow Scheduler Active"
```

---

### 4.11 Bảng Tổng Hợp Các "Bẫy Cấu Hình Chết Người" Cần Ghim Chặt Trong Đầu

```
┌──────────────────────────┬──────────────────────────────────────────┬──────────────────────────────────────────┐
│ Framework                │ Bẫy Cấu Hình Nguy Hiểm                   │ Cách Xử Lý Chuẩn Xác Của Senior DE       │
├──────────────────────────┼──────────────────────────────────────────┼──────────────────────────────────────────┤
│ 1. Hadoop HDFS           │ Dòng `127.0.1.1 slave1` trong `/etc/hosts`│ Xóa sạch dòng `127.0.1.1`, chỉ map IP LAN│
│                          │ làm DataNode bind nhầm loopback mạng.    │ để NameNode nhìn thấy DataNodes.         │
├──────────────────────────┼──────────────────────────────────────────┼──────────────────────────────────────────┤
│ 2. Hadoop Format         │ Format NameNode lần 2 làm lệch ClusterID │ Chỉ format 1 lần; nếu format lại phải xóa│
│                          │ khiến DataNodes tự động crash.           │ sạch thư mục data trên cả DataNodes.     │
├──────────────────────────┼──────────────────────────────────────────┼──────────────────────────────────────────┤
│ 3. Hadoop Disk Full      │ HDFS ghi đầy 100% đĩa cứng DataNode làm  │ Cấu hình `dfs.datanode.du.reserved = 2GB`│
│                          │ máy chủ Linux bị treo cứng.              │ trong `hdfs-site.xml` để bảo vệ OS.      │
├──────────────────────────┼──────────────────────────────────────────┼──────────────────────────────────────────┤
│ 4. Kafka Advertised      │ `listeners=0.0.0.0` trả về IP 0.0.0.0 làm│ Bắt buộc đặt `advertised.listeners` trỏ  │
│                          │ client bên ngoài không thể kết nối.      │ đúng hostname/IP LAN (`master:9092`).    │
├──────────────────────────┼──────────────────────────────────────────┼──────────────────────────────────────────┤
│ 5. PostgreSQL CDC        │ Đổi `wal_level=logical` chỉ reload làm   │ Bắt buộc phải `sudo systemctl restart    │
│                          │ Postgres không nhận cấu hình CDC.        │ postgresql` thì WAL logical mới có hiệu lực│
├──────────────────────────┼──────────────────────────────────────────┼──────────────────────────────────────────┤
│ 6. Debezium Data Type    │ Không set `decimal.handling.mode=double` │ Bắt buộc thêm `"decimal.handling.mode":  │
│                          │ làm kiểu NUMERIC bị biến thành Base64.   │ "double"` trong JSON connector config.   │
├──────────────────────────┼──────────────────────────────────────────┼──────────────────────────────────────────┤
│ 7. Spark Classpath       │ Spark không đọc được HDFS, báo lỗi       │ Thêm `export SPARK_DIST_CLASSPATH=$(hadoop│
│                          │ `ClassNotFound: FSDataInputStream`.      │ classpath)` vào `spark-env.sh`.          │
├──────────────────────────┼──────────────────────────────────────────┼──────────────────────────────────────────┤
│ 8. Hive Guava Conflict   │ Lỗi `NoSuchMethodError: checkArgument`   │ Xóa `guava-19.0.jar` trong Hive, copy    │
│    & HDFS Permissions    │ do xung đột Guava Hadoop 3 và Hive 3.    │ `guava-27.0-jre.jar` từ Hadoop sang.     │
└──────────────────────────┴──────────────────────────────────────────┴──────────────────────────────────────────┘
```

---

# PHẦN 5: KỶ LUẬT KỸ THUẬT, QUẢN TRỊ SOURCE CODE & CI/CD NATIVE

```
┌────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ QUY TRÌNH QUẢN TRỊ SOURCE CODE & CI/CD NATIVE (KHÔNG PHỤ THUỘC DOCKER):                                │
│                                                                                                        │
│  [Developer Commit] ──► [Pre-commit Hooks] ──► [Pull Request] ──► [GitLab CI Runner] ──► [Deploy VM]  │
│  (feature/*)            (Gitleaks/Flake8/      (Code Review       (PyTest PySpark        (Shell Sync   │
│                          Sqlfluff)              >= 1 Approval)     & Great Expectations)  & Systemd)   │
└────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

### 5.1 Quy chuẩn Git Flow:
* **`main`**: Nhánh Production. Bắt buộc bảo vệ nghiêm ngặt (Protected Branch), cấm push trực tiếp, yêu cầu ít nhất 1 Senior/Lead phê duyệt PR và $100\%$ bài test CI passed.
* **`develop`**: Nhánh Staging tích hợp liên tục.
* **`feature/*`**, **`hotfix/*`**: Phân nhánh theo từng Jira Ticket (`feature/FLEET-102-scd2-merge`).

### 5.2 Pre-commit Hooks & Quét lộ lọt bảo mật:
* **`gitleaks` / `trufflehog`**: Tự động chặn commit nếu phát hiện có chuỗi mật khẩu, token API, credentials (Đặc thù an ninh mạng VCS).
* **`flake8` & `black`**: Chuẩn hóa định dạng code Python theo PEP8.
* **`sqlfluff`**: Linter kiểm tra cú pháp và quy chuẩn đặt tên SQL cho toàn bộ DDL/DML.

### 5.3 Chiến lược Kiểm thử Tự động (Automated Testing):
1. **PySpark Unit Testing với Pytest**: Tạo local `SparkSession` giả lập để test độc lập các hàm transform, logic SCD2 và logic tính Năm tài khóa.
2. **Data Quality Testing (Great Expectations)**: Tự động kiểm tra tính duy nhất của Primary Key, Null constraint trên các trường bắt buộc, và độ lệch phân phối dữ liệu.

### 5.4 Pipeline CI/CD Native:
* **Stage 1 (Lint & Security Scan)**: SonarQube + Gitleaks.
* **Stage 2 (Unit & Integration Test)**: Chạy 100% Pytest và Schema validation.
* **Stage 3 (Deploy Native)**: Tự động sync file DAGs vào `/opt/airflow/dags`, copy file Python vào `/opt/spark/jobs` và gửi API reload connector trên Debezium.

---

# PHẦN 6: QUẢN TRỊ CÔNG VIỆC AGILE & LÀM VIỆC NHÓM LIÊN PHÒNG BAN

```
┌──────────────────────────┬─────────────────────────────────────────────────────────────────────────────┐
│ Đối Tác Liên Phòng Ban   │ Cách Thức Làm Việc, Đàm Phán & Giải Quyết Bất Đồng Kỹ Thuật                 │
├──────────────────────────┼─────────────────────────────────────────────────────────────────────────────┤
│ 1. Đội ngũ DBA & ERP     │ • Thống nhất cấu hình `wal_level=logical` và chốt `max_slot_wal_keep_size`. │
│                          │ • Cam kết 0% query polling đè lên DB vận hành; phối hợp lịch migration.     │
├──────────────────────────┼─────────────────────────────────────────────────────────────────────────────┤
│ 2. Đội ngũ BI & Phân tích│ • Ký kết Data Contract: Định nghĩa rõ Grain của Fact và chuẩn hóa Data Dict.│
│                          │ • Cam kết SLA độ tươi dữ liệu $T+15\text{ phút}$; hỗ trợ tối ưu PowerBI < 1s│
├──────────────────────────┼─────────────────────────────────────────────────────────────────────────────┤
│ 3. Đội ngũ Hạ Tầng & ATTT│ • Cấp phát tài nguyên YARN/HDFS; tuân thủ chính sách cách ly mạng VLAN.     │
│    (Infra & Security)    │ • Quản lý mật khẩu qua HashiCorp Vault; phối hợp diễn tập phục hồi sự cố.   │
├──────────────────────────┼─────────────────────────────────────────────────────────────────────────────┤
│ 4. Product Owner / Lãnh đạo│ • Dịch bài toán kinh doanh thành chỉ số kỹ thuật (KPIs/SLAs).             │
│                          │ • Báo cáo tiến độ minh bạch trên Jira Sprint 2 tuần; chứng minh ROI dự án.  │
└──────────────────────────┴─────────────────────────────────────────────────────────────────────────────┘
```

---

# PHẦN 7: TƯ DUY AN TOÀN & BẢO MẬT DỮ LIỆU (VCS MINDSET)

1. **Che mờ Dữ liệu Định danh Cá nhân (PII Data Masking)**:
   * Số điện thoại, CCCD, biển số xe tài xế được băm một chiều (SHA-256 có Salt) hoặc Masking (`0912***678`) ngay tại tầng Ingestion của Spark trước khi ghi xuống HDFS Data Lake.
2. **Nguyên tắc Đặc quyền Tối thiểu (Principle of Least Privilege & RBAC)**:
   * Tài khoản Debezium chỉ có quyền `REPLICATION` và `SELECT` trên các bảng cần thiết.
   * Phân quyền Role-Based Access Control trên PostgreSQL và HDFS: BI Analysts chỉ được đọc trên schema Gold (`Data Mart`), tuyệt đối không có quyền truy cập Bronze/Silver.
3. **Quản trị Bí mật (Secrets Management)**:
   * Không bao giờ lưu mật khẩu plain text trong code hoặc config. Toàn bộ được nạp từ **HashiCorp Vault / Linux Environment Variables** có phân quyền `chmod 600`.
4. **Audit Logging & Dấu vết Nguồn gốc Dữ liệu (Data Lineage)**:
   * Lưu vết $100\%$ dòng chảy dữ liệu từ LSN WAL PostgreSQL $\to$ Topic/Offset Kafka $\to$ Spark Job ID $\to$ Bảng DWH phục vụ kiểm toán an toàn thông tin.

---

# PHẦN 8: BỘ CÂU HỎI KỸ THUẬT ĐÀO SÂU POSTGRESQL & BIG DATA

```
┌───────────────────────────────────────┬────────────────────────────────────────────────────────────────┐
│ Câu Hỏi Phỏng Vấn                     │ Điểm Chốt Hạ Kỹ Thuật Cần Nhớ (60-90 Giây)                    │
├───────────────────────────────────────┼────────────────────────────────────────────────────────────────┤
│ • DATE vs DATETIME vs TIMESTAMP?      │ DATE (4B), DATETIME (8B raw clock), TIMESTAMP (UTC auto).      │
│   Bẫy Indexing thời gian?             │ Bẫy `WHERE DATE(col)` làm mất SARGable; dùng range scan và     │
│                                       │ **BRIN Index** cho telemetry (nhỏ hơn 95% B-Tree).             │
├───────────────────────────────────────┼────────────────────────────────────────────────────────────────┤
│ • MVCC & Table Bloat trong Postgres?  │ `UPDATE` tạo tuple mới + đánh dấu tuple cũ là Dead Tuple `xmax`│
│                                       │ Standard VACUUM dọn dead space vào FSM; `pg_repack` rebuild    │
│                                       │ online không khóa; tune `autovacuum_vacuum_scale_factor = 0.05`│
├───────────────────────────────────────┼────────────────────────────────────────────────────────────────┤
│ • Memory Architecture trong Postgres? │ `shared_buffers` (25% RAM); `work_mem` cấp phát per sort/hash! │
│                                       │ Set `SET work_mem = '256MB'` cho session ETL để chạy in-memory.│
├───────────────────────────────────────┼────────────────────────────────────────────────────────────────┤
│ • Parquet Format & Kích thước chuẩn?  │ Columnar + Snappy + Min/Max Metadata. Dung lượng chuẩn:        │
│                                       │ **128MB - 512MB/file** (khớp HDFS block size 128/256MB).       │
├───────────────────────────────────────┼────────────────────────────────────────────────────────────────┤
│ • Spark Persist trên Disk?            │ `MEMORY_AND_DISK_SER` lưu mảng byte trên JVM Storage Memory.   │
│                                       │ Đầy RAM thì `BlockManager` dùng **LRU** spill xuống disk.      │
├───────────────────────────────────────┼────────────────────────────────────────────────────────────────┤
│ • Ý nghĩa Q1, Q2, Q3, Median, σ?      │ Boxplot 5-number: $IQR = Q3-Q1$ chứa 50% dữ liệu lõi; Outlier  │
│                                       │ ngoài $[Q1-1.5IQR, Q3+1.5IQR]$. Median chống chịu Outlier tốt  │
│                                       │ hơn Mean; Độ lệch chuẩn $\sigma$ đo rủi ro biến động SLA.      │
└───────────────────────────────────────┴────────────────────────────────────────────────────────────────┘
```

---

# PHẦN 9: KỊCH BẢN TRÌNH BÀY MẪU 10 PHÚT CHUẨN STAR & 2 CÂU HỎI CUỐI

### 🎬 KỊCH BẢN NÓI MẪU 10 PHÚT CHUẨN STAR (NÓI TRONG PHÒNG PHỎNG VẤN):

> **[Phút 1-2: Giới thiệu & Bối cảnh bài toán kinh doanh]**
> *"Kính chào các anh trong Hội đồng Tuyển dụng Viettel Cyber Security. Dự án em tâm đắc nhất và thể hiện rõ nét nhất năng lực Data Engineering toàn diện của em là **Fleet Maintenance & Repair Data Platform** — hệ thống nền tảng dữ liệu tập trung phục vụ chuỗi 50 trạm dịch vụ xe tải trên toàn quốc.*
> *Bài toán thực tế lúc đó là: Hệ thống ERP Odoo trên PostgreSQL thường xuyên bị nghẽn I/O cuối tháng; Ban Giám đốc phải chờ báo cáo Excel thủ công tới T+30 ngày; và việc cập nhật đè trạng thái khách hàng làm mất sạch dữ liệu lịch sử tài khóa.*
>
> **[Phút 3-5: Đột phá Kiến trúc, Cài đặt Bare-Metal & Kỷ luật Kỹ thuật]**
> *Với vai trò là Lead Data Engineer chịu trách nhiệm thiết kế và triển khai toàn bộ nền tảng, em đã thực hiện 4 đột phá kiến trúc:*
> *1. Thay vì query polling gây áp lực lên DB vận hành, em cấu hình **Debezium CDC đọc trực tiếp WAL của PostgreSQL với REPLICA IDENTITY FULL**, đẩy qua cụm Kafka 3 brokers. Giải pháp này giúp triệt tiêu **100% tải query lên OLTP**, bắt trọn vẹn sự kiện DELETE và trạng thái trung gian.*
> *2. Về hạ tầng: Toàn bộ hệ thống được cài đặt **trực tiếp trên cụm 3 máy chủ Bare-Metal Ubuntu**, cấu hình native từ HDFS 3.3.6, Kafka 3.7.1, Spark, PostgreSQL đến Redis mà không qua Docker để tối ưu $100\%$ hiệu năng I/O đĩa và kiểm soát tuyệt đối bẫy mạng loopback hay chốt dung lượng `du.reserved`.*
> *3. Xây dựng Data Lakehouse trên **HDFS Parquet** và DWH theo **Star Schema**, tự tay lập trình logic **SCD Type 2** cho bảng chiều khách hàng với Surrogate Key MD5, bảo toàn 100% lịch sử giao dịch.*
> *4. Tối ưu Serving Layer với **Redis 7+**: Lệnh `GEOSEARCH` đưa thời gian tìm trạm từ 85ms xuống dưới 1ms; lập trình **Redis Lua Script nguyên tử** triệt tiêu 100% lỗi đặt trùng lịch; và lớp Backend **WebSocket** chủ động đẩy số liệu mới xuống Dashboard trong dưới 15ms.*
> *5. Về quản trị mã nguồn: Áp dụng nghiêm ngặt **Git Flow**, Pre-commit hooks quét lỗi cú pháp và tự động quét lộ lọt mật khẩu bằng **Gitleaks**, kiểm thử tự động với Pytest và triển khai CI/CD native.*
>
> **[Phút 6-8: Năng lực Xử lý Sự cố Thực chiến & Bài học RCA]**
> *Hành trình làm dự án cũng cho em những bài học xương máu thực tế. Đáng nhớ nhất là **Sự cố kép giữa Lệch LSN trên Debezium và Data Skew trên Spark**:*
> *Khi mạng LAN bị đứt, Postgres tự giải phóng WAL > 10GB để cứu đĩa khiến slot bị lost. Em đã lập tức tái tạo slot tại LSN mới, kích hoạt **Signal Incremental Snapshot** bù dữ liệu không gây khóa bảng. Khi 5 triệu bản ghi dồn về Kafka làm Spark bị treo Task 42 mất 45 phút, em mở Spark UI phát hiện Skew trên khách hàng lớn và áp dụng **Kỹ thuật Salting 2 giai đoạn**, đưa thời gian chạy từ **45 phút xuống còn 1.4 phút**.*
>
> **[Phút 9-10: Outcome & Khẳng định Giá trị với VCS]**
> *Kết quả chung cuộc: Hệ thống đưa độ trễ báo cáo từ **T+30 ngày xuống T+15 phút**, giảm **100% tải trên OLTP**, đạt thông lượng xử lý **86 triệu sự kiện Telemetry/ngày** và bảo mật PII tuyệt đối.*
> *Em tin rằng tư duy kiến trúc vững chắc, kỷ luật quản trị code bài bản và kinh nghiệm làm chủ hạ tầng Bare-Metal thực chiến này sẽ đóng góp ngay lập tức vào các bài toán dữ liệu lớn và an ninh mạng tại Viettel Cyber Security."*

---

### 🎯 2 CÂU HỎI CHIẾN LƯỢC DÀNH CHO BẠN HỎI NGƯỢC HỘI ĐỒNG Ở PHÚT 28:

1. *"Tại Viettel Cyber Security, với đặc thù dữ liệu an ninh mạng và log SIEM có thông lượng hàng trăm ngàn events/giây, kiến trúc Data Platform của team hiện tại đang ưu tiên tối ưu hóa theo hướng **Streaming-First (như Apache Flink / Kafka Streams)** kết hợp kho dữ liệu dạng cột **ClickHouse / Apache Iceberg** như thế nào?"*
2. *"Đối với một Data Engineer tại VCS, tiêu chuẩn về **Kỷ luật kỹ thuật (Engineering Standards)** và **Văn hóa chủ động làm chủ sự cố (Ownership & Incident Response)** được đo lường và kỳ vọng cụ thể ra sao trong giai đoạn thử việc?"*
