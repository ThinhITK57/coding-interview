# P10 — BẢN TỔNG HỢP CHIẾN LƯỢC PHỎNG VẤN VÒNG 2 (30 PHÚT)
## Chuẩn Hóa Theo Góc Nhìn Data Engineer: Đào Sâu PostgreSQL, Đột Phá Kiến Trúc & Outcome Định Lượng

> **Tôn Chỉ Phỏng Vấn Data Engineer Senior:**
> *"Một Data Engineer không nói chuyện lý thuyết mô hình trừu tượng. Mỗi giải pháp kỹ thuật đều phải giải thích được qua 4 yếu tố: **(1) Điểm nghẽn dữ liệu & I/O**, **(2) Đột phá kiến trúc pipeline & cơ sở dữ liệu**, **(3) Quản trị rủi ro & độ tin cậy hệ thống**, và **(4) Con số đo lường (Outcome) về độ trễ, thông lượng và tài nguyên**."*

---

# MỤC LỤC CHIẾN LƯỢC

```
┌────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ PHẦN 0: CHIẾN THUẬT PHÂN BỔ THỜI GIAN 30 PHÚT VÒNG 2                                                  │
├────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ PHẦN 1: BA DỰ ÁN DƯỚI GÓC NHÌN DATA ENGINEER — OUTCOME & ĐỘT PHÁ KIẾN TRÚC                             │
│   1.1 Dự Án 1: Fleet Maintenance & Repair Data Platform (PostgreSQL CDC → Lakehouse → Redis Serving)   │
│   1.2 Dự Án 2: CarDamageSAGE (Multi-modal Data Ingestion, Vector Deduplication & Low-Latency Serving)   │
│   1.3 Dự Án 3: Student Behavioral Streaming Platform (Kafka Stream Ingestion & Neo4j Graph Storage)    │
├────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ PHẦN 2: CHUYÊN ĐỀ ĐÀO SÂU POSTGRESQL CHO SENIOR DATA ENGINEER (TỔNG HỢP CÁC CÂU HỎI HÓC BÚA)          │
│   PG.1 Bản chất MVCC, Dead Tuples, Table Bloat & Chiến lược Autovacuum Tuning cho bảng CDC/Staging     │
│   PG.2 WAL (Write-Ahead Log), Replication Slots, Plugin pgoutput & Quản trị rủi ro đĩa đầy            │
│   PG.3 Memory Architecture: shared_buffers, work_mem, maintenance_work_mem cho tác vụ ETL/Batch       │
│   PG.4 Masterclass Indexing: B-Tree, BRIN cho Time-series, GIN cho JSONB, Partial & Covering (INCLUDE) │
│   PG.5 Đọc hiểu và tối ưu kế hoạch thực thi EXPLAIN (ANALYZE, BUFFERS)                                │
│   PG.6 Declarative Partitioning & Cơ chế Partition Pruning                                            │
│   PG.7 Concurrency Control, Khóa (Locks) & An toàn Migration với CREATE INDEX CONCURRENTLY            │
├────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ PHẦN 3: BỘ 13 CÂU HỎI VÒNG 1 ĐÃ TINH LỌC — ĐÁP ÁN CHUẨN SENIOR DE (60-90 GIÂY/CÂU)                     │
│   Q1. Roadmap Data Engineer Middle cần đạt những gì?                                                   │
│   Q2. Hiểu thế nào về Data Modeling (3NF vs Star Schema vs OBT)?                                       │
│   Q3. DATE vs DATETIME vs TIMESTAMP & Indexing dữ liệu thời gian?                                      │
│   Q4. Các phương pháp tối ưu hóa câu truy vấn SQL & Spark SQL?                                         │
│   Q5. Những cấu trúc dữ liệu đã ứng dụng trong thực tế dự án DE?                                      │
│   Q6. Tại sao sử dụng HDFS? Framework tương tự và Trade-offs?                                          │
│   Q7. Ứng dụng thống kê & khai phá dữ liệu để tối ưu mở rộng hệ thống?                                 │
│   Q8. Phân biệt bản chất OLTP và OLAP?                                                                 │
│   Q9. File Format Parquet trong Spark: Tiêu chuẩn dung lượng và số dòng tối ưu?                        │
│   Q10. Cơ chế Spark Persist trên Disk hoạt động như thế nào?                                           │
│   Q11. Hiểu và áp dụng Data Governance trong Data Platform?                                            │
│   Q12. Các quy tắc vàng khi thiết kế và tối ưu Index dữ liệu?                                          │
│   Q13. Ý nghĩa bản chất của Q1, Q2, Q3, Median, Phương sai, Độ lệch chuẩn trong báo cáo dữ liệu?       │
├────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ PHẦN 4: HAI KỊCH BẢN ĐỘT PHÁ DỰ PHÒNG CHO VÒNG 2 (DATA SKEW & PRODUCTION INCIDENT RCA)                 │
└────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

# PHẦN 0: CHIẾN THUẬT PHÂN BỔ THỜI GIAN 30 PHÚT VÒNG 2

```
┌───────────────────┬──────────────┬─────────────────────────────────────────────────────────────────────┐
│ Khung Thời Gian   │ Thời Lượng   │ Mục Tiêu Chiến Lược Cần Đạt                                         │
├───────────────────┼──────────────┼─────────────────────────────────────────────────────────────────────┤
│ 00:00 – 03:00     │ 3 Phút       │ Mở đầu: Giới thiệu bản thân ngắn gọn, nêu rõ định vị Senior/Lead DE.│
│ 03:00 – 16:00     │ 13 Phút      │ Trình bày 2 dự án trọng điểm (Fleet Platform & CarDamageSAGE) theo  │
│                   │              │ cấu trúc Problem → DE Architecture → Measurable Outcomes.           │
│ 16:00 – 27:00     │ 11 Phút      │ Xử lý nhanh gọn các câu hỏi kỹ thuật chuyên sâu (PostgreSQL & BigData)│
│ 27:00 – 30:00     │ 3 Phút       │ Kết thúc: Đặt 2 câu hỏi chiến lược ngược lại cho Nhà Tuyển Dụng.    │
└───────────────────┴──────────────┴─────────────────────────────────────────────────────────────────────┘
```

---

# PHẦN 1: BA DỰ ÁN DƯỚI GÓC NHÌN DATA ENGINEER — OUTCOME & ĐỘT PHÁ KIẾN TRÚC

---

## 1.1 Dự Án 1: Fleet Maintenance & Repair Data Platform (Flagship)

```
┌────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ KIẾN TRÚC DÒNG CHẢY DỮ LIỆU END-TO-END (DATA ENGINEERING PIPELINE)                                     │
│                                                                                                        │
│  [PostgreSQL OLTP] ──(WAL Log)──► [Debezium CDC] ──► [Kafka Cluster (3 Brokers)]                       │
│  (REPLICA IDENTITY FULL)          (pgoutput plugin)    (RF=3, ISR=2, Retention 7d)                     │
│                                                                  │                                     │
│                                 ┌────────────────────────────────┴────────────────────────────────┐    │
│                                 ▼                                                                 ▼    │
│                     [Spark Structured Streaming]                                         [Redis 7+ Sync]│
│                     (Micro-batch 30s + Watermark 10m)                                    (GEOSEARCH <1ms│
│                                 │                                                         Lua Atomic)  │
│                                 ▼                                                                 │    │
│                     [HDFS Data Lake (Bronze)]                                                     │    │
│                     (Parquet, Partitioned by Date)                                                │    │
│                                 │                                                                 │    │
│                                 ▼                                                                 ▼    │
│                     [Airflow DAGs Batch Processing]                                      [FastAPI Backend]
│                     (Nightly Compaction + SCD2 DWH)                                       (WebSocket Push)
│                                 │                                                                 │    │
│                                 ▼                                                                 ▼    │
│                     [Star Schema DWH (Gold)] ──────────────────────────────────────────► [Executive BI]│
│                     (2 Fact Tables + 4 Dimensions)                                       (Latency <15ms│
└────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

### 1. Bối cảnh & Điểm nghẽn dữ liệu (Problem & Data Bottlenecks):
* Chuỗi 50 trạm bảo dưỡng xe tải vận hành trên PostgreSQL (Odoo ERP).
* **3 điểm nghẽn nghiêm trọng**:
  1. *Quá tải cơ sở dữ liệu vận hành*: Các câu truy vấn báo cáo doanh thu tài khóa và phân tích vị trí địa lý đè trực tiếp lên PostgreSQL OLTP, gây khóa bảng và làm chậm giao dịch bán hàng tại trạm.
  2. *Độ trễ báo cáo quá lớn*: Ban Giám đốc chỉ nhận báo cáo qua file Excel tổng hợp thủ công vào cuối tháng (**T+30 ngày**).
  3. *Mất dấu vết lịch sử*: Khi trạng thái khách hàng thay đổi (*Mới $\to$ Thường niên*), OLTP ghi đè dữ liệu (`UPDATE`) khiến báo cáo tài chính của các quý trước bị sai lệch số liệu lịch sử.

### 2. Bốn đột phá kiến trúc Data Engineering của bản thân:
1. **Giải phóng 100% tải DB bằng Log-based CDC (Debezium + PostgreSQL WAL)**:
   * Cấu hình logical replication với plugin `pgoutput` đọc trực tiếp Write-Ahead Log (WAL) của PostgreSQL, thiết lập `REPLICA IDENTITY FULL` để bắt trọn vẹn bản ghi trước (before-image) và sau (after-image).
   * Publish toàn bộ thay đổi vào Kafka Cluster (3 brokers, Partition=6, Replication Factor=3, `acks=all`).
   * **Kết quả**: Triệt tiêu hoàn toàn **0% query load** lên OLTP DB, bắt được trọn vẹn cả sự kiện `DELETE` và các trạng thái cập nhật trung gian.
2. **Xây dựng Data Lakehouse & Mô hình hóa Star Schema với SCD Type 2**:
   * *Streaming Ingestion*: Spark Structured Streaming đọc từ Kafka, ingest liên tục vào HDFS Data Lake dưới định dạng Parquet nén Snappy (phân vùng `year/month/day`).
   * *Batch Modeling*: Airflow điều phối Spark batch job tổng hợp dữ liệu thành Star Schema gồm 2 Fact Tables (`fact_repair_service_revenue`, `fact_parts_sales`) và 4 Dimension Tables.
   * *Xử lý chiều thay đổi chậm (SCD Type 2)*: Thiết kế logic Merge 5 bước cho `dim_customer` sử dụng Surrogate Key (MD5), trường `effective_date`, `expiration_date` và cờ `is_current` để bảo toàn 100% lịch sử giao dịch tại đúng thời điểm phát sinh.
3. **Serving Layer độ trễ cực thấp (Redis 7+ GEO & Lua Concurrency Control)**:
   * Thay vì để ứng dụng tìm kiếm trạm query vào PostgreSQL PostGIS (tốn 50-100ms disk I/O), dữ liệu trạm được sync sang Redis 7+ $\to$ Dùng lệnh **`GEOSEARCH`** tính toán khoảng cách tọa độ trong **$< 1\text{ms}$**.
   * Dùng **Redis Lua Script** đảm bảo tính nguyên tử (Atomicity) khi tài xế đặt lịch giữ chỗ (Slot Reservation), triệt tiêu hoàn toàn lỗi đặt trùng lịch (Overbooking).
   * Dùng **WebSocket Push Layer** trung gian giữa Redis và Dashboard: Dữ liệu được chủ động đẩy (push) xuống màn hình Ban Giám đốc trong $<15\text{ms}$ ngay khi Spark hoàn tất tính toán, loại bỏ hoàn toàn 360 request polling/phút.
4. **Hệ thống Giám sát & Quản trị Chất lượng Dữ liệu 5 Tầng (Data Quality Gates)**:
   * Sau sự cố replication slot bị treo, tự tay xây dựng 5 tầng kiểm soát tự động: (1) Check WAL Lag trên PostgreSQL $\to$ (2) Đo Kafka Consumer Lag $\to$ (3) Freshness Gate ($\le 4\text{h}$) $\to$ (4) Volume Anomaly Gate ($> 2\sigma$) $\to$ (5) Reconciliation Gate (đối soát tổng số dòng). Nếu có bất kỳ lỗi nào, Airflow tự ngắt pipeline và bắn cảnh báo ngay lập tức.

### 3. Outcome Đo Lường Được (Measurable DE Metrics):
* **Độ trễ báo cáo**: Giảm từ **T+30 ngày (Excel cuối tháng) xuống T+15 phút** (cập nhật tự động).
* **Tải trên OLTP Database**: Giảm **100% truy vấn phân tích**, DB vận hành mượt mà $24/7$.
* **Độ trễ tìm trạm gần nhất**: Giảm từ **$85\text{ms}$ (PostGIS) xuống $< 1\text{ms}$ (Redis GEO)**.
* **Tỉ lệ đặt trùng chỗ (Overbooking)**: Duy trì **0%** nhờ Redis Lua Script.
* **Thời gian phát hiện sự cố**: Giảm từ **2 ngày xuống $< 5$ phút** nhờ Data Quality Gate 5 tầng.

---

## 1.2 Dự Án 2: CarDamageSAGE — Multi-modal Data Ingestion, Vector Deduplication & Serving Pipeline

### 1. Bối cảnh & Điểm nghẽn dữ liệu (Problem & Data Bottlenecks):
* Hệ thống giám định tổn thất ô tô cần xử lý tập dữ liệu thô $15.600$ hình ảnh và metadata kỹ thuật từ nhiều nguồn phân tán.
* **Điểm nghẽn**: Dữ liệu thu thập bị trùng lặp, nhiễu nhãn sai lệch nghiêm trọng ($28.1\%$), định dạng JSON không đồng nhất. Chi phí thuê con người gán nhãn thủ công hàng chục ngàn mẫu VQA ước tính tốn hàng trăm triệu đồng, trong khi việc gọi API mô hình thương mại bên ngoài (GPT-4V) gây tốn kém chi phí định kỳ và vi phạm bảo mật dữ liệu khách hàng.

### 2. Ba đột phá kiến trúc Data Engineering của bản thân:
1. **Pipeline Làm sạch & Khử trùng lặp Dữ liệu bằng Vector Curation**:
   * Xây dựng pipeline tự động hóa trích xuất vector đặc trưng $768$ chiều bằng `CLIP-ViT-L/14`, lưu trữ và lập chỉ mục không gian vector.
   * Áp dụng thuật toán gom cụm $K\text{-Means}$ kết hợp đo độ lệch **Cosine Similarity** trên siêu cầu đơn vị $\mathbb{S}^{768}$ $\to$ Xây dựng **Data Quality Gate** tự động: Cô lập các ảnh có độ tương đồng $\text{sim} < 0.65$ và tự động gán lại nhãn đúng cho các ảnh có $\text{sim} > 0.85$.
   * **Kết quả**: Lọc bỏ chính xác **$2.841$ bản ghi rác ($28.1\%$)**, tạo ra tập dữ liệu sạch $12.759$ ảnh chuẩn hóa.
2. **ETL Synthesis Engine sinh 105.000 mẫu VQA theo Ontology**:
   * Thiết kế pipeline 3 giai đoạn tự động sinh $105.000$ cặp dữ liệu hỏi-đáp kỹ thuật (VQA), ép schema đầu ra tuân thủ nghiêm ngặt theo Ontology từ điển ngành xe (~50 cặp từ khóa phụ tùng/loại hư hại chuẩn ERP).
   * Tích hợp kỹ thuật sinh mẫu phủ định (Negative Sampling) để cân bằng phân phối dữ liệu, triệt tiêu hiện tượng đoán mò của hệ thống.
3. **Tối ưu hóa Hạ tầng Huấn luyện & Serving Pipeline Low-Latency**:
   * Áp dụng kỹ thuật **Dynamic Image Patching ($448 \times 448$)**: Giảm số lượng token thị giác từ $1.024 \to 256$ tokens/ảnh $\implies$ Cắt giảm **$27.3\times$ dung lượng bộ nhớ ma trận Self-Attention**, tăng tốc độ xử lý batch lên **$27\%$** ($15.3\text{s} \to 11.2\text{s/step}$).
   * Kết hợp lượng tử hóa 4-bit (QLoRA) đưa dung lượng VRAM phục vụ từ **$>18\text{GB} \to 6.2\text{GB}$ / GPU**, cho phép đóng gói toàn bộ pipeline thành **API FastAPI phục vụ offline với độ trễ $< 2$ giây/ảnh**.
4. **Hệ thống Kiểm định Dữ liệu Tự động (Negation-Aware Validation Engine)**:
   * Thiết kế bộ quy tắc chấm điểm tự động (Rubric Rule Engine) kết hợp giải thuật **quét cửa sổ ngữ cảnh phủ định (Lookback Context Window 18 ký tự)** để triệt tiêu lỗi phạt oan khi mô hình trả lời phủ định $\to$ Đưa độ chính xác kiểm định pipeline từ **$4.4\% \to 89.93\%$**.

### 3. Outcome Đo Lường Được (Measurable DE Metrics):
* **Chi phí gán nhãn**: Tiết kiệm **$100\%$ chi phí gán nhãn thủ công**, tự động sinh **$105.000$ bản ghi dữ liệu chuẩn**.
* **Tài nguyên phần cứng**: Giảm **$65.5\%$ bộ nhớ GPU** ($18\text{GB} \to 6.2\text{GB}$), chạy hoàn toàn offline trên GPU T4 phổ thông.
* **Độ trễ suy luận API**: Đạt **$< 2$ giây / yêu cầu giám định**.
* **Độ tin cậy dữ liệu**: Điểm chất lượng đầu ra đạt **$89.93\%$** trên tập kiểm thử độc lập 637 ảnh.

---

## 1.3 Dự Án 3: Student Behavioral Streaming Platform (Neo4j & Kafka)

### 1. Bối cảnh & Điểm nghẽn dữ liệu (Problem & Data Bottlenecks):
* Nền tảng học trực tuyến ghi nhận $500.000$ sự kiện tương tác mỗi ngày (xem bài, làm quiz, nộp bài tập).
* **Điểm nghẽn**: Bài toán cần phát hiện sinh viên có nguy cơ bỏ học thông qua chuỗi hành vi liên kết 5 tầng (`Student` $\to$ `Enrollment` $\to$ `Course` $\to$ `Module` $\to$ `Assignment` $\to$ `Submission`). Khi thực hiện 5 phép JOIN trên PostgreSQL, chỉ mục B-Tree bị quá tải, câu truy vấn mất **$> 12.5$ giây** làm tê liệt hệ thống báo cáo.

### 2. Đột phá kiến trúc Data Engineering:
* **Chuyển đổi sang Mô hình Labeled Property Graph (LPG) trên Neo4j**: Tận dụng cơ chế **Index-Free Adjacency** — mỗi Node lưu trực tiếp con trỏ bộ nhớ (Memory Pointer 34 bytes) tới các đỉnh lân cận $\to$ Thao tác duyệt quan hệ đa tầng là phép giải tham chiếu con trỏ trực tiếp trong RAM với chi phí **$O(1)$ per hop** (hoàn toàn không phụ thuộc vào tổng số hàng triệu dòng trong DB).
* **Kafka Streaming Ingestion**: Ingest luồng sự kiện thời gian thực từ Kafka vào Neo4j bằng connector chuyên dụng, cập nhật trạng thái đồ thị tức thì.

### 3. Outcome Đo Lường Được (Measurable DE Metrics):
* **Thời gian truy vấn đa tầng**: Giảm từ **$12.5\text{giây} \to < 8\text{mili-giây}$ (nhanh hơn $1500\times$)**.
* **Khả năng mở rộng**: Xử lý dòng stream **$500.000$ events/ngày** với độ trễ ingestion $< 1$ giây.
* **Hiệu quả nghiệp vụ**: Cảnh báo sớm sinh viên nguy cơ bỏ học ngay trong **tuần thứ 2** của khóa học.

---

# PHẦN 2: CHUYÊN ĐỀ ĐÀO SÂU POSTGRESQL CHO SENIOR DATA ENGINEER

---

### PG.1 Bản chất MVCC, Dead Tuples, Table Bloat & Chiến Lược Autovacuum Tuning

```
              CƠ CHẾ PHÁT SINH DEAD TUPLES TRONG POSTGRESQL MVCC
┌─────────────────────────────────────────────────────────────────────────────┐
│ 1. KHI CÓ LỆNH: UPDATE customers SET status = 'active' WHERE id = 1;        │
│    • PostgreSQL KHÔNG ghi đè lên dữ liệu cũ (Non-destructive update).       │
│    • Dòng cũ: Được đánh dấu xmax = Transaction_ID (Trở thành DEAD TUPLE).   │
│    • Dòng mới: Được INSERT vào cuối trang đĩa với xmin = Transaction_ID.    │
├─────────────────────────────────────────────────────────────────────────────┤
│ 2. HẬU QUẢ: TABLE BLOAT & INDEX BLOAT                                       │
│    • Bảng có 1 triệu dòng thực tế nhưng chiếm dung lượng của 5 triệu dòng   │
│      (4 triệu dòng rác Dead Tuples chưa được dọn).                          │
│    • Cây B-Tree Index chứa đầy con trỏ trỏ vào Dead Tuples → Phình to,      │
│      làm suy thoái nghiêm trọng hiệu năng bộ đệm RAM (shared_buffers).       │
└─────────────────────────────────────────────────────────────────────────────┘
```

#### 1. Cơ chế hoạt động của VACUUM:
* **Standard VACUUM**: Quét qua các trang đĩa, dọn dẹp các Dead Tuples và đánh dấu vùng trống vào **Free Space Map (FSM)** để các lệnh `INSERT/UPDATE` tương lai tái sử dụng. **Standard VACUUM KHÔNG trả lại dung lượng đĩa cho Hệ Điều Hành (OS)**.
* **VACUUM FULL**: Tạo một bản sao vật lý mới của bảng và viết lại toàn bộ dữ liệu sạch.
  * ⚠️ **Cực kỳ nguy hiểm trên Production**: Chiếm khóa độc quyền **`AccessExclusiveLock`**, chặn toàn bộ truy vấn ĐỌC và GHI, dễ gây sập hệ thống nếu bảng lớn.
  * **Giải pháp thay thế trên Production**: Dùng công cụ **`pg_repack`** để rebuild bảng và index online mà hoàn toàn không khóa bảng.

#### 2. Chiến lược Autovacuum Tuning cho bảng CDC / Staging có tần suất ghi cao:
Mặc định của PostgreSQL là `autovacuum_vacuum_scale_factor = 0.20` (bảng phải có $20\%$ dòng bị thay đổi thì Autovacuum mới chạy). Với bảng có $10$ triệu dòng, cần tới $2$ triệu dòng chết Autovacuum mới kích hoạt $\to$ Bảng bị phình to (Bloat) trước khi được dọn!

```sql
-- Cấu hình Autovacuum tích cực riêng cho bảng giao dịch CDC / Staging
ALTER TABLE invoices SET (
    autovacuum_vacuum_scale_factor = 0.05,     -- Giảm xuống 5% (500k dòng chết là dọn ngay)
    autovacuum_vacuum_threshold = 1000,        -- Ngưỡng tối thiểu 1000 dòng
    autovacuum_vacuum_cost_limit = 2000,       -- Tăng hạn mức I/O để worker dọn nhanh gấp 10 lần (mặc định 200)
    autovacuum_vacuum_cost_delay = 2           -- Giảm thời gian nghỉ giữa các đợt dọn (mặc định 2ms)
);
```

---

### PG.2 WAL (Write-Ahead Log), Replication Slots & Quản Trị Rủi Ro Đĩa Đầy

#### 1. Bản chất của WAL và Logical Replication:
* Mọi giao dịch `INSERT/UPDATE/DELETE` đều được ghi tuần tự vào bộ nhớ đệm WAL và flush xuống các file `pg_wal` (kích thước $16\text{MB}$/file) trước khi cập nhật lên Data Pages trên đĩa (đảm bảo tính bền vững Durability của ACID).
* **Debezium CDC** sử dụng cơ chế **Logical Decoding** qua plugin **`pgoutput`** để đọc các bản ghi WAL này và giải mã thành các sự kiện JSON/Avro.

#### 2. Hiểm họa Replication Slot & Câu lệnh Giám sát sống còn:
* **Cơ chế**: Replication Slot có nhiệm vụ "ghim giữ" con trỏ vị trí đọc (Log Sequence Number - `restart_lsn`). PostgreSQL đảm bảo **KHÔNG BAO GIỜ xóa các file WAL** mà Replication Slot chưa xác nhận đã đọc xong.
* **Thảm họa Production**: Nếu Debezium Connector bị crash hoặc network bị đứt trong 2 ngày, PostgreSQL tiếp tục giữ lại toàn bộ WAL $\to$ Thư mục `pg_wal` phình to chiếm $100\%$ dung lượng ổ cứng $\to$ **Toàn bộ máy chủ PostgreSQL bị crash sập**!

```sql
-- Query giám sát dung lượng WAL Lag của Replication Slot (Chạy định kỳ 1 phút/lần)
SELECT 
    slot_name,
    plugin,
    active,
    pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS replication_lag_bytes
FROM pg_replication_slots;

-- Thiết lập chốt an toàn trong postgresql.conf (PostgreSQL 13+)
-- Nếu Debezium chết làm lag vượt quá 10GB, tự động hủy giữ WAL để cứu sống DB!
max_slot_wal_keep_size = 10240MB;
```

#### 3. Tại sao bắt buộc phải dùng `REPLICA IDENTITY FULL` cho CDC?
* Mặc định (`REPLICA IDENTITY DEFAULT`), khi có lệnh `UPDATE`, PostgreSQL chỉ ghi giá trị Primary Key vào WAL cho phần Before-image.
* Khi cấu hình **`ALTER TABLE customers REPLICA IDENTITY FULL;`**, PostgreSQL sẽ ghi **toàn bộ giá trị của tất cả các cột trước và sau khi UPDATE** vào WAL $\implies$ Giúp Spark Batch Job so sánh được sự thay đổi của từng trường để xây dựng **SCD Type 2** trong Data Warehouse.

---

### PG.3 Quản Trị Bộ Nhớ (Memory Architecture) Cho Tác Vụ ETL & Batch Processing

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ CẤU TRÚC BỘ NHỚ POSTGRESQL (POSTGRESQL MEMORY ARCHITECTURE)                 │
│                                                                             │
│ ┌─────────────────────────────────────────────────────────────────────────┐ │
│ │ 1. SHARED MEMORY (Dùng chung cho toàn bộ các Connection):               │ │
│ │    • shared_buffers: Bộ đệm Data Pages trong RAM (Khuyên dùng 25% RAM). │ │
│ │    • wal_buffers: Bộ đệm ghi WAL log (Thường để 16MB - 64MB).           │ │
│ └─────────────────────────────────────────────────────────────────────────┘ │
│                                                                             │
│ ┌─────────────────────────────────────────────────────────────────────────┐ │
│ │ 2. LOCAL MEMORY (Cấp phát riêng cho TỪNG Process / Query):              │ │
│ │    • work_mem: Cấp phát cho MỖI phép toán SORT, HASH JOIN trong query!  │ │
│ │    • maintenance_work_mem: Dùng cho VACUUM, CREATE INDEX, ALTER TABLE.  │ │
│ └─────────────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────┘
```

#### 1. Bí quyết cấu hình `work_mem` chống tràn đĩa (Spill to Disk):
* **Cạm bẫy**: Nếu một câu query thực hiện 3 phép toán `ORDER BY` + 2 phép `HASH JOIN`, nó sẽ tiêu tốn **$5 \times \text{work\_mem}$**. Nếu có 100 kết nối đồng thời, tổng RAM tiêu thụ $= 500 \times \text{work\_mem} \to$ Dễ gây tràn RAM máy chủ (OS OOM Killer).
* **Chiến lược Senior**:
  * Đặt `work_mem` toàn cục ở mức an toàn: `work_mem = 16MB` (cho các truy vấn OLTP nhỏ).
  * Đối với các Session chạy ETL / Batch Data Pipeline nặng, chủ động nâng `work_mem` cục bộ trong phiên làm việc:
    ```sql
    SET work_mem = '256MB'; -- Chỉ áp dụng cho pipeline hiện tại, giải phóng ngay khi chạy xong
    SELECT customer_id, SUM(amount) FROM invoices GROUP BY customer_id ORDER BY SUM(amount) DESC;
    ```
    $\implies$ Giúp thuật toán Sort chạy $100\%$ trên RAM (`quicksort in memory`), triệt tiêu hoàn toàn hiện tượng tràn xuống đĩa (`external merge disk sort` làm chậm truy vấn gấp hàng trăm lần).

---

### PG.4 Masterclass Indexing: B-Tree, BRIN, GIN, Partial & Covering (`INCLUDE`)

```
┌───────────┬──────────────────────────────────┬─────────────────────────────────────────────────────────┐
│ Loại Index│ Trường Hợp Sử Dụng Tối Ưu        │ Cơ Chế & Lợi Thế Kỹ Thuật                              │
├───────────┼──────────────────────────────────┼─────────────────────────────────────────────────────────┤
│ B-Tree    │ `=`, `<`, `>`, `BETWEEN`, `SORT` │ Cây cân bằng chuẩn, tìm kiếm $O(\log N)$.               │
│ BRIN      │ Time-series, Append-only Logs    │ Lưu Min/Max cho từng dải block đĩa. Nhỏ hơn 95% B-Tree! │
│ GIN       │ JSONB, Mảng (Arrays), Full-Text  │ Chỉ mục đảo (Inverted Index), tối ưu toán tử `@>`, `?`.│
│ GiST      │ Tọa độ địa lý (PostGIS), Hình học│ Cây không gian (R-Tree), tối ưu tìm kiếm bán kính/vùng. │
└───────────┴──────────────────────────────────┴─────────────────────────────────────────────────────────┘
```

#### 1. Khi nào dùng BRIN Index thay cho B-Tree?
* Bảng `truck_telemetry` có 500 triệu dòng, dữ liệu ghi nối đuôi liên tục theo thời gian (`created_at`).
* Nếu dùng B-Tree Index: Dung lượng file index chiếm tới **$> 12\text{GB}$ RAM/Disk**.
* Dùng **BRIN Index (Block Range Index)**:
  ```sql
  CREATE INDEX idx_telemetry_brin ON truck_telemetry USING BRIN (created_at) WITH (pages_per_range = 128);
  ```
  $\implies$ Dung lượng index chỉ tốn **$\sim 15\text{MB}$ (giảm $99.8\%$)**, tốc độ quét khoảng ngày nhanh tương đương B-Tree vì dữ liệu vật lý trên đĩa đã được sắp xếp tự nhiên theo thời gian!

#### 2. Covering Index với mệnh đề `INCLUDE` (Đạt Index-Only Scan):
```sql
-- Tạo Covering Index
CREATE INDEX idx_invoices_covering 
ON invoices (customer_id, invoice_date) 
INCLUDE (total_amount, status);

-- Câu query được hưởng lợi tuyệt đối:
SELECT customer_id, invoice_date, total_amount, status
FROM invoices
WHERE customer_id = 1001 AND invoice_date >= '2026-01-01';
```
* **Bản chất kỹ thuật**: Các trường trong `INCLUDE` được lưu trực tiếp tại các nút lá (Leaf Nodes) của cây B-Tree nhưng không tham gia vào cấu trúc sắp xếp khóa. PostgreSQL lấy trọn vẹn dữ liệu ngay trên cây index mà không cần thực hiện thao tác **Table Heap Fetch (Heap Fetches: 0)** $\to$ Tối ưu $100\%$ Disk I/O!

#### 3. An toàn Migration với `CREATE INDEX CONCURRENTLY`:
* `CREATE INDEX` thông thường chiếm khóa `ShareLock`, chặn toàn bộ các thao tác `INSERT, UPDATE, DELETE` trên bảng cho đến khi đánh index xong (có thể mất vài tiếng trên bảng lớn).
* **Quy tắc bắt buộc trên Production**: Luôn dùng **`CREATE INDEX CONCURRENTLY`** $\to$ PostgreSQL thực hiện qua 2 lượt quét bảng mà chỉ lấy khóa nhẹ `ShareUpdateExclusiveLock`, cho phép ứng dụng tiếp tục đọc và ghi bình thường.

---

### PG.5 Đọc Hiểu Kế Hoạch Thực Thi EXPLAIN (ANALYZE, BUFFERS)

```
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF)
SELECT customer_id, SUM(total_amount) 
FROM invoices 
WHERE invoice_date >= '2026-06-01' AND status = 'paid' 
GROUP BY customer_id;
```

```
-> HashAggregate (actual time=12.150..14.200 rows=500 loops=1)
     Group Key: customer_id
     Batches: 1  Memory Usage: 120kB
     Buffers: shared hit=450 read=12
     -> Bitmap Heap Scan on invoices (actual time=1.120..8.450 rows=15000 loops=1)
          Recheck Cond: (status = 'paid' AND invoice_date >= '2026-06-01')
          Buffers: shared hit=420 read=12
          -> Bitmap Index Scan on idx_invoices_paid_date (actual time=0.850..0.850 rows=15000 loops=1)
               Index Cond: (invoice_date >= '2026-06-01')
               Buffers: shared hit=30
Total Runtime: 14.550 ms
```

#### Các chỉ số kỹ thuật then chốt cần phân tích:
1. **`Buffers: shared hit=450 read=12`**:
   * `shared hit = 450`: Đọc 450 trang đĩa trực tiếp từ RAM (`shared_buffers`) $\to$ Cực nhanh ($< 0.1\text{ms}$).
   * `read = 12`: Chỉ có 12 trang đĩa phải đọc từ ổ cứng vật lý (Disk I/O).
   * **Cache Hit Ratio** $= \frac{450}{450 + 12} = \mathbf{97.4\%}$ (Chỉ số lý tưởng phải $\ge 99\%$).
2. **Các bước quét dữ liệu (Scan Types theo thứ tự từ chậm đến nhanh)**:
   $$\text{Seq Scan (Chậm nhất)} \longrightarrow \text{Bitmap Index Scan} \longrightarrow \text{Index Scan} \longrightarrow \text{Index Only Scan (Nhanh nhất)}$$
3. **Các thuật toán JOIN (Join Types)**:
   * **Nested Loop Join**: Tối ưu khi bảng ngoài nhỏ và bảng trong có index trên join key.
   * **Hash Join**: Tối ưu khi bảng lớn chưa được sắp xếp (xây dựng Hash Table trong RAM bằng `work_mem`).
   * **Merge Join**: Tối ưu nhất cho 2 tập dữ liệu khổng lồ đã được sắp xếp sẵn theo join key.

---

### PG.6 Declarative Partitioning & Cơ Chế Partition Pruning

```sql
-- Tạo bảng cha phân vùng theo khoảng thời gian (Range Partitioning)
CREATE TABLE fact_invoices (
    id BIGINT NOT NULL,
    invoice_date DATE NOT NULL,
    customer_id INT NOT NULL,
    total_amount NUMERIC(15, 2)
) PARTITION BY RANGE (invoice_date);

-- Tạo các bảng con theo từng năm tài khóa
CREATE TABLE fact_invoices_fy2025 PARTITION OF fact_invoices
    FOR VALUES FROM ('2024-10-01') TO ('2025-10-01');

CREATE TABLE fact_invoices_fy2026 PARTITION OF fact_invoices
    FOR VALUES FROM ('2025-10-01') TO ('2026-10-01');
```

* **Cơ chế Partition Pruning (`enable_partition_pruning = on`)**:
  * Khi người dùng chạy: `SELECT * FROM fact_invoices WHERE invoice_date = '2026-05-15';`
  * Query Planner tự động loại bỏ hoàn toàn bảng `fact_invoices_fy2025` ngay trong giai đoạn lập kế hoạch, chỉ quét duy nhất bảng phân vùng `fy2026` $\implies$ Giảm $50\% - 90\%$ khối lượng quét dữ liệu vật lý.

---

# PHẦN 3: BỘ 13 CÂU HỎI VÒNG 1 ĐÃ TINH LỌC — ĐÁP ÁN CHUẨN SENIOR DE (60-90 GIÂY/CÂU)

---

### Q1: Một Data Engineer level Middle cần nắm được những gì trong roadmap để được đánh giá là đạt?

**Đáp án phản xạ (60 giây):**
> *"Một Middle Data Engineer đạt chuẩn sản xuất cần làm chủ **4 khối năng lực kỹ thuật cốt lõi**:*
>
> 1. ***Data Modeling & Storage Engine***: Nắm chắc 3NF cho OLTP, Dimensional Modeling (Star Schema / SCD2) cho OLAP, hiểu sâu cơ chế lưu trữ phân tán (Parquet Columnar, Row Groups, Snappy, HDFS Block Size).
> 2. ***Distributed Processing & Performance Tuning***: Viết Spark thành thạo, hiểu sâu cơ chế Shuffle, Broadcast Join, quản lý Memory Overhead, và xử lý dứt điểm Data Skew bằng kỹ thuật Salting.
> 3. ***Stream Processing & CDC Pipeline***: Làm chủ Log-based CDC (PostgreSQL WAL qua Debezium), cơ chế phân vùng Kafka, quản trị Consumer Group Lag, và đảm bảo tính Idempotent khi nạp dữ liệu.
> 4. ***Production Reliability & Data Governance***: Xây dựng Data Quality Gates tự động (Freshness, Volume, Schema Drift), cấu hình Airflow DAGs có khả năng tự phục hồi lỗi, và tối ưu hóa chuyên sâu cơ sở dữ liệu (PostgreSQL Indexing, MVCC Autovacuum)."*

---

### Q2: Hiểu thế nào về Data Modeling?

**Đáp án phản xạ (60 giây):**
> *"Data Modeling là nghệ thuật thiết kế cấu trúc dữ liệu tối ưu hóa theo mục đích sử dụng:
>
> * **Ở tầng OLTP**: Sử dụng **Chuẩn hóa 3NF** để loại bỏ hoàn toàn dữ liệu dư thừa, bảo vệ tính toàn vẹn giao dịch ACID và tối ưu hóa tốc độ ghi đơn dòng.
> * **Ở tầng OLAP / Data Warehouse**: Sử dụng **Mô hình Chiều (Star Schema / Kimball)**. Ta chủ động phi chuẩn hóa (Denormalize) các bảng Dimension để đưa các truy vấn phân tích về dạng **1-hop join** giữa Fact và Dim, giảm thiểu tối đa chi phí Shuffle Join trên hệ thống tính toán phân tán.
> * **Trong dự án Fleet**: Em thiết kế Star Schema gồm 2 Fact Tables (`fact_repair_service_revenue`, `fact_parts_sales`) liên kết với các Conformed Dimensions, trong đó `dim_customer` áp dụng **SCD Type 2** với Surrogate Key MD5 để lưu vết lịch sử thay đổi hạng khách hàng mà không làm sai lệch báo cáo doanh thu tài khóa cũ."*

---

### Q3: DATE, DATETIME, TIMESTAMP khác gì nhau? Cơ chế Indexing dữ liệu thời gian?

**Đáp án phản xạ (75 giây):**
> *"Về bản chất lưu trữ:
> * **DATE (4 Bytes)**: Chỉ lưu ngày tháng năm (`YYYY-MM-DD`).
> * **DATETIME (8 Bytes)**: Lưu ngày giờ nguyên bản theo đồng hồ nhập liệu (Raw Clock), hoàn toàn không gắn liền với múi giờ.
> * **TIMESTAMP (4 hoặc 8 Bytes - `TIMESTAMPTZ` trong PostgreSQL)**: Tự động chuyển đổi và lưu trữ dưới chuẩn **UTC** trong database, khi client truy vấn sẽ tự động convert sang múi giờ của session client.
>
> *Về cơ chế Indexing dữ liệu thời gian:*
> 1. **Cạm bẫy phá vỡ B-Tree Index**: Tránh tuyệt đối việc bọc hàm `WHERE DATE(created_at) = '2026-08-16'` vì làm mất tính SARGable, ép DB chạy Full Table Scan (Seq Scan). Phải luôn viết theo khoảng chặn: `WHERE created_at >= '2026-08-16 00:00:00' AND created_at < '2026-08-17 00:00:00'`.
> 2. **BRIN Index cho Time-series khổng lồ**: Với bảng cảm biến/telemetry chỉ ghi nối đuôi (Append-only), em sử dụng **BRIN Index** (Block Range Index). BRIN chỉ lưu giá trị Min/Max cho từng dải block đĩa $\to$ Giúp kích thước index **nhỏ hơn 95% so với B-Tree** và tốc độ quét range cực nhanh."*

---

### Q4: Các phương pháp tối ưu hóa câu truy vấn SQL & Spark SQL?

**Đáp án phản xạ (75 giây):**
> *"Em tối ưu hóa truy vấn dựa trên kế hoạch thực thi (**EXPLAIN ANALYZE**) qua 4 bước:
>
> 1. ***SARGable Predicates & Partition Pruning***: Đảm bảo điều kiện lọc đánh đúng vào cột Index và Partition Key, tránh ép kiểu ngầm định (`CAST`).
> 2. ***Chỉ mục chuyên biệt trong PostgreSQL***:
>    * **Covering Index (`INCLUDE`)**: Đưa các trường cần `SELECT` vào mệnh đề `INCLUDE` để đạt **Index-Only Scan (Heap Fetches: 0)**, không cần chạm vào bảng dữ liệu gốc.
>    * **Partial Index**: Chỉ index các dòng có `status = 'paid'`, giảm $50\%$ kích thước file index.
> 3. ***Tối ưu phép JOIN***: Thay thế subquery lồng nhau bằng CTE và Window Functions; trong Spark sử dụng **Broadcast Hash Join** cho bảng nhỏ ($< 10\text{MB}$) để loại bỏ Shuffle Network I/O.
> 4. ***Xử lý Skewed Data trong Spark***: Áp dụng kỹ thuật **Salting** (thêm prefix ngẫu nhiên $0-9$ vào key) để chia nhỏ partition bị lệch, đưa thời gian chạy từ **$30\text{ phút} \to 1.5\text{ phút}$**."*

---

### Q5: Những cấu trúc dữ liệu đã ứng dụng trong thực tế dự án DE?

**Đáp án phản xạ (75 giây):**
> *"Mỗi cấu trúc dữ liệu em chọn đều gắn liền với bài toán tối ưu độ phức tạp và bộ nhớ:
>
> 1. ***HashMap / HashTable ($O(1)$)***: Dùng làm In-memory Lookup Table cho Broadcast Join và logic matching trạng thái.
> 2. ***Min-Heap / PriorityQueue ($O(n \log k)$)***: Dùng tính **Top-K doanh thu khách hàng** từ dòng dữ liệu streaming, chỉ tốn $O(k)$ bộ nhớ RAM thay vì sort toàn bộ tập dữ liệu $O(n \log n)$.
> 3. ***Doubly Linked List + HashMap***: Cấu trúc lõi của **LRU Cache** trong cơ chế giải phóng bộ nhớ của Redis Serving Layer.
> 4. ***Adjacency List & Memory Pointers***: Mô hình hóa đồ thị trong Neo4j để đạt **$O(1)$ bước nhảy quan hệ (Index-Free Adjacency)**.
> 5. ***Bloom Filter (BitSet)***: Kiểm tra nhanh `message_id` trùng lặp trong Kafka Stream với $O(k)$ hàm băm, tiết kiệm $90\%$ RAM so với HashSet.
> 6. ***HyperLogLog (HLL)***: Ứng dụng Redis `PFADD/PFCOUNT` chỉ tốn **$12\text{KB}$ RAM** để ước lượng số lượng $100$ triệu người dùng duy nhất với sai số $\le 0.81\%$."*

---

### Q6: Tại sao sử dụng HDFS? Framework tương tự và Trade-offs?

**Đáp án phản xạ (75 giây):**
> *"Em chọn HDFS cho dự án Fleet vì 2 lý do:
>
> 1. ***Data Locality trên hạ tầng Bare-Metal***: Spark Executor đọc trực tiếp các block Parquet từ ổ cứng DataNode cục bộ qua mạng LAN nội bộ, tối ưu hóa băng thông cho batch job tổng hợp ban đêm.
> 2. ***Tích hợp chuẩn mực với YARN & Spark***: Hỗ trợ sẵn cơ chế chia Block ($256\text{MB}$), Speculative Execution và InputFormat tối ưu.
>
> * **Giải pháp tương đương**: AWS S3, Google GCS, Azure ADLS Gen2 (Cloud) và MinIO, Ceph (On-premise).
> * **Trade-off cốt lõi**:
>   * *HDFS*: Ghép chung Compute và Storage $\to$ Tối ưu I/O đọc cục bộ nhưng mở rộng kém linh hoạt (muốn tăng ổ cứng phải mua thêm cả server CPU/RAM).
>   * *S3 / MinIO*: Tách rời hoàn toàn Compute và Storage (Decoupled) $\to$ Mở rộng độc lập, tối ưu chi phí nhưng phải đánh đổi bằng độ trễ truyền dữ liệu qua mạng.
>   * *Nếu chuyển Cloud*: Em sẽ chuyển sang kiến trúc **S3 + Spark on Kubernetes (hoặc EMR/Databricks)**."*

---

### Q7: Ứng dụng thống kê & khai phá dữ liệu để tối ưu mở rộng hệ thống?

**Đáp án phản xạ (90 giây):**
> *"Em đã vận dụng thống kê và khai phá dữ liệu vào 4 bài toán tối ưu production:
>
> 1. ***Phát hiện bất thường cảm biến (Sliding Window Z-Score & DBSCAN)***: Dùng cửa sổ trượt để thích nghi với Concept Drift theo mùa; dùng DBSCAN để gom cụm mật độ và cô lập các điểm nhiễu (Noise Points) cảnh báo xe sắp hỏng máy.
> 2. ***Khai phá luật kết hợp (Association Rules - Apriori/Lift)***: Phân tích giỏ hóa đơn, tìm ra luật có $\text{Lift} = 8.0$ và $\chi^2 > 3.841$ giữa *Láng đĩa phanh* và *Thay má phanh* $\to$ **Tăng $22\%$ doanh thu bán chéo phụ tùng**.
> 3. ***Lọc rác dữ liệu đa phương thức (Vector Curation & Cosine Similarity)***: Áp dụng Cosine Quality Gate trên vector $768$ chiều để loại bỏ tự động **$28.1\%$ ảnh sai nhãn** trong CarDamageSAGE.
> 4. ***Tối ưu phân loại theo ma trận chi phí (Cost-Sensitive Bayes Thresholding)***: Vì chi phí xe gãy trục trên đèo ($50\text{ triệu}$) gấp $500\times$ chi phí kiểm tra nhầm ($100\text{k}$), em chủ động dịch chuyển ngưỡng xác suất $p^* \approx 0.002$ để **tối đa hóa Recall $\ge 98\%$**."*

---

### Q8: Phân biệt bản chất OLTP và OLAP?

**Đáp án phản xạ (60 giây):**
> *"Điểm khác biệt cốt lõi nằm ở **Mục đích thiết kế và Kiểu truy cập I/O**:
>
> * **OLTP (Online Transaction Processing)**:
>   * *Mục tiêu*: Phục vụ vận hành giao dịch hàng ngày ($100\%$ tuân thủ ACID).
>   * *Mô hình & Lưu trữ*: Chuẩn hóa 3NF, lưu trữ theo dòng (Row-Store: PostgreSQL, MySQL).
>   * *Truy vấn*: Thao tác `INSERT, UPDATE, DELETE` đơn dòng với độ trễ mili-giây ($< 10\text{ms}$).
> * **OLAP (Online Analytical Processing)**:
>   * *Mục tiêu*: Phục vụ phân tích, báo cáo tổng hợp và ra quyết định kinh doanh.
>   * *Mô hình & Lưu trữ*: Phi chuẩn hóa (Star Schema, Snowflake), lưu trữ theo cột (Columnar: Parquet, ClickHouse, BigQuery).
>   * *Truy vấn*: Quét hàng triệu dòng với các phép tính toán tổng hợp lớn (`SUM, AVG, GROUP BY`).
>
> *Trong dự án Fleet: PostgreSQL Odoo là hệ thống **OLTP** nguồn; Debezium CDC đồng bộ sang Data Lake HDFS và Star Schema DWH phục vụ **OLAP** trên PowerBI mà không làm suy giảm hiệu năng DB vận hành."*

---

### Q9: File Format Parquet trong Spark: Tiêu chuẩn dung lượng và số dòng tối ưu?

**Đáp án phản xạ (75 giây):**
> *"Parquet là định dạng lưu trữ theo cột (**Columnar Storage**) tối ưu cho Big Data nhờ 3 cơ chế:
> 1. **Nén theo kiểu dữ liệu (Dictionary Encoding & Snappy)**: Giảm dung lượng $5-10\times$ so với CSV/JSON.
> 2. **Column Pruning & Predicate Pushdown**: Chỉ đọc đúng cột cần query và dùng Metadata Min/Max ở Footer của Row Group để bỏ qua hàng triệu dòng không thỏa mãn `WHERE`.
>
> * **Kích thước file Parquet chuẩn trong Production**: Nên duy trì từ **$128\text{MB}$ đến $512\text{MB}$ / file** (khớp hoàn hảo với HDFS Block Size $128\text{MB}/256\text{MB}$).
> * **Số dòng tương ứng cho 1 file $128\text{MB}$**:
>   * *Bảng hẹp (Cảm biến Telemetry, 10 cột số)*: Chứa được **$\sim 10$ triệu dòng**.
>   * *Bảng trung bình (Fact DWH, 25 cột)*: Chứa được **$\sim 1.5$ triệu dòng**.
>   * *Bảng rộng (Customer Profile, 50 cột text)*: Chứa được **$\sim 300.000$ dòng**.
> * **Xử lý Small File Problem**: Trong pipeline Fleet Streaming, em chạy Airflow nightly job dùng `coalesce(4)` để gộp hàng ngàn file nhỏ thành các file chuẩn $128\text{MB}-256\text{MB}$, giải phóng $98\%$ áp lực RAM trên NameNode."*

---

### Q10: Cơ chế Spark Persist trên Disk hoạt động như thế nào?

**Đáp án phản xạ (75 giây):**
> *"Khi gọi `df.persist(StorageLevel.MEMORY_AND_DISK_SER)`:
> 1. **Tầng Bộ nhớ (Memory Storage)**: Dữ liệu Partition được nạp vào vùng `Storage Memory` của JVM dưới dạng mảng byte đã serialize (tiết kiệm $50\%$ RAM so với Java Object thô).
> 2. **Cơ chế Tràn Đĩa (Spill to Disk)**:
>    * Khi vùng RAM bị đầy và có Partition mới cần lưu, module **`BlockManager`** của Spark áp dụng giải thuật **LRU (Least Recently Used)** để đẩy các block ít dùng nhất xuống đĩa cục bộ (`spark.local.dir`).
>    * Dữ liệu được ghi tuần tự dưới dạng binary file kèm theo file index phân đoạn.
> 3. **Khi truy xuất lại**: Nếu block nằm trên RAM $\to$ Đọc tức thì; nếu block đã bị đẩy xuống Disk $\to$ Spark đọc từ đĩa cục bộ lên mà **không cần tính toán lại toàn bộ cây DAG (Lineage Graph)**."*

---

### Q11: Hiểu và áp dụng Data Governance trong Data Platform?

**Đáp án phản xạ (75 giây):**
> *"Data Governance là bộ khung quản trị đảm bảo dữ liệu **Đúng - Đủ - Kịp Thời - An Toàn**. Em triển khai qua 4 trụ cột:
>
> 1. ***Data Quality Gates (5 Tầng kiểm soát tự động)***: Kiểm tra trạng thái Replication Slot $\to$ Đo Kafka Consumer Lag $\to$ Kiểm tra độ tươi dữ liệu (Freshness $< 4\text{h}$) $\to$ Bắt dị biệt khối lượng (Volume Anomaly $> 2\sigma$) $\to$ Đối soát tổng số dòng (Row Reconciliation). Nếu fail, Airflow tự ngắt pipeline không cho đẩy số liệu lỗi xuống BI.
> 2. ***Data Lineage (Truy vết nguồn gốc)***: Lưu vết toàn bộ dòng chảy dữ liệu từ WAL PostgreSQL $\to$ Topic Kafka $\to$ Spark Batch Job $\to$ Bảng Fact/Dim DWH.
> 3. ***Quản trị Schema Evolution***: Dùng Schema Registry để kiểm soát tính tương thích ngược (Backward Compatibility) khi source Odoo thêm/sửa cột.
> 4. ***Bảo mật & Phân quyền (RBAC & PII Masking)***: Che mờ (Masking) số điện thoại, thông tin định danh cá nhân của tài xế trước khi đưa vào Data Lake và Vector Database."*

---

### Q12: Các quy tắc vàng khi thiết kế và tối ưu Index dữ liệu?

**Đáp án phản xạ (75 giây):**
> *"Có 5 quy tắc vàng em luôn tuân thủ khi thiết kế Index trong cơ sở dữ liệu:
>
> 1. ***Quy tắc Cardinality / Độ chọn lọc (Selectivity)***: Cột có độ phân tán cao (nhiều giá trị phân biệt như `customer_id`, `invoice_id`) luôn đặt ở vị trí đầu tiên trong Composite Index.
> 2. ***Quy tắc Tiền tố bên trái (Leftmost Prefix Rule)***: Composite Index `(A, B, C)` chỉ phục vụ tốt cho các truy vấn lọc theo `(A)`, `(A, B)`, `(A, B, C)`; truy vấn chỉ lọc theo `(B, C)` sẽ không tận dụng được index.
> 3. ***Covering Index với mệnh đề `INCLUDE`***: Tạo Index `ON invoices(invoice_date) INCLUDE (total_amount, customer_id)` giúp câu truy vấn lấy đủ dữ liệu ngay trên cây B-Tree mà không cần tốn Disk I/O vào bảng dữ liệu gốc (Index-Only Scan).
> 4. ***Partial Index cho dữ liệu phân bố lệch***: Bảng có 1 triệu đơn hàng nhưng chỉ $5\%$ là trạng thái `pending` $\to$ Tạo index `WHERE status = 'pending'`, giảm $95\%$ kích thước index.
> 5. ***Quy tắc Đánh đổi Chi phí Ghi (Write Overhead)***: Mỗi Index làm tăng thời gian `INSERT/UPDATE/DELETE` vì DB phải cập nhật cây B-Tree $\to$ Chỉ đánh index cho các cột thực sự nằm trong điều kiện lọc và JOIN thường xuyên."*

---

### Q13: Ý nghĩa bản chất của Q1, Q2, Q3, Median, Phương sai, Độ lệch chuẩn trong báo cáo dữ liệu?

**Đáp án phản xạ (90 giây):**
> *"Trong Khai phá và Báo cáo Dữ liệu, các con số thống kê mô tả phản ánh **hình dạng phân phối (Distribution Shape) và mức độ rủi ro** của hệ thống:
>
> 1. ***Tóm tắt 5 con số (Boxplot 5-Number Summary: Min, Q1, Q2/Median, Q3, Max)***:
>    * **$Q1$ (25th percentile)** và **$Q3$ (75th percentile)** bao bọc vùng **$IQR = Q3 - Q1$ (Khoảng trải giữa)** — nơi chứa $50\%$ dữ liệu cốt lõi nhất, không bị ảnh hưởng bởi các giá trị cực trị.
>    * **Ngưỡng tìm Outlier khách quan**: Điểm bất thường được định nghĩa nằm ngoài khoảng $[Q1 - 1.5 \times IQR, \; Q3 + 1.5 \times IQR]$.
> 2. ***Tại sao Median (Trung vị - $Q2$) trung thực hơn Mean (Trung bình)?***:
>    * Mean cực kỳ nhạy cảm với Outliers (Nếu 1 xe tải có hóa đơn sửa chữa $1\text{ tỷ}$ lọt vào nhóm $99$ xe sửa $1\text{ triệu}$, Mean vọt lên $11\text{ triệu}$ làm sai lệch toàn bộ kế hoạch dự phòng vốn). Trong khi đó, **Median giữ nguyên ở mức $1\text{ triệu}$**, phản ánh chính xác hành vi của đa số khách hàng.
>    * So sánh Mean và Median cho biết **Độ lệch phân phối (Skewness)**: Nếu $\text{Mean} > \text{Median} \implies$ Phân phối lệch phải (Right-skewed, có một số giao dịch giá trị siêu lớn).
> 3. ***Phương sai ($\sigma^2$) và Độ lệch chuẩn ($\sigma$)***:
>    * Thể hiện **Mức độ phân tán / Rủi ro biến động** quanh giá trị trung bình. Trong giám sát SLA độ trễ của Data Pipeline: Nếu độ trễ trung bình là $100\text{ms}$ nhưng $\sigma = 500\text{ms}$, hệ thống đang cực kỳ bất ổn định với nhiều độ trễ p99 nguy hiểm."*

---

# PHẦN 4: HAI KỊCH BẢN ĐỘT PHÁ DỰ PHÒNG CHO VÒNG 2

---

### 💥 Kịch bản 1: "Nếu gặp Data Skew trong Spark thì em xử lý triệt để thế nào?"

**Đáp án chuẩn Senior (60 giây):**
> *"Trong thực tế tổng hợp doanh thu theo khách hàng, khách hàng doanh nghiệp lớn chiếm $40\%$ lượng hóa đơn $\to$ Khi `groupBy("customer_id")`, 1 Task Spark bị dồn $2$ triệu dòng và chạy mất $30$ phút trong khi 99 Task khác xong trong $1$ phút.
> Em xử lý bằng 2 phương pháp:
> 1. ***Kỹ thuật Salting (Tối ưu thủ công 2 giai đoạn)***:
>    * *Stage 1*: Thêm số ngẫu nhiên từ $0-9$ vào key: `concat(customer_id, "_", rand()*10)`. Phân tán $2$ triệu dòng đều sang 10 Partitions $\to$ Chạy Partial Aggregation trong $1$ phút.
>    * *Stage 2*: Cắt bỏ đuôi số ngẫu nhiên và Aggregate lần cuối trên kết quả nhỏ $\to$ Tổng thời gian chạy giảm từ **$30\text{ phút} \to 1.5\text{ phút}$ (nhanh hơn $20\times$)**.
> 2. ***Bật Adaptive Query Execution (AQE Skew Join) trong Spark 3.0+***:
>    `spark.sql.adaptive.skewJoin.enabled = true`, Spark sẽ tự động phát hiện Partition vượt ngưỡng $5\times$ trung vị và tự chia nhỏ Partition khi JOIN."*

---

### 💥 Kịch bản 2: "Kể về sự cố Production nghiêm trọng nhất em từng gặp và bài học RCA?"

**Đáp án chuẩn STAR Story (90 giây):**
> *"Sự cố đáng nhớ nhất của em là **Sự cố Mất kết nối CDC ngầm 2 ngày**:
>
> * **Bối cảnh**: Slot replication của Debezium trên PostgreSQL bị treo do lỗi network, nhưng hệ thống không có cảnh báo nào bắn ra. Dashboard PowerBI vẫn hiển thị số liệu bình thường nhưng thực chất là số liệu cũ của 2 ngày trước.
> * **Hành động khắc phục ngay**: Em lập tức restart connector, khôi phục offset từ Kafka và chạy bù dữ liệu (Backfill) thành công nhờ cơ chế lưu trữ 7 ngày của Kafka.
> * **Bài học & Giải pháp vĩnh viễn (Root-Cause Fix)**:
>   Sau sự cố đó, em đã tự tay xây dựng **Hệ thống Data Quality Gate 5 tầng tích hợp vào Airflow**:
>   1. Query trực tiếp `pg_replication_slots` kiểm tra xem WAL Lag có vượt quá $5\text{GB}$ hay không.
>   2. Bắn heartbeat message định kỳ $30\text{s}$ trên Debezium.
>   3. Tự động kiểm tra độ tươi dữ liệu (`MAX(loaded_at)`).
> * **Kết quả**: Từ chỗ mất 2 ngày mới phát hiện sự cố, hệ thống hiện tại **bắn cảnh báo Telegram/Slack trong vòng dưới 5 phút**, đảm bảo độ tin cậy $100\%$ cho Ban Giám đốc."*

---

## 🎯 2 CÂU HỎI CHIẾN LƯỢC DÀNH CHO BẠN HỎI NGƯỢC NHÀ TUYỂN DỤNG Ở PHÚT 28

1. *"Trong lộ trình 1-2 năm tới, công ty mình đang ưu tiên hiện đại hóa Data Platform theo hướng **Real-time Streaming Lakehouse (như Apache Iceberg / Delta Lake)** hay tối ưu hóa sâu hạ tầng on-premise/hybrid hiện tại?"*
2. *"Với quy mô dữ liệu và khối lượng giao dịch hiện tại của team, thách thức kỹ thuật lớn nhất mà team đang phải giải quyết ở tầng Data Quality và PostgreSQL Performance Tuning là gì?"*
