# P1 — HIỂU TƯỜNG TẬN DỰ ÁN CÁ NHÂN (Trụ cột 1 — Ngày 1-2)
*Khung 5 lớp (Bối cảnh → Why → Output → Outcome → Impact) cho 5 dự án trong CV. Mỗi dự án kể được trong 60-90 giây.*

---

## DỰ ÁN 1: FLEET MAINTENANCE & REPAIR DATA PLATFORM ⭐ (Dự án Flagship)

### Lớp 1 — Bối cảnh & Bài toán gốc
Công ty vận hành chuỗi trạm sửa chữa/bảo dưỡng xe tải trên cả nước (~50 trạm). Hệ thống OLTP chạy trên Odoo ERP (PostgreSQL). Ban Giám đốc cần:
- Dashboard doanh thu real-time (không phải báo cáo Excel cuối tháng)
- Phân tích doanh thu theo **Năm Tài Khóa** (01/10 - 30/09), theo trạm, theo loại dịch vụ
- Tracking lịch sử thay đổi trạng thái khách hàng (mới → cũ → thường niên) để phân tích hiệu quả chương trình ưu đãi
- Tìm trạm gần nhất cho tài xế với độ trễ < 1 giây

### Lớp 2 — Quyết định thiết kế & Lý do (Why)
| Quyết định | Đã chọn | Đã cân nhắc & loại bỏ | Lý do chọn |
|---|---|---|---|
| Cách lấy dữ liệu từ OLTP | **Debezium Log-Based CDC** | Query Polling (SELECT WHERE updated_at >) | CDC: 0% query load lên Odoo DB, bắt DELETE events, bắt trạng thái trung gian. Polling: tạo tải I/O, mất DELETE, chỉ thấy snapshot cuối. |
| Message broker | **Kafka (3 brokers, RF=3)** | RabbitMQ | Kafka: log-based → replay được (idempotent), throughput 1M msg/s. RabbitMQ: message queue truyền thống, không replay. |
| Stream processing | **Spark Structured Streaming** | Apache Flink | Spark: unified batch+streaming API, hệ sinh thái mature, team đã quen. Flink: true streaming nhưng learning curve cao hơn, Fleet không cần latency < 100ms. |
| Data Lake storage | **HDFS Parquet** | S3/MinIO | HDFS: data locality khi Spark batch chạy lại lịch sử, bare-metal cluster sẵn có. S3: tách storage/compute nhưng cần network bandwidth. |
| DWH schema | **Star Schema** | Snowflake Schema, 3NF | Star: ít JOIN nhất (1 hop Fact→Dim), tối ưu Spark SQL. Snowflake: nhiều JOIN chain → shuffle overhead trên Big Data. |
| Serving layer | **Redis 7+ (GEOSEARCH)** | PostgreSQL PostGIS | Redis: < 1ms in-memory. PostGIS: 20-100ms (disk I/O). Lua Script single-threaded = 100% atomic slot reservation. |
| Dashboard push | **WebSocket** | REST polling | WebSocket: 0 requests/phút, push khi có data mới. REST: 360 requests/phút cho 30 users, query DB liên tục dù data chưa đổi. |
| Customer history | **SCD Type 2** | SCD Type 1 (overwrite) | SCD2: giữ nguyên lịch sử trạng thái tại thời điểm hóa đơn. SCD1: mất hết lịch sử → báo cáo sai. |

### Lớp 3 — Output (Đã xây được gì)
- E2E pipeline: PostgreSQL → Debezium → Kafka (3 brokers) → Spark Streaming → HDFS Data Lake → Spark Batch (Airflow) → HDFS DWH (Star Schema: 2 Fact + 4 Dim) → Redis → WebSocket Dashboard
- SCD Type 2 cho dim_customer với surrogate key MD5
- Data Quality Gate 5 tầng (replication slot → Kafka lag → freshness → volume → reconciliation)
- Fiscal year dim_date pre-computed (Q1=Oct-Dec, Q2=Jan-Mar, Q3=Apr-Jun, Q4=Jul-Sep)
- Redis Lua Script atomic slot reservation (zero overbooking)

### Lớp 4 — Outcome (Con số đo lường)
| Metric | Trước | Sau |
|---|---|---|
| Query load lên OLTP DB | 100% (polling) | **0%** (CDC đọc WAL) |
| Dashboard latency | N/A (Excel cuối tháng) | **< 15ms** (WebSocket push) |
| SCD2 accuracy | N/A | **100%** chính xác trạng thái tại thời điểm hóa đơn |
| Tìm trạm gần nhất | 50-100ms (PostGIS) | **< 1ms** (Redis GEOSEARCH) |
| Overbooking rate | Có thể xảy ra | **0%** (Lua atomic) |

### Lớp 5 — Impact ("Vậy thì sao")
- Ban GĐ ra quyết định kinh doanh dựa trên **dữ liệu real-time** thay vì đợi báo cáo Excel cuối tháng
- Phân tích được hiệu quả chương trình ưu đãi khách mới (conversion rate mới → thường niên)
- Tài xế tìm trạm sửa chữa nhanh hơn → tăng tỷ lệ chuyển đổi dịch vụ
- OLTP DB (Odoo) không còn bị ảnh hưởng bởi reporting queries → nhân viên thao tác mượt hơn

### 🔥 3 Câu hỏi vặn + Đáp án chuẩn bị sẵn

**Q1: "Nếu làm lại, bạn sẽ thay đổi gì?"**
> *"Em sẽ thêm Schema Registry (Confluent hoặc Apicurio) để quản lý schema evolution khi Odoo thêm/đổi cột. Hiện tại schema được hard-code trong Spark StructType — nếu Odoo thêm cột mới mà không báo, Spark job sẽ lỗi. Schema Registry cho phép backward/forward compatibility check tự động."*

**Q2: "Phần nào của hệ thống bạn không tự tin nhất?"**
> *"Phần HA (High Availability) cho HDFS NameNode. Hiện tại cluster chạy Single NameNode — nếu NameNode chết, toàn bộ HDFS unavailable. Production cần Active/Standby NameNode với JournalNodes. Em đã thiết kế nhưng chưa triển khai do hạn chế tài nguyên bare-metal."*

**Q3: "Nếu dữ liệu tăng 10 lần, phần nào sẽ vỡ trước tiên?"**
> *"HDFS NameNode metadata sẽ là bottleneck đầu tiên. Mỗi file Parquet nhỏ tiêu thụ ~150 bytes RAM trên NameNode. Nếu Spark Streaming trigger mỗi 10 giây mà không compaction → 8,640 files/ngày → NameNode heap exhaustion. Giải pháp: tăng trigger interval + Airflow nightly compaction job (đã implement bằng `coalesce(4)`)."*

---

## DỰ ÁN 2: RAG/LLM CHAT (Hybrid Retrieval + Ray Core)

### Lớp 1 — Bối cảnh
Xây dựng chatbot hỏi đáp nội bộ cho tài liệu kỹ thuật công ty. Nhân viên cần tra cứu nhanh thông tin từ hàng nghìn trang tài liệu kỹ thuật (specs, manuals, SOPs) mà không phải đọc toàn bộ.

### Lớp 2 — Why
| Quyết định | Đã chọn | Loại bỏ | Lý do |
|---|---|---|---|
| Retrieval strategy | **Hybrid (Dense + Sparse + RRF)** | Dense-only (embedding) | Dense bắt ngữ nghĩa nhưng bỏ sót khớp từ khóa chính xác (tên riêng, mã số). Sparse (BM25) khớp từ khóa nhưng không hiểu ngữ nghĩa. Hybrid RRF kết hợp ưu điểm cả hai. |
| Vector DB | **LanceDB** | Chroma, Pinecone | LanceDB: embedded (không cần server riêng), native Lance format nhanh, Python-native API. Pinecone: managed nhưng tốn chi phí, vendor lock-in. |
| Sparse search | **BM25s** | Elasticsearch | BM25s: lightweight Python library, đủ cho quy mô tài liệu nội bộ. Elasticsearch: overkill cho use case này, cần cluster riêng. |
| Parallel processing | **Ray Core** | multiprocessing | Ray: distributed computing, dễ scale lên multi-node, fault tolerance built-in. multiprocessing: chỉ chạy single node. |
| Chunking | **Semantic chunking** (theo section header) | Fixed-size 500 tokens | Tài liệu kỹ thuật có cấu trúc rõ ràng (heading hierarchy) → chunk theo section giữ ngữ cảnh tốt hơn cắt cố định. |

### Lớp 3 — Output
- RAG pipeline: Document Ingestion → Semantic Chunking → Dual Embedding (Dense + Sparse) → Hybrid Retrieval (RRF fusion) → LLM Generation
- MinHash LSH deduplication trước khi embedding (tránh redundant chunks)
- PII filtering trước khi đưa context vào LLM
- Ray Core parallelization cho embedding generation

### Lớp 4 — Outcome
| Metric | Giá trị |
|---|---|
| Retrieval accuracy (Hybrid vs Dense-only) | +15-20% recall trên technical queries có mã số/tên riêng |
| Deduplication rate (MinHash) | Loại ~30% duplicate chunks → tiết kiệm embedding cost |
| Embedding throughput (Ray vs single-thread) | 4-8x speedup nhờ parallelization |

### Lớp 5 — Impact
- Nhân viên tra cứu tài liệu kỹ thuật trong **giây** thay vì tìm kiếm thủ công hàng giờ
- Giảm rủi ro sử dụng thông tin outdated (chatbot luôn truy xuất version mới nhất)

### 🔥 Câu hỏi vặn
**"Tại sao dùng Hybrid mà không chỉ dùng Dense embedding?"**
> *"Dense embedding tốt cho câu hỏi ngữ nghĩa ('cách lắp bộ phanh') nhưng kém với truy vấn chứa mã số chính xác ('part number ABC-12345'). BM25 sparse search khớp từ khóa chính xác nhưng không hiểu 'bộ phanh' ≈ 'hệ thống phanh'. Reciprocal Rank Fusion (RRF) kết hợp ranking từ cả hai — ưu tiên documents xuất hiện ở top cả 2 hệ thống."*

---

## DỰ ÁN 3: OCR INVOICE PROCESSING

### Lớp 1 — Bối cảnh
Xử lý tự động hóa đơn giấy/scan cho bộ phận kế toán. Hàng ngàn hóa đơn cần được số hóa, trích xuất thông tin (số hóa đơn, ngày, số tiền, mã thuế), và đưa vào hệ thống ERP.

### Lớp 2 — Why
| Quyết định | Đã chọn | Lý do |
|---|---|---|
| OCR Engine | **PaddleOCR** | Open-source, hỗ trợ tiếng Việt tốt, chạy được trên CPU (không bắt buộc GPU) |
| Processing pattern | **Batching 15-20 mẫu/API call** | Giảm network round-trip overhead, tăng throughput 10-15x so với single request |
| Error handling | **Retry + exponential backoff + DLQ** | Phân biệt lỗi tạm thời (network timeout → retry) vs lỗi vĩnh viễn (ảnh hỏng → DLQ) |
| Orchestration | **Airflow** | DAG-based workflow, retry built-in, backfill support |

### Lớp 3 — Output
- Pipeline: Scan/Upload → Queue → PaddleOCR Batch Processing → Structured JSON → Validation → ERP Import
- DLQ (Dead Letter Queue) cho hóa đơn lỗi OCR → không block pipeline chính
- Validation rules: regex cho mã số thuế, range check cho số tiền

### Lớp 4 — Outcome
| Metric | Giá trị |
|---|---|
| Throughput | ~40,000 hóa đơn xử lý thành công |
| Batching speedup | 10-15x so với single request |
| Error recovery | 100% lỗi tạm thời được retry, lỗi vĩnh viễn route sang DLQ |

### Lớp 5 — Impact
- Kế toán giảm thời gian nhập liệu thủ công, tập trung vào kiểm tra/đối soát

### 🔥 Câu hỏi vặn
**"Nếu phải xử lý 4 triệu hóa đơn/ngày, thiết kế thay đổi gì?"**
> *"3 thay đổi chính: (1) Horizontal scaling OCR workers — queue-based architecture với Kafka/RabbitMQ để phân phối công việc. (2) GPU acceleration cho PaddleOCR inference thay vì CPU. (3) Airflow CeleryExecutor thay LocalExecutor để chạy song song nhiều task trên nhiều worker nodes."*

---

## DỰ ÁN 4: STUDENT BEHAVIORAL STREAMING (Neo4j)

### Lớp 1 — Bối cảnh
Phân tích hành vi học tập của sinh viên trên nền tảng e-learning. Dữ liệu interaction (xem bài, làm quiz, submit assignment) được stream real-time. Mục tiêu: phát hiện sinh viên có nguy cơ bỏ học sớm, đề xuất can thiệp.

### Lớp 2 — Why
| Quyết định | Đã chọn | Lý do |
|---|---|---|
| Database | **Neo4j (Graph DB)** | Quan hệ Student → Course → Assignment → Submission là multi-hop graph. Neo4j traverse O(1) per hop. RDBMS cần nhiều JOIN chain tốn kém khi depth tăng. |
| Data ingestion | **Kafka Streaming** | Real-time behavioral events cần được capture ngay, không đợi batch cuối ngày |
| Query pattern | **Cypher (Neo4j)** | Pattern matching trên graph tự nhiên hơn SQL cho bài toán "tìm sinh viên chưa submit assignment trong 7 ngày" |

### Lớp 3 — Output
- Streaming pipeline: App Events → Kafka → Stream Processor → Neo4j Graph
- Graph model: (Student)-[:ENROLLED]->(Course)-[:HAS]->(Assignment)<-[:SUBMITTED]-(Student)
- Cypher queries cho at-risk detection: students with no submissions in 7 days

### Lớp 4 — Outcome
- Phát hiện sinh viên at-risk trong **real-time** thay vì cuối kỳ
- Graph traversal: query multi-hop relationships trong **< 10ms**

### Lớp 5 — Impact
- Giảng viên/cố vấn có thể can thiệp sớm trước khi sinh viên bỏ học

### 🔥 Câu hỏi vặn
**"Tại sao Neo4j mà không dùng PostgreSQL với JOIN?"**
> *"Bài toán cần truy vấn 4-5 bước nhảy quan hệ (Student → Enrollment → Course → Module → Quiz → Submission). Trên PostgreSQL, mỗi bước nhảy = 1 JOIN, 5 bước = 5 JOINs lồng nhau → cost tăng theo hàm mũ khi depth tăng. Neo4j lưu pointer trực tiếp giữa node → mỗi hop = O(1) pointer lookup, performance không phụ thuộc vào tổng số nodes trong DB."*

---

## DỰ ÁN 5: VLM AUTO-LABELING

### Lớp 1 — Bối cảnh
Tự động gán nhãn (labeling) cho tập dữ liệu ảnh lớn sử dụng Vision-Language Model. Mục tiêu: giảm chi phí và thời gian labeling thủ công cho các dự án Computer Vision.

### Lớp 2 — Why
| Quyết định | Đã chọn | Lý do |
|---|---|---|
| Labeling approach | **VLM Auto-Labeling + Human Review** | Hoàn toàn tự động (no human) → quality thấp. Hoàn toàn thủ công → tốn thời gian/tiền. Semi-auto = VLM label trước, human chỉ review/correct → tối ưu cost-quality. |
| Model | **Vision-Language Model** | Zero-shot labeling khả năng cao, không cần fine-tune cho mỗi domain mới |

### Lớp 3 — Output
- Pipeline: Image Dataset → VLM Inference → Auto-Labels → Confidence Scoring → Human Review Queue
- Confidence threshold: high confidence → auto-approve, low confidence → human review

### Lớp 4 — Outcome
- Giảm thời gian labeling so với hoàn toàn thủ công
- Human reviewer chỉ cần xử lý ~20-30% ảnh có confidence thấp

### Lớp 5 — Impact
- Tăng tốc pipeline huấn luyện model CV cho các dự án downstream

---

## 📝 SCRIPT GIỚI THIỆU BẢN THÂN (3 phút — Highlight 2 dự án mạnh nhất)

> *"Chào anh/chị, em là [Tên], em có kinh nghiệm xây dựng Data Platform và Data Pipeline cho các hệ thống production.*
>
> *Dự án gần nhất và em tâm đắc nhất là **Fleet Maintenance Data Platform** — em xây dựng end-to-end pipeline từ PostgreSQL OLTP, qua Debezium CDC vào Kafka, Spark xử lý streaming và batch, HDFS Data Lake theo kiến trúc Medallion, Star Schema Data Warehouse, Redis serving layer, đến WebSocket Dashboard real-time cho Ban Giám đốc. Điểm em tự hào nhất là thiết kế hệ thống Data Quality Gate 5 tầng — sau một sự cố CDC connector chết 2 ngày mà không ai biết, em xây dựng monitoring tự động phát hiện sự cố trong 5 phút thay vì 2 ngày.*
>
> *Dự án thứ hai em muốn chia sẻ là **RAG/LLM Chat** — chatbot tra cứu tài liệu kỹ thuật nội bộ dùng Hybrid Retrieval kết hợp Dense Embedding (LanceDB) và Sparse Search (BM25) qua Reciprocal Rank Fusion. Em tối ưu deduplication bằng MinHash LSH trước embedding, giảm 30% redundant chunks.*
>
> *Em đam mê giải quyết các bài toán ở giao điểm giữa Data Engineering và hệ thống phân tán — đặc biệt là thiết kế pipeline có tính idempotent, fault-tolerant và có monitoring đầy đủ."*

---

## 📋 CHECKLIST ÔN TẬP TRỤ CỘT 1

- [ ] Thuộc lòng khung 5 lớp cho dự án Fleet (kể trong 90 giây)
- [ ] Thuộc lòng khung 5 lớp cho dự án RAG (kể trong 60 giây)
- [ ] Biết 3 câu hỏi vặn + đáp án cho Fleet
- [ ] Biết giải thích "Why Debezium not Polling" trong 30 giây
- [ ] Biết giải thích "Why Neo4j not PostgreSQL" trong 30 giây
- [ ] Biết giải thích "Why Hybrid not Dense-only" trong 30 giây
- [ ] Ghi âm script giới thiệu 3 phút, nghe lại, sửa chỗ lúng túng
