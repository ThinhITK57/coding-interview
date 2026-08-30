# CHIẾN LƯỢC PHỎNG VẤN FINAL ROUND TẠI VIETTEL CYBER SECURITY (VCS)
## Kịch Bản Toàn Diện: Trình Bày Dự Án Flagship, Vai Trò, Kỷ Luật Kỹ Thuật, Quản Trị Source Code & Làm Việc Nhóm

> **Vị thế & Kỳ vọng của Viettel Cyber Security (VCS):**
> *VCS là đơn vị an ninh mạng và công nghệ cao hàng đầu, nơi quản trị khối lượng dữ liệu khổng lồ với yêu cầu cực kỳ khắt khe về **Kỷ luật kỹ thuật (Engineering Rigor)**, **Tính làm chủ (Ownership)**, **Độ tin cậy hệ thống (System Reliability)**, và **Tư duy an toàn bảo mật (Security Mindset)**.*
> *Hội đồng phỏng vấn Final Round (Director / Tech Lead / Principal Architect) muốn thấy bạn là một **Senior/Middle Data Engineer thực chiến**: nói chuyện bằng kiến trúc logic, quy trình quản trị code bài bản, xử lý sự cố có chiều sâu RCA và khả năng phối hợp đa phòng ban.*

---

# MỤC LỤC CHIẾN LƯỢC

```
┌────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ PHẦN 0: TÂM THẾ & PHÂN BỔ THỜI LƯỢNG PHỎNG VẤN TẠI VCS                                                 │
├────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ PHẦN 1: BỐI CẢNH DỰ ÁN FLAGSHIP & MỤC TIÊU KINH DOANH (BUSINESS CONTEXT)                               │
├────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ PHẦN 2: HÀNH TRÌNH 4 GIAI ĐOẠN TRIỂN KHAI KỸ THUẬT (PROJECT LIFECYCLE & MILESTONES)                    │
│   2.1 Giai đoạn 1: Khảo sát, PoC & Thiết kế kiến trúc dòng chảy dữ liệu E2E                            │
│   2.2 Giai đoạn 2: Xây dựng Ingestion & Stream Processing (PostgreSQL WAL CDC → Kafka → Spark)         │
│   2.3 Giai đoạn 3: Thiết kế Data Lakehouse & Data Warehouse Star Schema (SCD Type 2)                   │
│   2.4 Giai đoạn 4: Tối ưu hóa Serving Layer Low-Latency & Real-time Push Dashboard (Redis → WebSocket) │
├────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ PHẦN 3: VAI TRÒ ĐÓNG GÓP CÁ NHÂN & OUTCOME ĐỊNH LƯỢNG RÕ RÀNG (MEASURABLE METRICS)                     │
├────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ PHẦN 4: QUẢN LÝ SOURCE CODE, CI/CD & KỶ LUẬT KỸ THUẬT (ENGINEERING EXCELLENCE)                         │
│   4.1 Quy chuẩn Git Flow & Branching Strategy                                                          │
│   4.2 Quy trình Code Review, Linting & Pre-commit Hooks                                                │
│   4.3 Chiến lược Kiểm thử tự động (Unit Test, Integration Test, Data Quality Testing)                  │
│   4.4 Pipeline CI/CD tự động hóa Deployment (GitLab CI / Docker / Helm / Airflow)                      │
├────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ PHẦN 5: QUẢN LÝ CÔNG VIỆC & PHƯƠNG PHÁP LÀM VIỆC NHÓM (AGILE & CROSS-TEAMWORK)                         │
│   5.1 Quy trình Quản trị công việc (Agile/Scrum trên Jira)                                             │
│   5.2 Tương tác và giải quyết bài toán với 4 bên liên quan (DBA, BI, Infra/Security, Product Owner)    │
├────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ PHẦN 6: NĂNG LỰC XỬ LÝ TÌNH HUỐNG, SỰ CỐ PRODUCTION & BÀI HỌC RCA (INCIDENT MANAGEMENT)               │
│   6.1 Sự cố 1: Treo Replication Slot & Nguy cơ tràn ổ đĩa PostgreSQL                                   │
│   6.2 Sự cố 2: Small File Problem làm kiệt quệ RAM NameNode HDFS                                       │
│   6.3 Sự cố 3: Data Skew khi tổng hợp báo cáo doanh thu trên Spark                                     │
├────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ PHẦN 7: TƯ DUY AN TOÀN & BẢO MẬT DỮ LIỆU (SECURITY MINDSET — ĐIỂM CỘNG ĐẶC BIỆT CHO VCS)               │
├────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ PHẦN 8: KỊCH BẢN TRÌNH BÀY MẪU 8-10 PHÚT CHUẨN STAR & CÂU HỎI CHIẾN LƯỢC CUỐI BUỔI                     │
└────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

# PHẦN 0: TÂM THẾ & PHÂN BỔ THỜI LƯỢNG PHỎNG VẤN TẠI VCS

```
┌───────────────────┬──────────────┬─────────────────────────────────────────────────────────────────────┐
│ Khung Thời Gian   │ Thời Lượng   │ Mục Tiêu Chiến Lược                                                 │
├───────────────────┼──────────────┼─────────────────────────────────────────────────────────────────────┤
│ 00:00 – 02:00     │ 2 Phút       │ Mở đầu: Phong thái tự tin, tác phong kỷ luật, tóm lược bản thân.    │
│ 02:00 – 12:00     │ 10 Phút      │ Trình bày Dự án Flagship theo khung chuẩn: Bối cảnh → Đột phá kiến │
│                   │              │ trúc → Quản lý Code/CI/CD → Làm việc nhóm → Outcome định lượng.     │
│ 12:00 – 25:00     │ 13 Phút      │ Trả lời phỏng vấn sâu: Giải thích quyết định công nghệ, bảo mật,     │
│                   │              │ xử lý sự cố thực tế và các tình huống scale hệ thống.               │
│ 25:00 – 30:00     │ 5 Phút       │ Kết thúc: Đặt 2 câu hỏi chiến lược thể hiện tầm nhìn và văn hóa VCS.│
└───────────────────┴──────────────┴─────────────────────────────────────────────────────────────────────┘
```

---

# PHẦN 1: BỐI CẢNH DỰ ÁN FLAGSHIP & MỤC TIÊU KINH DOANH

### 🎯 Tên dự án:
**Hệ Thống Nền Tảng Dữ Liệu Bảo Dưỡng & Sửa Chữa Xe Tải Tập Trung (Fleet Maintenance & Repair Data Platform)**

```
┌────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ MỤC TIÊU KINH DOANH CỦA DOANH NGHIỆP:                                                                 │
│ Nền tảng quản lý vận hành chuỗi 50 trạm sửa chữa (Heads) trên toàn quốc, phục vụ 2 nguồn doanh thu:    │
│   (1) Doanh thu Dịch vụ Sửa chữa / Bảo dưỡng sau bán xe.                                               │
│   (2) Doanh thu Bán Linh kiện Phụ tùng thay thế.                                                       │
├────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ BA ĐIỂM NGHẼN DỮ LIỆU NGHIÊM TRỌNG TRƯỚC KHI XÂY DỰNG DATA PLATFORM:                                   │
│   1. Quá tải Cơ sở dữ liệu vận hành: Hệ thống ERP (Odoo trên PostgreSQL) thường xuyên bị nghẽn I/O    │
│      và khóa bảng vào cuối tháng do các truy vấn SQL báo cáo doanh thu tài khóa và phân tích khoảng cách.│
│   2. Độ trễ báo cáo quá lớn (T+30 ngày): Ban Giám đốc phải đợi báo cáo Excel tổng hợp thủ công cuối    │
│      tháng, không có khả năng phát hiện sớm sự sụt giảm doanh thu theo từng trạm.                      │
│   3. Sai lệch dữ liệu lịch sử: Khi trạng thái khách hàng thay đổi (Mới → Thường niên), OLTP update     │
│      đè trực tiếp lên dòng cũ, làm sai lệch toàn bộ báo cáo phân tích hiệu quả khuyến mãi các quý trước.│
└────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

# PHẦN 2: HÀNH TRÌNH 4 GIAI ĐOẠN TRIỂN KHAI KỸ THUẬT

```
┌────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ SƠ ĐỒ DÒNG CHẢY DỮ LIỆU KIẾN TRÚC END-TO-END                                                           │
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

### 2.1 Giai Đoạn 1: Khảo sát, PoC & Thiết Kế Kiến Trúc
* Đánh giá và lựa chọn giải pháp: So sánh giữa *Query Polling* và *Log-based CDC*. Quyết định chọn **Debezium CDC** đọc trực tiếp WAL của PostgreSQL để đảm bảo **0% tải phụ lên DB vận hành**, đồng thời bắt trọn vẹn sự kiện `DELETE` và các trạng thái cập nhật trung gian.
* Thiết lập môi trường cụm phân tán: Cài đặt cụm **Hadoop Multi-Node (Master, Slave1, Slave2)**, phân định rõ ràng các tiến trình NameNode, DataNode, ResourceManager và NodeManager.

### 2.2 Giai Đoạn 2: Xây Dựng Pipeline Ingestion & Stream Processing
* Cấu hình Debezium với plugin `pgoutput` và bật `REPLICA IDENTITY FULL` trên PostgreSQL để ghi nhận đầy đủ Before-image và After-image của từng giao dịch.
* Thiết kế Kafka Cluster 3 Brokers, Partitioning key theo `head_id` hoặc `customer_id` để đảm bảo thứ tự sự kiện (In-order processing).
* Viết ứng dụng **Spark Structured Streaming**: Xử lý dòng dữ liệu với cơ chế Watermark 10 phút, checkpointing định kỳ 30 giây, nạp trực tiếp vào HDFS Data Lake dưới định dạng Parquet nén Snappy.

### 2.3 Giai Đoạn 3: Thiết Kế Data Lakehouse & Data Warehouse Star Schema
* Xây dựng mô hình **Star Schema** gồm 2 Fact Tables (`fact_repair_service_revenue`, `fact_parts_sales`) và 4 Dimensions (`dim_customer`, `dim_head`, `dim_component`, `dim_date`).
* Xây dựng logic **SCD Type 2 (Slowly Changing Dimension)** cho `dim_customer`: Tự động sinh Surrogate Key bằng mã băm MD5, quản lý khoảng thời gian hiệu lực `effective_date`, `expiration_date` và cờ `is_current` để lưu vết lịch sử chính xác 100%.
* Thiết kế bảng `dim_date` tính toán sẵn logic Năm Tài Khóa (Fiscal Year từ 01/10 đến 30/09) phục vụ báo cáo tài chính đặc thù của doanh nghiệp.

### 2.4 Giai Đoạn 4: Tối Ưu Hóa Serving Layer & Real-Time Push Dashboard
* Đồng bộ danh mục trạm và tồn kho sang **Redis 7+**: Sử dụng **`GEOSEARCH`** tính toán khoảng cách vị trí tài xế đến 50 trạm trong **$< 1\text{ms}$** (thay vì 50-100ms trên PostGIS).
* Lập trình **Redis Lua Script** chạy đơn luồng nguyên tử (Single-threaded Atomic Execution) để xử lý logic giữ chỗ khung giờ sửa chữa (Slot Reservation), triệt tiêu hoàn toàn rủi ro đặt trùng lịch (Overbooking).
* Xây dựng tầng Backend trung gian kết nối **WebSocket**: Khi Spark/Airflow hoàn tất mẻ tính toán tổng hợp, hệ thống chủ động đẩy (Push) dữ liệu xuống Dashboard PowerBI/Web trong **$< 15\text{ms}$**, giải phóng hoàn toàn 360 request polling/phút.

---

# PHẦN 3: VAI TRÒ ĐÓNG GÓP CÁ NHÂN & OUTCOME ĐỊNH LƯỢNG RÕ RÀNG

### 🎯 Vai trò cá nhân:
**Lead / Sole Data Engineer chịu trách nhiệm 100% thiết kế kiến trúc, triển khai pipeline, tối ưu hóa cơ sở dữ liệu và vận hành hệ thống dữ liệu.**

```
┌────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ BẢNG THỐNG KÊ OUTCOME ĐO LƯỜNG ĐƯỢC (BEFORE VS AFTER):                                                 │
├───────────────────────────────┬──────────────────────────────┬─────────────────────────────────────────┤
│ Chỉ Số Đo Lường (Metrics)     │ Trước Khi Có Hệ Thống        │ Sau Khi Tôi Triển Khai Hệ Thống         │
├───────────────────────────────┼──────────────────────────────┼─────────────────────────────────────────┤
│ Độ trễ Báo cáo Doanh thu      │ T+30 ngày (Excel cuối tháng) │ T+15 phút (Cập nhật tự động liên tục)   │
│ Tải I/O trên PostgreSQL OLTP  │ Rất cao (Nghẽn bảng cuối quý)│ 0% tải phân tích (Chỉ đọc WAL Log)      │
│ Độ trễ Tìm trạm gần nhất      │ 85 ms (Truy vấn PostGIS)     │ < 1 ms (Redis GEOSEARCH in-memory)      │
│ Tỉ lệ Đặt trùng lịch trạm     │ Có xảy ra tranh chấp         │ 0% Overbooking (Redis Lua Atomic)       │
│ Thời gian Phát hiện Sự cố CDC │ Mất 2 ngày mới phát hiện     │ < 5 phút (Data Quality Gate 5 tầng)     │
│ Độ chính xác Lịch sử Khách    │ 0% (Bị ghi đè mất dữ liệu)   │ 100% chính xác lịch sử (SCD Type 2)     │
└───────────────────────────────┴──────────────────────────────┴─────────────────────────────────────────┘
```

---

# PHẦN 4: QUẢN LÝ SOURCE CODE, CI/CD & KỶ LUẬT KỸ THUẬT

### 4.1 Quy Chuẩn Quản Lý Mã Nguồn (Git Flow Strategy):

```
  [main] ─────────────────────────────────────────────────────────────● (v1.2.0 Production Release)
    │                                                                 ▲
    │                                                    [hotfix/*] ──┘ (Sửa lỗi khẩn cấp)
    ▼
  [develop] ───────────────────●───────────────────────●──────────────► (Staging / Integration)
    │                          ▲                       ▲
    ├──────► [feature/cdc-sync]─┘                       │
    └──────► [feature/scd2-merge]───────────────────────┘
```

* **Branch Rules**:
  * Nhánh `main`: Tuyệt đối không push trực tiếp. Chỉ nhận code từ `develop` qua Pull Request (PR) sau khi vượt qua $100\%$ bài test và có ít nhất 1 Senior/Lead phê duyệt (Approved).
  * Nhánh `develop`: Môi trường Staging tích hợp liên tục.
  * Nhánh `feature/*`: Phát triển tính năng mới, tạo từ `develop` và rebase thường xuyên.

### 4.2 Kỷ Luật Code Review, Linting & Pre-commit Hooks:
* **Pre-commit Hooks tự động**:
  * `flake8`, `black`: Tự động định dạng code Python theo chuẩn PEP8.
  * `sqlfluff`: Tự động chuẩn hóa cú pháp SQL cho các câu lệnh DDL/DML.
  * `gitleaks` / `trufflehog`: Quét tự động để ngăn chặn lập trình viên vô tình commit mật khẩu, API keys, hoặc credentials lên Git repository (Đặc biệt quan trọng theo tiêu chuẩn an ninh mạng VCS).
* **Tiêu chuẩn Code Review**: Mọi PR đều phải giải trình rõ: (1) Mục tiêu thay đổi, (2) Kế hoạch kiểm thử (Test Plan), và (3) Đánh giá ảnh hưởng hiệu năng (Performance Impact).

### 4.3 Chiến Lược Kiểm Thử Tự Động (Automated Testing Strategy):
1. **Unit Testing cho PySpark**: Sử dụng `pytest` kết hợp tạo local `SparkSession` giả lập để kiểm tra tính đúng đắn của các hàm Transformation, logic tách ngày tài khóa và thuật toán sinh Surrogate Key.
2. **Data Quality Testing (Great Expectations / Custom Checks)**:
   * Kiểm tra tính duy nhất (Uniqueness) của Primary Key.
   * Kiểm tra Null constraint trên các trường bắt buộc (`amount`, `invoice_date`).
   * Kiểm tra Schema Drift: Tự động cảnh báo nếu cấu trúc bảng nguồn bị thêm/bớt cột.

### 4.4 Pipeline CI/CD Tự Động Hóa (GitLab CI / Docker / Helm):
* **Stage 1 (Lint & Security Scan)**: Chạy SonarQube quét lỗ hổng bảo mật và code smell.
* **Stage 2 (Test)**: Chạy tự động $100\%$ bộ Unit Test và Data Quality Test.
* **Stage 3 (Build)**: Đóng gói ứng dụng Spark và FastAPI thành Docker Image, đẩy lên Docker Registry nội bộ.
* **Stage 4 (Deploy)**: Tự động deploy Airflow DAGs mới vào thư mục `/dags` và trigger đồng bộ cấu hình connector trên Kafka Connect.

---

# PHẦN 5: QUẢN LÝ CÔNG VIỆC & PHƯƠNG PHÁP LÀM VIỆC NHÓM

### 5.1 Quản Trị Công Việc Theo Chuẩn Agile / Scrum Trên Jira:
* Vận hành theo **Sprint 2 tuần**:
  * *Sprint Planning*: Đánh giá Story Points, phân rã công việc kỹ thuật (Task decomposition).
  * *Daily Standup (15 phút)*: Báo cáo 3 câu hỏi: Hôm qua làm gì? Hôm nay làm gì? Có gặp blocker kỹ thuật nào cần hỗ trợ không?
  * *Sprint Review & Retrospective*: Đo lường Velocity của team và rút ra các điểm cần cải tiến quy trình.

### 5.2 Tương Tác & Phối Hợp Hiệu Quả Với 4 Nhóm Phòng Ban (Cross-Functional Collaboration):

```
┌────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ BẢN ĐỒ PHỐI HỢP LIÊN PHÒNG BAN CỦA DATA ENGINEER:                                                      │
├──────────────────────────┬─────────────────────────────────────────────────────────────────────────────┤
│ Nhóm Chức Năng           │ Cách Thức Làm Việc & Giải Quyết Vấn Đề Thực Tế                              │
├──────────────────────────┼─────────────────────────────────────────────────────────────────────────────┤
│ 1. Đội ngũ DBA & ERP     │ • Thống nhất cấu hình wal_level=logical trên PostgreSQL.                    │
│    (Database Admin)      │ • Đàm phán thiết lập Replication Slot và cơ chế cảnh báo dung lượng disk.  │
│                          │ • Cam kết 0% truy vấn polling định kỳ đè lên hệ thống ERP vận hành.         │
├──────────────────────────┼─────────────────────────────────────────────────────────────────────────────┤
│ 2. Đội ngũ BI & Phân tích│ • Ký kết Data Contract: Định nghĩa rõ Grain của Fact và chuẩn hóa Data Dict.│
│    (BI / Data Analysts)  │ • Thống nhất SLA về độ tươi dữ liệu (Data Freshness <= 15 phút).            │
│                          │ • Hỗ trợ tối ưu hóa mô hình Star Schema để PowerBI load dữ liệu dưới 1s.    │
├──────────────────────────┼─────────────────────────────────────────────────────────────────────────────┤
│ 3. Đội ngũ Hạ Tầng & ATTT│ • Phối hợp cấp phát RAM/CPU trên cụm phân tán YARN và dung lượng HDFS.      │
│    (Infra & Security)    │ • Tuân thủ chính sách mạng cách ly (VLAN/Firewall), mã hóa dữ liệu nhạy cảm.│
│                          │ • Quản lý tập trung mật khẩu/chứng chỉ qua Vault/Secrets.                   │
├──────────────────────────┼─────────────────────────────────────────────────────────────────────────────┤
│ 4. Product Owner & Lãnh  │ • Dịch các bài toán kinh doanh (Doanh thu năm tài khóa, tỉ lệ chuyển đổi    │
│    đạo Doanh nghiệp      │   khách hàng) thành các tiêu chí kỹ thuật cụ thể (KPIs & SLAs).             │
│                          │ • Báo cáo tiến độ minh bạch và đề xuất các giải pháp tối ưu chi phí hạ tầng.│
└──────────────────────────┴─────────────────────────────────────────────────────────────────────────────┘
```

---

# PHẦN 6: NĂNG LỰC XỬ LÝ TÌNH HUỐNG, SỰ CỐ PRODUCTION & BÀI HỌC RCA

### 6.1 Sự Cố 1: Treo Replication Slot & Nguy Cơ Tràn Ổ Đĩa PostgreSQL
* **Hiện tượng**: Debezium Connector bị mất kết nối mạng âm thầm trong 2 ngày, không có cảnh báo nào được kích hoạt. Thư mục `pg_wal` trên máy chủ PostgreSQL tăng vọt từ $5\text{GB} \to 45\text{GB}$, suýt làm tràn $100\%$ ổ cứng hệ điều hành.
* **Xử lý khẩn cấp**: Khởi động lại connector, giải phóng WAL và chạy bù dữ liệu (Backfill) thành công nhờ Kafka giữ log 7 ngày.
* **Giải pháp gốc rễ (Root Cause Analysis - RCA)**:
  * Viết câu query giám sát dung lượng giữ chân WAL:
    `SELECT pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) FROM pg_replication_slots;`
  * Bật chốt an toàn `max_slot_wal_keep_size = 10GB` trong `postgresql.conf` để tự động ngắt slot nếu lag vượt ngưỡng, cứu sống DB.
  * Tích hợp **Data Quality Gate 5 tầng** cảnh báo Telegram/Slack trong **$< 5$ phút**.

### 6.2 Sự Cố 2: Small File Problem Làm Kiệt Quệ RAM NameNode HDFS
* **Hiện tượng**: Spark Streaming trigger mỗi 10 giây ghi ra HDFS $\to$ Sau 3 tuần sinh ra hơn $150.000$ file Parquet nhỏ $\to$ NameNode heap usage tăng lên $80\%$, dính hiện tượng Java GC Pause liên tục.
* **Giải pháp gốc rễ (RCA)**:
  1. Tăng trigger interval từ $10\text{s} \to 5\text{ phút}$.
  2. Viết Airflow DAG chạy **Nightly Compaction Job**: Đọc toàn bộ file nhỏ trong partition ngày hôm trước, dùng `coalesce(4)` gộp thành 4 file chuẩn ($128\text{MB}-256\text{MB}$), ghi đè lại partition.
  3. Kết quả: Giảm **$98\%$ số lượng file**, RAM NameNode giải phóng về mức an toàn $40\%$.

### 6.3 Sự Cố 3: Data Skew Khi Tổng Hợp Doanh Thu Khách Hàng Lớn Trên Spark
* **Hiện tượng**: 1 khách hàng doanh nghiệp vận tải lớn chiếm $40\%$ lượng hóa đơn toàn hệ thống $\to$ Khi thực hiện `groupBy("customer_id")`, 1 Task bị dồn $2$ triệu dòng chạy mất $30$ phút trong khi 99 Task khác xong trong $1$ phút.
* **Giải pháp gốc rễ (RCA)**:
  * Áp dụng kỹ thuật **Salting (2-Stage Aggregation)**: Thêm số ngẫu nhiên $0-9$ vào key (`customer_id_0` đến `customer_id_9`) để phân tán đều $2$ triệu dòng sang 10 Partition $\to$ Chạy partial aggregate $\to$ Gộp lại ở Stage 2.
  * Kết quả: Đưa tổng thời gian thực thi từ **$30\text{ phút} \to 1.5\text{ phút}$ (nhanh hơn $20\times$)**.

---

# PHẦN 7: TƯ DUY AN TOÀN & BẢO MẬT DỮ LIỆU (SECURITY MINDSET — ĐIỂM CỘNG VCS)

1. **Che mờ Dữ liệu Định danh Cá nhân (PII Data Masking)**:
   * Số điện thoại, địa chỉ, thông tin tài xế được mã hóa một chiều (SHA-256 kèm Salt) hoặc Masking (`0912***678`) ngay tại tầng Ingestion trước khi lưu trữ vào Data Lake và Vector Database.
2. **Nguyên Tắc Đặc Quyền Tối Thiểu (Principle of Least Privilege & RBAC)**:
   * Tài khoản Debezium chỉ được cấp quyền `REPLICATION` và `SELECT` trên đúng danh sách bảng chỉ định.
   * Phân quyền Role-Based Access Control trên PostgreSQL và HDFS: BI Team chỉ có quyền đọc trên schema Data Mart (`Gold`), không được truy cập dữ liệu thô (`Bronze/Silver`).
3. **Quản Trị Bí Mật (Secrets Management)**:
   * Không lưu trữ mật khẩu DB, Kafka credentials dạng plain text trong code hoặc file cấu hình. Toàn bộ được quản lý tập trung qua **HashiCorp Vault / Kubernetes Secrets** và nạp qua Environment Variables.
4. **Audit Logging & Dấu Vết Dữ Liệu (Data Lineage)**:
   * Mọi sự thay đổi cấu trúc bảng, quyền truy cập dữ liệu và lịch sử thực thi pipeline đều được ghi log tập trung phục vụ công tác kiểm toán an toàn thông tin.

---

# PHẦN 8: KỊCH BẢN TRÌNH BÀY MẪU 8-10 PHÚT CHUẨN STAR & 2 CÂU HỎI CUỐI

### 🎬 KỊCH BẢN TRÌNH BÀY DỰ ÁN FLAGSHIP (NÓI TRONG 8-10 PHÚT):

> **[Phút 1 — Mở đầu & Bối cảnh kinh doanh]**
> *"Kính chào các anh trong Hội đồng Tuyển dụng Viettel Cyber Security. Dự án em tâm đắc nhất và thể hiện rõ nét nhất năng lực Data Engineering toàn diện của em là **Fleet Maintenance & Repair Data Platform** — hệ thống dữ liệu tập trung phục vụ chuỗi 50 trạm dịch vụ xe tải trên toàn quốc.*
> *Bài toán đặt ra lúc đó là: Hệ thống ERP Odoo trên PostgreSQL thường xuyên bị quá tải vào cuối tháng do các truy vấn báo cáo tài khóa; Ban Giám đốc phải chờ đợi báo cáo Excel thủ công tới T+30 ngày; và việc cập nhật đè trạng thái khách hàng làm mất sạch dữ liệu lịch sử phân tích.*
>
> **[Phút 2-4 — Vai trò & Đột phá Kiến trúc Kỹ thuật]**
> *Với vai trò là Lead Data Engineer chịu trách nhiệm thiết kế và triển khai toàn bộ nền tảng, em đã thực hiện 4 đột phá kiến trúc:*
> *Thứ nhất — Thay vì query polling gây áp lực lên DB vận hành, em cấu hình **Debezium CDC đọc trực tiếp WAL của PostgreSQL với REPLICA IDENTITY FULL**, đẩy qua cụm Kafka 3 brokers. Giải pháp này giúp triệt tiêu **100% tải query lên OLTP**, bắt trọn vẹn sự kiện DELETE và trạng thái trung gian.*
> *Thứ hai — Xây dựng Data Lakehouse trên **HDFS Parquet** và DWH theo **Star Schema**, tự tay lập trình logic **SCD Type 2** cho bảng chiều khách hàng với Surrogate Key MD5, bảo toàn 100% lịch sử giao dịch.*
> *Thứ ba — Tối ưu Serving Layer với **Redis 7+**: Lệnh `GEOSEARCH` đưa thời gian tìm trạm từ 85ms xuống dưới 1ms; lập trình **Redis Lua Script nguyên tử** triệt tiêu 100% lỗi đặt trùng lịch; và lớp Backend **WebSocket** chủ động đẩy số liệu mới xuống Dashboard trong dưới 15ms.*
> *Thứ tư — Xây dựng hệ thống **Data Quality Gate 5 tầng** tự động phát hiện mọi bất thường của pipeline trong dưới 5 phút.*
>
> **[Phút 5-6 — Quản trị Code, CI/CD & Làm việc nhóm]**
> *Để đảm bảo tính kỷ luật kỹ thuật cao nhất:*
> *Về quản lý mã nguồn: Em áp dụng chuẩn **Git Flow**, bảo vệ nhánh main nghiêm ngặt qua Pull Request, tích hợp **Pre-commit hooks** quét lỗi cú pháp và tự động quét lộ lọt mật khẩu bằng Gitleaks.*
> *Về kiểm thử & CI/CD: Viết Unit Test cho PySpark với Pytest, tự động hóa đóng gói Docker và deploy Airflow DAGs qua GitLab CI.*
> *Về phối hợp nhóm: Em làm việc chặt chẽ theo Agile/Scrum, đàm phán với team DBA về cấu hình WAL, ký kết Data Contract với team BI và làm việc với team Hạ tầng để cấp phát tài nguyên YARN/HDFS tối ưu.*
>
> **[Phút 7-8 — Sự cố Production & Bài học Thực chiến]**
> *Hành trình làm dự án cũng cho em những bài học xương máu. Đáng nhớ nhất là sự cố **Replication Slot bị treo 2 ngày** khiến dung lượng WAL tăng vọt suýt làm tràn ổ cứng PostgreSQL. Em đã khôi phục dữ liệu thành công từ Kafka, sau đó thiết lập cấu hình an toàn `max_slot_wal_keep_size = 10GB` và xây dựng cơ chế giám sát tự động bắn cảnh báo trong 5 phút.*
>
> **[Phút 9-10 — Outcome & Tổng kết]**
> *Kết quả chung cuộc: Hệ thống đưa độ trễ báo cáo từ **T+30 ngày xuống T+15 phút**, giảm **100% tải trên OLTP**, đạt thông lượng xử lý ổn định và đảm bảo an toàn dữ liệu tuyệt đối.*
> *Em tin rằng tư duy kiến trúc vững chắc, kỹ luật quản trị code bài bản và kinh nghiệm xử lý sự cố thực chiến này sẽ đóng góp ngay lập tức vào các bài toán dữ liệu lớn và an ninh mạng tại Viettel Cyber Security."*

---

### 🎯 2 CÂU HỎI CHIẾN LƯỢC DÀNH CHO BẠN HỎI NGƯỢC HỘI ĐỒNG TUYỂN DỤNG Ở PHÚT 28:

1. *"Tại Viettel Cyber Security, với đặc thù dữ liệu an ninh mạng và log SIEM có thông lượng hàng trăm ngàn events/giây, kiến trúc Data Platform của team hiện tại đang ưu tiên tối ưu hóa theo hướng **Streaming-First (như Apache Flink / Kafka Streams)** kết hợp kho dữ liệu dạng cột **ClickHouse / Apache Iceberg** như thế nào?"*
2. *"Đối với một Data Engineer tại VCS, tiêu chuẩn về **Kỷ luật kỹ thuật (Engineering Standards)** và **Văn hóa chủ động làm chủ sự cố (Ownership & Incident Response)** được đo lường và kỳ vọng cụ thể ra sao trong giai đoạn thử việc?"*
