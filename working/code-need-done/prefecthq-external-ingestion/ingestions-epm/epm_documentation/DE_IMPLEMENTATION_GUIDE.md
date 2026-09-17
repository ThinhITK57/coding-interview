# HƯỚNG DẪN CHI TIẾT TRIỂN KHAI VÀ VẬN HÀNH PIPELINE EPM
## (DATA ENGINEER IMPLEMENTATION & OPERATIONAL MANUAL)

> **Dự án:** `crawler-prefecthq-02` (Planview Clarizen Lakehouse Data Platform)  
> **Phạm vi:** Triển khai Chiến lược Crawl API, Chuyển đổi Bronze-Silver-Gold, Views 6 Bài toán Nghiệp vụ và Tầng dbt Semantic Layer.  
> **Môi trường kỹ thuật:** Python 3.7.1 | Apache Spark 2.3.2 | Java 8 | Prefect HQ | Trino (Hive Metastore) | dbt-trino 1.3+  
> **Vị trí tài liệu:** `working/crawler-prefecthq-02/crawler-prefecthq/docs/DE_IMPLEMENTATION_GUIDE.md`  

---

## 1. NGUYÊN TẮC PHÂN ĐỊNH TRÁCH NHIỆM: DATA ENGINEER (DE) vs DATA ANALYST (DA)

Trong kiến trúc Enterprise Lakehouse hiện đại, vai trò của **Data Engineer** và **Data Analyst** được phân định một cách nghiêm ngặt:

```
┌───────────────────────────────────────────────────────────────────────────────────┐
│                      TẦNG VẬT LÝ DATA WAREHOUSE (DE CHỊU TRÁCH NHIỆM)             │
│  - Raw API Ingestion -> Parquet Lakehouse (MinIO/S3).                             │
│  - Conformed Data Types & Deduplication (sysid + last_updated_on).                │
│  - Tạo 6 Gold DW Views/Tables chứa ĐÚNG VÀ ĐỦ các cột dữ liệu gốc theo 6 BRs.     │
│  - TUYỆT ĐỐI KHÔNG tự ý suy diễn hoặc tính sẵn các cột chỉ số nghiệp vụ            │
│    (Không tạo cột achievement_rate, gap, is_overdue, health_score...).            │
└────────────────────────────────────────┬──────────────────────────────────────────┘
                                         │
                                         ▼
┌───────────────────────────────────────────────────────────────────────────────────┐
│                     TẦNG SEMANTIC LAYER & METRICS (DA & GenBI CHỊU TRÁCH NHIỆM)   │
│  - Định nghĩa công thức tính toán tại dbt `meta.semantic_guidance` và `metrics:`. │
│  - achievement_rate = (target_result / target_value) * 100.                       │
│  - gap = target_value - target_result.                                            │
│  - is_overdue = CASE WHEN due_date < CURRENT_DATE AND percent_completed < 100.    │
│  - Trợ lý GenBI và công cụ BI tự động render công thức động theo ngữ cảnh lọc.    │
└───────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. BẢNG CHIẾN LƯỢC CRAWL DỮ LIỆU ĐÃ TRIỂN KHAI VÀO CODE

Toàn bộ chiến lược crawl dữ liệu đã được cấu hình trực tiếp vào [prefect.yaml](../prefect.yaml) và [config.json](../config.json):

| Thực thể (Table) | Tần suất Crawl | Chế độ (Mode) | Trường Watermark | Cron Expression (Prefect HQ) | Triển khai trong Code & Deployment |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Tasks** | Mỗi 2 tiếng (Giờ HC: 08:00 - 18:00) | **Incremental** (Lookback 15m) | `LastUpdatedOn` | `0 8-18/2 * * 1-5` | Deployment: `epm-tasks-incremental-2h` |
| **Tasks (Weekly)** | 1 lần / tuần (Chủ Nhật 23:00) | **Full Sync** | Không (Full Scan) | `0 23 * * 0` | Deployment: `epm-tasks-full-weekly` |
| **Projects** | 2 lần / ngày (12:00 trưa & 18:30 tối) | **Incremental** (Lookback 30m) | `LastUpdatedOn` | Trưa: `0 12 * * 1-5`<br>Tối: `30 18 * * 1-5` | Deployment: `epm-projects-targets-midday` / `epm-projects-targets-evening` |
| **Targets** | 2 lần / ngày (12:15 trưa & 18:45 tối) | **Incremental** (Lookback 30m) | `LastUpdatedOn` | Trưa: `15 12 * * 1-5`<br>Tối: `45 18 * * 1-5` | Deployment: `epm-targets-midday` / `epm-targets-evening` |
| **Objectives (BSC)**| 1 lần / ngày (01:00 AM đêm) | **Full Sync** | Không (Full Scan) | `0 1 * * *` | Deployment: `epm-objectives-full-daily` |
| **Assignments (PGNV)**| 1 lần / ngày (01:30 AM đêm) | **Full Sync** | Không (Full Scan) | `30 1 * * *` | Deployment: `epm-assignments-full-daily` |
| **User Access Log** | 1 lần / ngày (02:00 AM đêm) | **Incremental** (T-1) | `login_date` | `0 2 * * *` | Deployment: `epm-access-log-daily` |
| **Master Orchestration**| 1 lần / ngày (03:00 AM đêm) | **DAG Orchestration** | Theo phụ thuộc | `0 3 * * *` | Deployment: `epm-all-endpoints-dag-master` |

---

## 3. DANH SÁCH FILE VÀ MÃ NGUỒN ĐÃ ĐƯỢC THIẾT KẾ VÀ BỔ SUNG

### 3.1. Cấu hình Điều phối Prefect HQ
- **[prefect.yaml](../prefect.yaml)**: Cấu hình 8 Deployments tương ứng với toàn bộ lịch trình crawl độc lập và DAG tổng thể, chạy trên Work Pool `epm-pool` với múi giờ chuẩn `Asia/Ho_Chi_Minh`.

### 3.2. Script Chuyển đổi Dữ liệu Apache Spark
- **[spark/bronze_to_silver.py](../spark/bronze_to_silver.py)**:
  - Đọc dữ liệu thô Bronze Parquet.
  - Áp dụng hợp đồng lược đồ chặt chẽ từ `data_type/*.sql` qua `SchemaContract`.
  - Thực hiện khử trùng lặp khóa chính `sysid` theo `last_updated_on` qua `DedupEngine`.
  - Phân vùng dữ liệu Silver theo `ingest_date` và nén snappy.
- **[spark/silver_to_gold.py](../spark/silver_to_gold.py)**:
  - Đọc 5 bảng Silver và Log truy cập.
  - Xây dựng 6 bảng Gold đại diện cho 6 bài toán nghiệp vụ với **CHÍNH XÁC VÀ DUY NHẤT các cột nghiệp vụ yêu cầu**, không suy diễn chỉ số.

### 3.3. Câu lệnh DDL Tạo View Vật lý
- **[generated_ddl/all_6_business_views_spark.sql](../generated_ddl/all_6_business_views_spark.sql)**: Cung cấp DDL Spark SQL (`CREATE OR REPLACE VIEW bi_gold.vw_...`) dùng cho tính toán hàng loạt hoặc phân tích qua Apache Zeppelin.
- **[generated_ddl/all_6_business_views_trino.sql](../generated_ddl/all_6_business_views_trino.sql)**: Cung cấp DDL Trino SQL (`CREATE OR REPLACE VIEW hive.bi_gold.vw_...`) phục vụ các BI Tools và kết nối Semantic Layer.

### 3.4. Mô hình dbt và Metadata Semantic Layer
Nằm trong thư mục `dbt_semantic/models/`:
- **BR-01:** [br01_bsc_yearly.sql](../dbt_semantic/models/br01_bsc_yearly.sql) & [br01_bsc_yearly__schema.yml](../dbt_semantic/models/br01_bsc_yearly__schema.yml)
- **BR-02:** [br02_dieu_hanh_cvct_klcd.sql](../dbt_semantic/models/br02_dieu_hanh_cvct_klcd.sql) & [br02_dieu_hanh_cvct_klcd__schema.yml](../dbt_semantic/models/br02_dieu_hanh_cvct_klcd__schema.yml)
- **BR-03:** [br03_task_report.sql](../dbt_semantic/models/br03_task_report.sql) & [br03_task_report__schema.yml](../dbt_semantic/models/br03_task_report__schema.yml)
- **BR-04:** [br04_project_report.sql](../dbt_semantic/models/br04_project_report.sql) & [br04_project_report__schema.yml](../dbt_semantic/models/br04_project_report__schema.yml)
- **BR-05:** [br05_user_access_traffic.sql](../dbt_semantic/models/br05_user_access_traffic.sql) & [br05_user_access_traffic__schema.yml](../dbt_semantic/models/br05_user_access_traffic__schema.yml)
- **BR-06:** [br06_board_objectives.sql](../dbt_semantic/models/br06_board_objectives.sql) & [br06_board_objectives__schema.yml](../dbt_semantic/models/br06_board_objectives__schema.yml)

---

## 4. HƯỚNG DẪN THỰC THI CHI TIẾT (STEP-BY-STEP COMMANDS)

### Bước 1: Kích hoạt môi trường và kiểm tra cấu hình
Mở terminal PowerShell tại thư mục:
`D:\dataguystory\coding-interview-university\working\crawler-prefecthq-02\crawler-prefecthq`

```powershell
# 1.1. Thiết lập biến môi trường chạy nội bộ
$env:PYTHONPATH = "D:\dataguystory\coding-interview-university\working\crawler-prefecthq-02\crawler-prefecthq"
$env:SPARK_LOCAL_IP = "127.0.0.1"

# 1.2. Kiểm tra tính hợp lệ của cấu hình config.json và biến môi trường
python test_env.py
```

### Bước 2: Đẩy Deployments lên Prefect Server
```powershell
# Khởi tạo và đồng bộ các deployment theo lịch trình vào Prefect HQ
prefect deploy --all
```

### Bước 3: Chạy Crawl Dữ liệu thủ công (Ad-hoc Execution)

**Kịch bản 3.1: Chạy cào tăng dần bảng Tasks (Incremental)**
```powershell
python prefect_flow.py --endpoint tasks --mode incremental --env prod
```

**Kịch bản 3.2: Chạy cào toàn bộ bảng Objectives và Assignments (Full Sync)**
```powershell
python prefect_flow.py --endpoint bsc --mode full --env prod
python prefect_flow.py --endpoint c_assignments --mode full --env prod
```

**Kịch bản 3.3: Chạy Master DAG điều phối toàn bộ 5 bảng theo sóng phụ thuộc**
```powershell
python prefect_flow.py --all --mode incremental --env prod
```

### Bước 4: Chạy Pipeline Spark Bronze -> Silver (Làm sạch & Khử trùng lặp)
```powershell
# Chuyển đổi toàn bộ các bảng sang Silver
python spark/bronze_to_silver.py --table all --spark-master "local[4]"

# Hoặc chuyển đổi riêng lẻ từng bảng
python spark/bronze_to_silver.py --table tasks --spark-master "local[4]"
python spark/bronze_to_silver.py --table projects --spark-master "local[4]"
```

### Bước 5: Chạy Pipeline Spark Silver -> Gold (Xây dựng 6 Báo cáo Nghiệp vụ)
```powershell
python spark/silver_to_gold.py --spark-master "local[4]"
```

### Bước 6: Khởi tạo View trong Trino / Spark SQL
- **Với Spark SQL / Apache Zeppelin:**
  Thực thi nội dung file `generated_ddl/all_6_business_views_spark.sql` qua notebook `%spark.sql` hoặc spark-sql CLI.
- **Với Trino CLI:**
  ```powershell
  trino --server http://localhost:8080 --catalog hive --schema bi_gold -f generated_ddl/all_6_business_views_trino.sql
  ```

### Bước 7: Biên dịch và Xác thực mô hình dbt
```powershell
cd D:\dataguystory\coding-interview-university\working\nextgen-bi-dbt\nextgen-bi-dbt\dbt_projects\epm
dbt compile
dbt test --select tag:epm
```

---

## 5. LÀM RÕ CƠ CHẾ HOẠT ĐỘNG: APACHE LIVY vs PREFECT HQ

Hệ thống tuân thủ nguyên tắc tách biệt tải xử lý (Separation of Concerns):
1. **Prefect HQ**:
   - Chịu trách nhiệm hoàn toàn về điều phối lịch trình, cào API Clarizen, ghi Parquet, và gọi các batch Spark ETL thông qua `SparkSessionFactory` cục bộ hoặc Spark submit.
   - **Tuyệt đối không sử dụng Apache Livy cho các tác vụ Prefect** để tránh việc xếp hàng (queue starvation) hoặc mất kết nối HTTP dài hạn khi cào khối lượng lớn.
2. **Apache Livy**:
   - Được dành riêng cho người dùng tương tác trực tiếp (Interactive Exploration) trên giao diện **Apache Zeppelin**.
   - Data Analysts và Data Scientists sử dụng Livy session (`%livy.pyspark`, `%spark.sql`) để chạy truy vấn phân tích đột xuất hoặc kiểm thử trực quan trên notebook mà không ảnh hưởng đến pipeline sản xuất của DE.

---

## 6. QUY TRÌNH PHỤC HỒI KHI GẶP SỰ CỐ (TROUBLESHOOTING & RECOVERY)

1. **Khắc phục đứt đoạn mạng hoặc chạm trần Rate Limit:**
   - Hệ thống tự động kích hoạt Circuit Breaker (ngắt sau 5 lỗi liên tiếp, phục hồi sau 60 giây).
   - Tiến trình ghi nhận checkpoint tại `./checkpoints/{endpoint_name}.json`. Khi chạy lại lệnh cào, hệ thống sẽ tự động tiếp tục từ trang và offset bị gián đoạn, không cào lại từ đầu.
2. **Khắc phục lỗi trôi lệch số liệu (Data Drift / Silent Deletes):**
   - Chạy lệnh Full Sync vào cuối tuần:
     ```powershell
     python prefect_flow.py --endpoint tasks --mode full --env prod
     python spark/bronze_to_silver.py --table tasks
     python spark/silver_to_gold.py
     ```
3. **Truy vấn bản ghi lỗi (DLQ - Dead Letter Queue):**
   - Các bản ghi sai schema hoặc parse JSON thất bại được lưu tại `data/dlq/{endpoint_name}/`.
   - DE có thể mở file JSON trong DLQ để kiểm tra payload gốc mà API trả về mà không làm gián đoạn dòng dữ liệu chính.
