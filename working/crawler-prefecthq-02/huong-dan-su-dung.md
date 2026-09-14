Searched for files: *
Listed directory crawler-prefecthq
Listed directory epm
Listed directory models
Viewed epm_targets.sql:1-3
Viewed epm_targets__schema.yml:1-100
Viewed epm_targets__schema.yml:180-266
Viewed prefect.yaml:1-45
Viewed config.json:1-100
Listed directory docs
Ran command: `git -C "D:\dataguystory\coding-interview-university\working\crawler-prefecthq" status`
Listed directory transform
Viewed schema_contract.py:1-100
Viewed schema_contract.py:101-214
Listed directory data_type
Listed directory generated_ddl
Listed directory spark
Viewed silver_to_gold.py
Viewed bronze_to_silver.py
Viewed prefect_flow.py:1-100
Searched for "def "
Viewed prefect_flow.py:101-250
Viewed prefect_flow.py:800-1048
Searched for ""name":"
Searched for "typeName"
Viewed config.json:200-300
Viewed config.json:320-400
Viewed config.json:800-950
Viewed config.json:951-1198
Viewed all_tables_mapped.yml:1-100
Viewed 6_ma_tran_khoa_ngoai.yml:1-100
Searched for "  - name: "
Viewed tables_registry.json:1-100
Viewed extractor.py:1-120
Viewed extractor.py:121-160
Listed directory crawler-prefecthq
Listed directory epm_documentation
Viewed DATA_WAREHOUSE_CODE_MAP.md:1-88
Viewed extractor.py:150-250
Viewed extractor.py:251-300
Viewed prefect_flow.py:730-800
Viewed target_dataType.sql:1-100
Viewed task_dataType.sql:1-100
Viewed project_dataType.sql:1-60
Viewed prefect.yaml:1-45
Created D:\dataguystory\coding-interview-university\working\crawler-prefecthq-02\crawler-prefecthq\prefect.yaml
Created prefect.yaml
Viewed spark_session.py:1-96
Created bronze_to_silver.py
Created silver_to_gold.py
Created all_6_business_views_spark.sql
Created all_6_business_views_trino.sql
Created br01_bsc_yearly.sql
Created br01_bsc_yearly__schema.yml
Created br02_dieu_hanh_cvct_klcd.sql
Created br02_dieu_hanh_cvct_klcd__schema.yml
Created br03_task_report.sql
Created br03_task_report__schema.yml
Created br04_project_report.sql
Created br04_project_report__schema.yml
Created br05_user_access_traffic.sql
Created br05_user_access_traffic__schema.yml
Created br06_board_objectives.sql
Created br06_board_objectives__schema.yml
Created DE_IMPLEMENTATION_GUIDE.md
Created CRAWL_STRATEGY_SPEC.md
Created DE_VS_DA_SEMANTIC_CONTRACT.md

Chào bạn,

Tôi đã hoàn tất việc thiết kế, xây dựng mã nguồn, đồng bộ cấu hình và tài liệu hóa toàn bộ nội dung mà chúng ta đã thống nhất qua hai cuộc thảo luận. Mọi nội dung đã được lưu trữ trực tiếp và đầy đủ tại thư mục:  
📁 `D:\dataguystory\coding-interview-university\working\crawler-prefecthq-02\crawler-prefecthq`

---

### 1. Phân định Giới hạn Nghiệp vụ: Data Engineer (DE) vs Data Analyst (DA)

Tuân thủ nghiêm ngặt nguyên tắc của bạn:
* **Tại tầng Kho dữ liệu vật lý (Data Warehouse Views & Materialized Tables):** Data Engineer **tuyệt đối không tự ý suy diễn hoặc tính sẵn các cột chỉ số nghiệp vụ** (như `achievement_rate`, `gap`, `is_overdue`, `health_score`, `work_efficiency`). Các View/Table chỉ chứa **chính xác và duy nhất các cột dữ liệu gốc/chuẩn hóa** theo đúng đặc tả của 6 Business Requirements (BRs).
* **Tại tầng ngữ nghĩa (dbt Semantic Layer & GenBI Metrics):** Toàn bộ các công thức tính toán, tỷ lệ hoàn thành, khoảng cách GAP, cờ cảnh báo quá hạn được định nghĩa tập trung tại `meta.semantic_guidance` và `metrics:` trong các file schema dbt (`.yml`). Trợ lý GenBI và các công cụ BI sẽ đọc các metadata này để sinh câu lệnh SQL động theo ngữ cảnh truy vấn của người dùng.

---

### 2. Triển khai Chiến lược Crawl Dữ liệu vào Code & Cấu hình

Chiến lược crawl tối ưu đã được chuyển hóa thành các deployment độc lập trong [`prefect.yaml`](file:///D:/dataguystory/coding-interview-university/working/crawler-prefecthq-02/crawler-prefecthq/prefect.yaml):

| Thực thể (Table) | Tần suất Crawl | Chế độ (Mode) | Trường Lọc / Watermark | Cron Expression (Prefect HQ) | Tên Deployment đã cấu hình |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Tasks** | 2 tiếng / lần (08:00 - 18:00, T2-T6) | **Incremental** (Lookback 15m) | `LastUpdatedOn` | `0 8-18/2 * * 1-5` | `epm-tasks-incremental-2h` |
| **Tasks (Weekly)** | 1 lần / tuần (Chủ Nhật 23:00) | **Full Sync** (Reconciliation) | Không (Full Scan) | `0 23 * * 0` | `epm-tasks-full-weekly` |
| **Projects** | 2 lần / ngày (12:00 trưa & 18:30 tối) | **Incremental** (Lookback 30m) | `LastUpdatedOn` | Trưa: `0 12 * * 1-5`<br>Tối: `30 18 * * 1-5` | `epm-projects-targets-midday`<br>`epm-projects-targets-evening` |
| **Targets** | 2 lần / ngày (12:15 trưa & 18:45 tối) | **Incremental** (Lookback 30m) | `LastUpdatedOn` | Trưa: `15 12 * * 1-5`<br>Tối: `45 18 * * 1-5` | `epm-targets-midday`<br>`epm-targets-evening` |
| **Objectives (BSC)** | 1 lần / ngày (01:00 AM) | **Full Sync** | Không (Full Scan) | `0 1 * * *` | `epm-objectives-full-daily` |
| **Assignments (PGNV)**| 1 lần / ngày (01:30 AM) | **Full Sync** | Không (Full Scan) | `30 1 * * *` | `epm-assignments-full-daily` |
| **User Access Log** | 1 lần / ngày (02:00 AM) | **Incremental** (T-1) | `login_date` | `0 2 * * *` | `epm-access-log-daily` |
| **Master Orchestration**| 1 lần / ngày (03:00 AM) | **DAG Waves** | Theo cây phụ thuộc | `0 3 * * *` | `epm-all-endpoints-dag-master` |

---

### 3. Danh mục Mã nguồn & Tài liệu Mới đã Bổ sung

```
crawler-prefecthq-02/crawler-prefecthq/
├── prefect.yaml                               # Đã cập nhật 8 deployments theo lịch trình crawl
├── spark/
│   ├── bronze_to_silver.py                    # ETL làm sạch, conform schema & dedup sysid
│   └── silver_to_gold.py                      # ETL tạo 6 bảng Gold chuẩn hóa (KHÔNG cột suy diễn)
├── generated_ddl/
│   ├── all_6_business_views_spark.sql         # DDL Spark SQL (CREATE OR REPLACE VIEW) cho Zeppelin
│   └── all_6_business_views_trino.sql         # DDL Trino SQL (CREATE OR REPLACE VIEW) cho BI / dbt
├── dbt_semantic/models/                       # 6 Models dbt & Schema YAML định nghĩa Semantic Metrics
│   ├── br01_bsc_yearly.sql & .yml             # Báo cáo BSC trong năm (17 cột chuẩn + metrics M/N/GAP)
│   ├── br02_dieu_hanh_cvct_klcd.sql & .yml    # Báo cáo CVCT/KLCĐ (8 cột chuẩn + metrics Overdue/Progress)
│   ├── br03_task_report.sql & .yml            # Báo cáo Task (15 cột chuẩn + metrics Backlog/Delay)
│   ├── br04_project_report.sql & .yml         # Báo cáo Dự án (11 cột chuẩn + metrics Portfolio Progress)
│   ├── br05_user_access_traffic.sql & .yml    # Báo cáo Lưu lượng truy cập (7 cột chuẩn + metrics DAU)
│   └── br06_board_objectives.sql & .yml       # Báo cáo Mục tiêu Ban Giám đốc (17 cột chuẩn + metrics BG)
└── docs/
    ├── DE_IMPLEMENTATION_GUIDE.md             # Sổ tay vận hành & lệnh thực thi từng bước
    ├── CRAWL_STRATEGY_SPEC.md                 # Đặc tả chi tiết cơ chế cào, buffer, checkpoint
    └── DE_VS_DA_SEMANTIC_CONTRACT.md          # Hợp đồng phân định trách nhiệm DE vs DA
```

---

### 4. Hướng dẫn Các Bước Bạn Cần Thực Hiện (Execution Steps)

Để chạy và kiểm thử toàn bộ hệ thống trên môi trường máy của bạn, hãy mở terminal PowerShell tại thư mục `crawler-prefecthq-02\crawler-prefecthq` và gõ các lệnh sau:

#### Bước 1: Thiết lập môi trường
```powershell
$env:PYTHONPATH = "D:\dataguystory\coding-interview-university\working\crawler-prefecthq-02\crawler-prefecthq"
$env:SPARK_LOCAL_IP = "127.0.0.1"
```

#### Bước 2: Đẩy Deployments lên Prefect HQ
```powershell
prefect deploy --all
```

#### Bước 3: Chạy Crawl Dữ liệu thử nghiệm (Ad-hoc)
```powershell
# Cào Tasks tăng dần
python prefect_flow.py --endpoint tasks --mode incremental --env prod

# Cào Objectives & Assignments toàn bảng (Full)
python prefect_flow.py --endpoint bsc --mode full --env prod
python prefect_flow.py --endpoint c_assignments --mode full --env prod
```

#### Bước 4: Chạy chuyển đổi Spark Bronze -> Silver (Làm sạch & Khử trùng lặp)
```powershell
python spark/bronze_to_silver.py --table all --spark-master "local[4]"
```

#### Bước 5: Chạy chuyển đổi Spark Silver -> Gold (Xây dựng 6 Bảng Nghiệp vụ)
```powershell
python spark/silver_to_gold.py --spark-master "local[4]"
```

#### Bước 6: Tạo View trên Trino hoặc Spark SQL (Zeppelin)
* **Với Trino CLI:**
  ```powershell
  trino --server http://localhost:8080 --catalog hive --schema bi_gold -f generated_ddl/all_6_business_views_trino.sql
  ```
* **Với Apache Zeppelin:** Sử dụng notebook `%spark.sql` và chạy nội dung file [`generated_ddl/all_6_business_views_spark.sql`](file:///D:/dataguystory/coding-interview-university/working/crawler-prefecthq-02/crawler-prefecthq/generated_ddl/all_6_business_views_spark.sql).

---

### 5. Làm Rõ Cơ chế Hoạt động: Apache Livy vs Prefect HQ

1. **Prefect HQ**: Đóng vai trò là tổng chỉ huy Orchestration:
   * Quản lý Trigger theo lịch trình cron (2h/lần, 2 lần/ngày, 1 lần/ngày).
   * Điều khiển luồng cào API -> lưu raw Parquet tại MinIO -> gọi các batch Spark ETL thông qua `SparkSessionFactory` nội bộ hoặc `spark-submit`.
   * **Không đi qua Livy** để tránh nghẽn hàng đợi (queue starvation) hoặc timeout phiên HTTP khi xử lý batch lớn.
2. **Apache Livy**:
   * Dành riêng cho giao diện tương tác **Apache Zeppelin** phục vụ các Data Analyst và Data Scientist.
   * Dùng để chạy truy vấn thăm dò ad-hoc (`%livy.pyspark`, `%spark.sql`) trực tiếp trên cụm dữ liệu mà không ảnh hưởng đến pipeline ETL tự động của Data Engineer.

Bạn có thể xem toàn bộ hướng dẫn vận hành chi tiết tại:  
📖 [`working/crawler-prefecthq-02/crawler-prefecthq/docs/DE_IMPLEMENTATION_GUIDE.md`](file:///D:/dataguystory/coding-interview-university/working/crawler-prefecthq-02/crawler-prefecthq/docs/DE_IMPLEMENTATION_GUIDE.md).