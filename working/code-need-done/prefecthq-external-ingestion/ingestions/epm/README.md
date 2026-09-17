# Planview Clarizen Enterprise Lakehouse Pipeline (`crawler-prefecthq-02`)

> **Hệ thống Thu thập, Làm sạch và Xây dựng Kho Dữ liệu Doanh nghiệp (EPM Data Lakehouse)**  
> **Môi trường kỹ thuật:** Python 3.7.1 | Apache Spark 2.3.2 | Ambari HDFS (Knox Gateway) | MinIO S3 | Trino / Hive Metastore | dbt Core 1.3+  
> **Kiến trúc dữ liệu:** Medallion Architecture (Bronze -> Silver Base -> Silver Kimball Dims/Facts -> Gold Marts -> dbt Semantic Layer)

---

## 1. TỔNG QUAN HỆ THỐNG

Dự án này chịu trách nhiệm thu thập dữ liệu từ **Planview Clarizen REST API v2.0**, xử lý làm sạch, khử trùng lặp và chuyển đổi qua các tầng dữ liệu:
* **Bronze Layer:** Lưu trữ Parquet thô kèm audit JSON gốc trên MinIO và Ambari HDFS.
* **Silver Layer (Base & Kimball):** Làm sạch, ép kiểu contract chuẩn, khử trùng lặp khóa chính `sysid`, và xây dựng 7 Conformed Dimensions cùng 5 Facts Periodic Snapshot theo mô hình Kimball.
* **Gold Layer (Business Marts):** Xây dựng 6 Bảng dữ liệu nghiệp vụ phục vụ 6 Business Requirements (BR-01 đến BR-06), tuân thủ nguyên tắc không chứa cột tính toán suy diễn.
* **Semantic Layer (dbt & Lightdash):** Định nghĩa toàn bộ metrics kinh doanh (tỷ lệ hoàn thành M/N, GAP, cờ quá hạn, giờ công tồn đọng, DAU) phục vụ Trợ lý GenBI và Dashboard.

---

## 2. TÀI LIỆU KỸ THUẬT & HƯỚNG DẪN CHI TIẾT

Toàn bộ tài liệu chi tiết được lưu trữ tại thư mục [`docs/`](docs/):
* 📖 **[Sổ Tay Kiến Trúc & Vận Hành Toàn Diện (MASTER RUNBOOK)](docs/MASTER_PIPELINE_ARCHITECTURE_AND_RUNBOOK.md):** Hướng dẫn đầy đủ nhất từ crawl, lưu trữ Ambari Knox, luồng xử lý Bronze to Silver, Kimball Dims/Facts, Gold Marts đến dbt Semantic metrics.
* 📄 **[Hợp Đồng Phân Định Trách Nhiệm DE vs DA](docs/DE_VS_DA_SEMANTIC_CONTRACT.md):** Quy chuẩn về ranh giới dữ liệu vật lý của DE và tầng ngữ nghĩa metrics của DA.
* ⚙️ **[Đặc Tả Kỹ Thuật Chiến Lược Crawl API](docs/CRAWL_STRATEGY_SPEC.md):** Chi tiết lịch trình, hạn ngạch, phân trang, bộ lọc watermark và cơ chế Checkpoint.
* 🛠️ **[Sổ Tay Triển Khai Kỹ Thuật (DE Implementation Guide)](docs/DE_IMPLEMENTATION_GUIDE.md):** Chi tiết cấu hình và lệnh chạy CLI.

---

## 3. BẢN ĐỒ CẤU TRÚC MÃ NGUỒN (SOURCE CODE MAP)

```
crawler-prefecthq-02/crawler-prefecthq/
├── config.json                               # Cấu hình 5 API endpoints, auth, watermark lookback, limits
├── prefect.yaml                              # Khai báo 8 scheduled deployments độc lập và DAG tổng thể
├── prefect_flow.py                           # Flow chính Prefect điều phối cào API và lưu checkpoint
├── common/
│   ├── ambari_client.py                      # Client HDFS qua Ambari Knox Gateway REST API
│   └── check_env.py                          # Kiểm tra biến môi trường và kết nối
├── data_type/                                # Hợp đồng schema cho 5 thực thể Clarizen
│   ├── task_dataType.sql
│   ├── project_dataType.sql
│   ├── target_dataType.sql
│   ├── objective_dataType.sql
│   └── c_assignment_dataType.sql
├── transform/                                # Module xử lý dữ liệu Spark
│   ├── schema_contract.py                    # Engine nạp contract, ép kiểu và sinh StructType
│   ├── dedup_engine.py                       # Khử trùng lặp khóa chính sysid + last_updated_on
│   └── spark_session.py                      # Factory tạo SparkSession cho Ambari YARN / Local
├── spark/                                    # Các Job ETL cốt lõi
│   ├── bronze_to_silver.py                   # Job chuyển đổi Bronze thô -> Silver Base conformed
│   ├── build_silver_dims_facts.py            # Job xây dựng 7 Dims & 5 Facts (Kimball Model)
│   └── silver_to_gold.py                     # Job tổng hợp 6 Bảng Gold Business Marts
├── generated_ddl/                            # DDL khởi tạo Views
│   ├── all_6_business_views_spark.sql        # Views Spark SQL cho Apache Zeppelin
│   └── all_6_business_views_trino.sql        # Views Trino SQL cho BI Tools
├── dbt_semantic/models/                      # 6 Mô hình dbt (.sql) & Semantic Schema (.yml)
│   ├── br01_bsc_yearly.*                     # Báo cáo BSC trong năm & metrics M/N/GAP
│   ├── br02_dieu_hanh_cvct_klcd.*            # Báo cáo CVCT/KLCĐ & metrics trễ hạn
│   ├── br03_task_report.*                    # Báo cáo Task & metrics giờ công tồn đọng
│   ├── br04_project_report.*                 # Báo cáo Dự án & metrics danh mục
│   ├── br05_user_access_traffic.*            # Báo cáo Lưu lượng truy cập & metrics DAU
│   └── br06_board_objectives.*               # Báo cáo Mục tiêu Ban Giám đốc & metrics BG
└── docs/                                     # Toàn bộ tài liệu kỹ thuật dự án
```

---

## 4. HƯỚNG DẪN VẬN HÀNH NHANH (QUICK RUNBOOK)

Mở PowerShell tại thư mục dự án và thực hiện tuần tự:

```powershell
# 1. Thiết lập môi trường và cấu hình Ambari Knox Gateway
$env:PYTHONPATH = "D:\dataguystory\coding-interview-university\working\crawler-prefecthq-02\crawler-prefecthq"
$env:AMBARI_USERNAME = "<your_username>"
$env:AMBARI_PASSWORD = "<your_password>"
$env:AMBARI_FILES_API = "https://datalake.viettelcyber.com/gateway/ui/ambari/api/v1/views/FILES/versions/1.0.0/instances/FILES/resources/files"

# 2. Đẩy cấu hình lập lịch lên Prefect HQ
prefect deploy --all

# 3. Kích hoạt cào dữ liệu vào Bronze
python prefect_flow.py --all --mode incremental --env prod

# 4. Chuyển đổi Bronze -> Silver Base (Làm sạch & Khử trùng lặp)
python spark/bronze_to_silver.py --table all --spark-master "local[4]"

# 5. Xây dựng 7 Dimensions & 5 Facts (Kimball Model - Phương án B)
python spark/build_silver_dims_facts.py --spark-master "local[4]"

# 6. Xây dựng 6 Bảng Gold Business Marts
python spark/silver_to_gold.py --spark-master "local[4]"

# 7. Biên dịch và kiểm thử dbt Semantic Layer
cd D:\dataguystory\coding-interview-university\working\nextgen-bi-dbt\nextgen-bi-dbt\dbt_projects\epm
dbt compile
dbt test --select tag:epm
```
