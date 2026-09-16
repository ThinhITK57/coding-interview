# TÀI LIỆU KIẾN TRÚC TOÀN DIỆN & SỔ TAY VẬN HÀNH PIPELINE EPM
## (MASTER PIPELINE ARCHITECTURE & OPERATIONAL RUNBOOK)

> **Dự án:** Planview Clarizen Enterprise Lakehouse Data Platform (`crawler-prefecthq-02`)  
> **Môi trường kỹ thuật:** Python 3.7.1 | Apache Spark 2.3.2 | Ambari HDFS (Knox Gateway) | MinIO S3 | Trino / Hive Metastore | dbt Core 1.3+  
> **Mô hình kiến trúc:** Medallion Architecture (Bronze -> Silver Base -> Silver Kimball Dims/Facts -> Gold Marts -> dbt Semantic Layer)  
> **Vị trí tài liệu:** `working/crawler-prefecthq-02/crawler-prefecthq/docs/MASTER_PIPELINE_ARCHITECTURE_AND_RUNBOOK.md`  

---

## MỤC LỤC
1. [Tổng Quan Kiến Trúc Dòng Chảy Dữ Liệu (End-to-End Data Flow)](#1-tổng-quan-kiến-trúc-dòng-chảy-dữ-liệu-end-to-end-data-flow)
2. [Giai Đoạn 1: Chiến Lược Crawl Dữ Liệu Chi Tiết (Clarizen API Ingestion)](#2-giai-đoạn-1-chiến-lược-crawl-dữ-liệu-chi-tiết-clarizen-api-ingestion)
3. [Giai Đoạn 2: Lưu Trữ Bronze vào MinIO / Ambari HDFS & Quản Lý Metastore trên Trino](#3-giai-đoạn-2-lưu-trữ-bronze-vào-minio--ambari-hdfs--quản-lý-metastore-trên-trino)
4. [Giai Đoạn 3: Luồng Xử Lý Chi Tiết Từ Bronze Sang Silver (Deep-Dive Bronze-to-Silver)](#4-giai-đoạn-3-luồng-xử-lý-chi-tiết-từ-bronze-sang-silver-deep-dive-bronze-to-silver)
5. [Giai Đoạn 4: Chuyển Đổi Silver Sang Dimensions, Facts & Xây Dựng Gold Marts (Phương Án B)](#5-giai-đoạn-4-chuyển-đổi-silver-sang-dimensions-facts--xây-dựng-gold-marts-phương-án-b)
6. [Giai Đoạn 5: Tầng Ngữ Nghĩa dbt Final Với Đầy Đủ Metrics Cho GenBI & BI Tools](#6-giai-đoạn-5-tầng-ngữ-nghĩa-dbt-final-với-đầy-đủ-metrics-cho-genbi--bi-tools)
7. [Bản Đồ Cấu Trúc Mã Nguồn (Source Code Map & File Locations)](#7-bản-đồ-cấu-trúc-mã-nguồn-source-code-map--file-locations)
8. [Sổ Tay Lệnh Thực Thi Vận Hành Từng Bước (Operational Runbook CLI)](#8-sổ-tay-lệnh-thực-thi-vận-hành-từng-bước-operational-runbook-cli)

---

## 1. TỔNG QUAN KIẾN TRÚC DÒNG CHẢY DỮ LIỆU (END-TO-END DATA FLOW)

Hệ thống được thiết kế theo mô hình **Medallion Architecture 5 tầng** khép kín, đảm bảo tính phân ly tải trọng, toàn vẹn dữ liệu và tách bạch hoàn toàn giữa Data Engineer (vật lý) và Data Analyst (ngữ nghĩa):

```
┌──────────────────────────────────────────────────────────────────────────────────────────────────┐
│ GIAI ĐOẠN 1: CRAWL DỮ LIỆU TỪ PLANVIEW CLARIZEN REST API v2.0                                    │
│ [tasks] [projects] [targets] [objectives/bsc] [c_assignments] [user_access_log]                  │
│ Bộ điều phối: Prefect HQ Flow (prefect_flow.py) + RateLimiter + Paginator + CheckpointStore      │
└────────────────────────────────┬─────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌──────────────────────────────────────────────────────────────────────────────────────────────────┐
│ GIAI ĐOẠN 2: LƯU TRỮ TẦNG BRONZE (RAW STORAGE & AUDIT)                                           │
│ Local Staging: ./data/staging/<source>/<endpoint>/                                               │
│ MinIO S3:      s3a://lakehouse/bronze/clarizen/<endpoint>/                                       │
│ Ambari HDFS:   https://datalake.viettelcyber.com/gateway/ui/ambari/... qua common/ambari_client.py│
│ Quản lý:       Trino External Tables (catalog: hive, schema: personal_raw)                       │
│ Định dạng:     Raw Parquet + _raw_payload + _batch_id + _ingest_timestamp                        │
└────────────────────────────────┬─────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼ (spark/bronze_to_silver.py)
┌──────────────────────────────────────────────────────────────────────────────────────────────────┐
│ GIAI ĐOẠN 3: LÀM SẠCH, ÉP KIỂU & KHỬ TRÙNG LẶP (SILVER BASE ENTITIES)                            │
│ 1. Schema Contract Enforcement (data_type/<table_name>_dataType.sql)                             │
│ 2. Primary Key Deduplication via DedupEngine (Nhóm sysid, lấy max(last_updated_on))              │
│ 3. Phân vùng ingest_date, nén Snappy Parquet                                                     │
│ HDFS Output:   /user/lakehouse/silver/epm/epm_<table_name>/                                      │
│ Quản lý:       Trino External Tables (catalog: hive, schema: bi_silver)                          │
└────────────────────────────────┬─────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼ (spark/build_silver_dims_facts.py - PHƯƠNG ÁN B)
┌──────────────────────────────────────────────────────────────────────────────────────────────────┐
│ GIAI ĐOẠN 4A: XÂY DỰNG MÔ HÌNH KIMBALL (SILVER CONFORMED DIMS & FACTS)                           │
│ 7 Dimensions:  dim_date, dim_department, dim_resource, dim_project (SCD 2), dim_task,            │
│                dim_objective, dim_assignment                                                     │
│ 5 Facts:       fact_target_bsc_snapshot, fact_cvct_execution_snapshot,                           │
│                fact_task_execution_snapshot, fact_project_progress_snapshot, fact_epm_user_access │
└────────────────────────────────┬─────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼ (spark/silver_to_gold.py & generated_ddl/*.sql)
┌──────────────────────────────────────────────────────────────────────────────────────────────────┐
│ GIAI ĐOẠN 4B: XÂY DỰNG TẦNG BÁO CÁO NGHIỆP VỤ (GOLD BUSINESS MARTS)                              │
│ Bảng/Views:    br01_bsc_yearly, br02_dieu_hanh_cvct_klcd, br03_task_report,                      │
│                br04_project_report, br05_user_access_traffic, br06_board_objectives               │
│ Ranh giới DE:  CHỈ CHỨA CÁC CỘT DỮ LIỆU GỐC THEO YÊU CẦU, ZERO CỘT SUY DIỄN (NO DERIVED METRICS) │
│ HDFS Output:   /user/lakehouse/gold/epm/br0*                                                     │
│ Trino Schema:  hive.bi_gold.vw_br0*                                                              │
└────────────────────────────────┬─────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼ (dbt_projects/epm/models/*.sql & *__schema.yml)
┌──────────────────────────────────────────────────────────────────────────────────────────────────┐
│ GIAI ĐOẠN 5: TẦNG NGỮ NGHĨA METRICS CHO GenBI & LIGHTDASH (dbt SEMANTIC LAYER)                   │
│ - Chuẩn CRM:   semantic_guidance (metric_rules, restrictions) + meta.metrics                     │
│ - Công thức:   achievement_rate_m, gap_m, overdue_flag, progress_gap, remaining_work, DAU...     │
│ - Thực thi:    GenBI và BI Tools dịch metadata thành câu truy vấn SQL động gửi tới Trino         │
└──────────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. GIAI ĐOẠN 1: CHIẾN LƯỢC CRAWL DỮ LIỆU CHI TIẾT (CLARIZEN API INGESTION)

### 2.1. Phân Tích Tính Chất 6 Nguồn Dữ Liệu
Clarizen REST API áp dụng hạn ngạch 1.000 requests/ngày. Bảng chiến lược được thiết kế để tiêu thụ tối đa ~120 requests/ngày (dư 88% hạn ngạch dự phòng):

| Nguồn Dữ Liệu (Endpoint) | Tần Suất Biến Động | Chế Độ Crawl (Mode) | Trường Watermark | Lookback Buffer | Tần Suất & Khung Giờ Chạy | Deployment Name trong Prefect |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **`tasks`** | Rất cao (Timesheet, status, %) | **Incremental** | `LastUpdatedOn` | 15 phút | 2 tiếng / lần (08:00 - 18:00, T2-T6) | `epm-tasks-incremental-2h` |
| **`tasks (weekly)`** | Đối soát toàn diện | **Full Sync** | Không | Không | Chủ Nhật lúc 23:00 | `epm-tasks-full-weekly` |
| **`projects`** | Trung bình (Chốt mốc tiến độ) | **Incremental** | `LastUpdatedOn` | 30 phút | 2 lần / ngày (12:00 & 18:30, T2-T6) | `epm-projects-targets-midday`<br>`epm-projects-targets-evening` |
| **`targets`** | Trung bình (Cập nhật giao ban) | **Incremental** | `LastUpdatedOn` | 30 phút | 2 lần / ngày (12:15 & 18:45, T2-T6) | `epm-targets-midday`<br>`epm-targets-evening` |
| **`objectives` (BSC)** | Rất thấp (Cây mục tiêu năm) | **Full Sync** | Không | Không | 1 lần / ngày (01:00 AM) | `epm-objectives-full-daily` |
| **`c_assignments`** | Thấp (Phiếu giao việc KPI) | **Full Sync** | Không | Không | 1 lần / ngày (01:30 AM) | `epm-assignments-full-daily` |
| **`user_access_log`** | Append-only (Nhật ký truy cập) | **Incremental (T-1)**| `login_date` | Lấy trọn ngày hôm trước | 1 lần / ngày (02:00 AM) | `epm-access-log-daily` |
| **Master Ingestion DAG**| Toàn bộ theo sóng phụ thuộc | **DAG Waves** | Theo từng bảng | Theo cấu hình | 1 lần / ngày (03:00 AM) | `epm-all-endpoints-dag-master` |

### 2.2. Cơ Chế Điều Phối & Phục Hồi (Resilience & Checkpoint)
* **File cấu hình:** `config.json` và `prefect.yaml`.
* **Cơ chế Phân trang & Rate Limit:** `pagination.page_size = 250`, trần 60 req/phút, tự động kích hoạt Circuit Breaker khi 5 lần liên tiếp lỗi mạng.
* **Cơ chế Checkpoint (`checkpoint/checkpoint_store.py`):** Mỗi batch cào thành công ghi nhận `last_offset` và `last_watermark` vào `./checkpoints/<endpoint>.json`. Nếu tiến trình bị ngắt, Prefect đọc lại checkpoint và tiếp tục trang dở dang, đảm bảo **At-least-once extraction**.

---

## 3. GIAI ĐOẠN 2: LƯU TRỮ BRONZE VÀO MINIO / AMBARI HDFS & QUẢN LÝ METASTORE TRÊN TRINO

### 3.1. Luồng Lưu Trữ Đa Tầng (Tri-Storage Sink)
Dữ liệu trích xuất từ Clarizen API trải qua quy trình 3 bước lưu trữ:
1. **Local Staging:** Dữ liệu JSON từng trang được ghi tạm tại `./data/staging/clarizen/<endpoint>/batch_<id>_page_<num>.json`.
2. **MinIO S3 Raw Parquet:**
   * Script `storage/tri_storage_sink.py` nén dữ liệu thành Parquet thô và lưu tại: `s3a://lakehouse/bronze/clarizen/<endpoint>/`.
3. **Đẩy lên Ambari HDFS qua Knox Gateway:**
   * Sử dụng client [`common/ambari_client.py`](../common/ambari_client.py).
   * Giao tiếp HTTPS an toàn qua endpoint Ambari Files View API:
     `https://datalake.viettelcyber.com/gateway/ui/ambari/api/v1/views/FILES/versions/1.0.0/instances/FILES/resources/files/upload`
   * Lưu tại thư mục HDFS: `/user/lakehouse/bronze/clarizen/<endpoint>/`.

### 3.2. Đăng Ký Quản Lý Tầng Bronze Trong Hive Metastore & Trino
Các bảng Bronze được đăng ký trong Trino dưới Catalog `hive`, Schema `personal_raw`:
```sql
CREATE TABLE hive.personal_raw.tasks (
    _raw_payload       VARCHAR,
    _batch_id          VARCHAR,
    _ingest_timestamp  TIMESTAMP,
    sysid              VARCHAR,
    lastupdatedon      VARCHAR
)
WITH (
    format = 'PARQUET',
    external_location = 'hdfs://datalake.viettelcyber.com:8020/user/lakehouse/bronze/clarizen/tasks'
);
```

---

## 4. GIAI ĐOẠN 3: LUỒNG XỬ LÝ CHI TIẾT TỪ BRONZE SANG SILVER (DEEP-DIVE BRONZE-TO-SILVER)

### 4.1. Bản Chất Luồng Xử Lý (Data Transformation Flow)
File thực thi chính: [`spark/bronze_to_silver.py`](../spark/bronze_to_silver.py). Quy trình xử lý diễn ra qua 5 bước tuần tự:

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│ BƯỚC 1: NẠP DỮ LIỆU BRONZE & XỬ LÝ BẢN GHI LỖI (_corrupt_record)                       │
│ - Đọc Parquet thô từ /user/lakehouse/bronze/clarizen/<table_name>/                     │
│ - Nếu có bản ghi hỏng JSON -> định tuyến sang DLQ (data/dlq/<table_name>/)             │
└──────────────────────────────────────────┬─────────────────────────────────────────────┘
                                           │
                                           ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│ BƯỚC 2: ÁP DỤNG HỢP ĐỒNG LƯỢC ĐỒ (SCHEMA CONTRACT ENFORCEMENT)                         │
│ - Đọc file contract chuẩn: data_type/<table_name>_dataType.sql                         │
│ - Khởi tạo SchemaContract: ánh xạ tên trường sang snake_case, chuẩn hóa kiểu:          │
│   STRING, DOUBLE, BOOLEAN, DATE, DECIMAL(18,2)                                         │
│ - Ép kiểu tường minh (cast) toàn bộ các cột, gán NULL có kiểu cho cột thiếu            │
└──────────────────────────────────────────┬─────────────────────────────────────────────┘
                                           │
                                           ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│ BƯỚC 3: KHỬ TRÙNG LẶP KHÓA CHÍNH (PRIMARY KEY DEDUPLICATION ENGINE)                     │
│ - Áp dụng transform.dedup_engine.DedupEngine                                           │
│ - Nhóm theo Primary Key sysid                                                          │
│ - Window Partition: OVER (PARTITION BY sysid ORDER BY last_updated_on DESC,            │
│                           _ingest_timestamp DESC)                                      │
│ - Lọc ROW_NUMBER() == 1 để giữ bản ghi mới nhất, đẩy bản ghi trùng sang audit log      │
└──────────────────────────────────────────┬─────────────────────────────────────────────┘
                                           │
                                           ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│ BƯỚC 4: BỔ SUNG CỘT PHÂN VÙNG VÀ METADATA KỸ THUẬT                                     │
│ - Bổ sung cột phân vùng ingest_date = COALESCE(TO_DATE(last_updated_on), CURRENT_DATE) │
│ - Bảo lưu các cột kiểm toán: _raw_payload, _batch_id, _ingest_timestamp                │
└──────────────────────────────────────────┬─────────────────────────────────────────────┘
                                           │
                                           ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│ BƯỚC 5: XUẤT BẢN SILVER PARQUET LÊN AMBARI HDFS & TẠO HIVE EXTERNAL TABLE              │
│ - Ghi Parquet nén Snappy, phân vùng theo ingest_date:                                  │
│   HDFS: /user/lakehouse/silver/epm/epm_<table_name>/                                   │
│ - Cập nhật bảng Hive Metastore: hive.bi_silver.epm_<table_name>                        │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

### 4.2. Input và Output Của Bước Bronze to Silver
* **Input:**
  * File Parquet thô trên HDFS: `/user/lakehouse/bronze/clarizen/<table_name>`
  * File hợp đồng schema: `data_type/<table_name>_dataType.sql`
* **Output:**
  * Thư mục phân vùng HDFS: `/user/lakehouse/silver/epm/epm_<table_name>/ingest_date=YYYY-MM-DD/*.parquet`
  * Bảng Hive/Trino: `hive.bi_silver.epm_<table_name>` (schema chuẩn, đã khử trùng lặp).

---

## 5. GIAI ĐOẠN 4: CHUYỂN ĐỔI SILVER SANG DIMENSIONS, FACTS & XÂY DỰNG GOLD MARTS (PHƯƠNG ÁN B)

Tuân thủ **Phương án B** mà bạn đã phê duyệt: Tầng Silver tiếp tục phân tách thành mô hình **Kimball Constellation (7 Dims & 5 Facts)** trước khi gom thành 6 Bảng Gold Marts.

### 5.1. Xây Dựng 7 Dimensions & 5 Facts (`spark/build_silver_dims_facts.py`)
1. **7 Dimensions conformed:**
   * `dim_date`: Khung ngày 2020–2030 tạo sẵn với đầy đủ năm tài chính, quý tài chính, ngày trong tuần.
   * `dim_department`: Khử trùng lặp và gom toàn bộ phòng ban từ 5 bảng nghiệp vụ.
   * `dim_resource`: Khử trùng lặp danh sách cán bộ, PM, nhân sự từ 8 trường nhân sự nguồn.
   * `dim_project`: Thiết kế theo chuẩn **SCD Type 2** với các trường `valid_from`, `valid_to`, `is_current`, `version`, cho phép lưu vết lịch sử biến động PM hoặc phòng ban phụ trách theo thời gian.
   * `dim_task`: Lưu giữ cấu trúc cây phân cấp công việc WBS và trạng thái tích hợp Jira.
   * `dim_objective`: Cây mục tiêu chiến lược BSC phân cấp (Level 1 Ban Giám đốc, Level 2 Khối/Phòng).
   * `dim_assignment`: Cây phân cấp phiếu giao nhiệm vụ và tổng trọng số KPI.
2. **5 Facts Snapshot:**
   * `fact_target_bsc_snapshot`: Snapshot định kỳ theo ngày lưu giá trị cam kết M/N và kết quả thực tế.
   * `fact_cvct_execution_snapshot`: Snapshot tiến độ công việc trọng tâm / KLCĐ.
   * `fact_task_execution_snapshot`: Snapshot giờ công kế hoạch, thời lượng và tiến độ task.
   * `fact_project_progress_snapshot`: Snapshot tiến độ và hiện trạng dự án.
   * `fact_epm_user_access`: Snapshot hoạt động đăng nhập hàng ngày của người dùng.

### 5.2. Xây Dựng 6 Bảng Báo Cáo Gold Nghiệp Vụ (`spark/silver_to_gold.py`)
Từ các bảng Dimensions và Facts trên, script gom dữ liệu thành 6 Data Marts vật lý lưu trên HDFS `/user/lakehouse/gold/epm/` và tạo Views tương ứng trong Trino `hive.bi_gold.vw_br0*`:
1. **`br01_bsc_yearly` (17 cột):** `associated_objective`, `associated_item`, `target_type`, `parent_target`, `c_department`, `name`, `c_assignee`, `unit`, `target_date_m`, `target_value_m`, `target_date_n`, `target_value_n`, `state`, `status`, `target_result_m`, `target_result_n`, `assignor`.
2. **`br02_dieu_hanh_cvct_klcd` (8 cột):** `assignee`, `resources`, `name`, `status`, `due_date`, `percent_completed`, `target_value_m`, `target_date_m`.
3. **`br03_task_report` (15 cột):** `task_type`, `assignee`, `department`, `resources`, `parent_project`, `jira_status`, `name`, `description`, `work`, `duration`, `epm_default`, `start_date`, `due_date`, `percent_completed`, `update_description`.
4. **`br04_project_report` (11 cột):** `name`, `project_type`, `due_date`, `state`, `status`, `percent_completed`, `department`, `assignor`, `assignee`, `project_manager`, `resources`.
5. **`br05_user_access_traffic` (7 cột):** `login_date`, `name`, `first_name`, `last_name`, `groups`, `direct_manager`, `job_title`.
6. **`br06_board_objectives` (17 cột):** `associated_objective`, `associated_item`, `target_type`, `parent_target`, `c_department`, `name`, `c_assignee`, `unit`, `c_target_date_m`, `c_target_date_n`, `c_target_value_m`, `c_target_value_n`, `c_target_result_m`, `c_target_result_n`, `state`, `status`, `c_assignor`.

> [!IMPORTANT]
> **Ranh Giới Bắt Buộc Của Data Engineer:** Toàn bộ 6 bảng/view Gold vật lý này **TUYỆT ĐỐI KHÔNG chứa các cột tính toán suy diễn** như tỷ lệ hoàn thành, khoảng cách GAP hay cờ quá hạn. Toàn bộ logic đó được bảo lưu nguyên vẹn để tầng dbt Semantic Layer xử lý.

---

## 6. GIAI ĐOẠN 5: TẦNG NGỮ NGHĨA dbt FINAL VỚI ĐẦY ĐỦ METRICS CHO GenBI & BI TOOLS

Toàn bộ 6 mô hình dbt đã được thiết lập tại:
* `working/crawler-prefecthq-02/crawler-prefecthq/dbt_semantic/models/`
* `working/nextgen-bi-dbt/nextgen-bi-dbt/dbt_projects/epm/models/`

### 6.1. Cấu Trúc Khối `semantic_guidance` và `metrics` Chuẩn CRM
Mỗi model bao gồm 1 file `.sql` định nghĩa view và 1 file `__schema.yml` chứa toàn bộ metadata:
```yaml
version: 2

models:
  - name: br01_bsc_yearly
    meta:
      label: "Báo cáo BSC Trong Năm (BR-01)"
      group_label: "Executive EPM Reports"

      semantic_guidance:
        source_type: "Targets joined with Objectives & Assignments"
        grain: "One row per quantitative BSC target"
        primary_date: "target_date_m"
        primary_department_dimension: "c_department"
        metric_rules:
          total_targets_count: "Tổng số lượng chỉ tiêu định lượng BSC."
          achieved_m_targets_count: "Số lượng chỉ tiêu đạt mốc cam kết tối thiểu M."
          overall_achievement_rate_m: "Tỷ lệ hoàn thành mức M: (Tổng result_m / Tổng value_m) * 100%."
        restrictions:
          - "Không tự ý gộp các chỉ tiêu khác đơn vị tính (unit)."

      metrics:
        - name: total_targets_count
          label: "Tổng số chỉ tiêu"
          type: count
          sql: ${name}

        - name: achieved_m_targets_count
          label: "Số chỉ tiêu đạt mốc M"
          type: count
          sql: |
            CASE
              WHEN ${target_result_m} >= ${target_value_m} AND ${target_value_m} > 0
              THEN ${name}
            END

        - name: overall_achievement_rate_m
          label: "Tỷ lệ đạt mục tiêu M (%)"
          type: number
          sql: |
            CASE
              WHEN SUM(COALESCE(${target_value_m}, 0.0)) > 0
              THEN (SUM(COALESCE(${target_result_m}, 0.0)) * 100.0) / SUM(COALESCE(${target_value_m}, 0.0))
              ELSE NULL
            END
```

### 6.2. Cơ Chế GenBI & Lightdash Biên Dịch Truy Vấn Động
Khi người dùng chat với Trợ lý GenBI hoặc kéo thả trên Lightdash:
* **Câu hỏi tự nhiên:** *"Cho tôi biết tỷ lệ hoàn thành mục tiêu M của Khối CNTT trong năm 2026?"*
* **GenBI Parser:** Đọc file `br01_bsc_yearly__schema.yml`, tìm thấy metric `overall_achievement_rate_m` và dimension `c_department`.
* **Trino Query Sinh Ra:**
  ```sql
  SELECT 
      c_department,
      (SUM(COALESCE(target_result_m, 0.0)) * 100.0) / SUM(COALESCE(target_value_m, 0.0)) AS overall_achievement_rate_m
  FROM hive.bi_gold.vw_br01_bsc_yearly
  WHERE c_department = 'Khối CNTT'
  GROUP BY c_department;
  ```

---

## 7. BẢN ĐỒ CẤU TRÚC MÃ NGUỒN (SOURCE CODE MAP & FILE LOCATIONS)

Toàn bộ các file trong dự án được tổ chức chặt chẽ theo chức năng tại thư mục:  
`D:\dataguystory\coding-interview-university\working\crawler-prefecthq-02\crawler-prefecthq`

| Đường Dẫn Tương Đối | Trách Nhiệm Kỹ Thuật | Input / Output |
| :--- | :--- | :--- |
| **`config.json`** | Cấu hình toàn bộ endpoints Clarizen, API key, auth, pagination, rate limit, watermark lookback. | Cấu hình tham số cho Extractor |
| **`prefect.yaml`** | Khai báo 8 scheduled deployments độc lập và DAG tổng thể trên work pool `epm-pool`. | Lập lịch Cron cho Prefect Agent |
| **`prefect_flow.py`** | Flow chính của Prefect điều phối cào API, phân trang, lưu checkpoint, gọi Spark và xuất báo cáo. | API Clarizen $\rightarrow$ Bronze Parquet |
| **`common/ambari_client.py`** | Client giao tiếp Ambari Files View REST API qua Knox Gateway (`datalake.viettelcyber.com`). | Quản lý file/folder HDFS qua HTTPS |
| **`data_type/*_dataType.sql`** | 5 Schema Contracts chuẩn mực định nghĩa kiểu dữ liệu duy nhất cho 5 thực thể API. | Hợp đồng schema cho SchemaContract |
| **`transform/schema_contract.py`** | Đọc file `.sql`, ép kiểu an toàn, sinh StructType cho Spark và DDL cho Trino. | Schema Contract Engine |
| **`transform/dedup_engine.py`** | Khử trùng lặp khóa chính `sysid` dựa trên cửa sổ `last_updated_on` mới nhất. | Deduplication Core |
| **`spark/bronze_to_silver.py`** | Job Spark đọc Bronze HDFS/S3, ép kiểu contract, dedup, ghi Silver Base partitioned Parquet. | Bronze Parquet $\rightarrow$ Silver Base |
| **`spark/build_silver_dims_facts.py`**| Job Spark xây dựng 7 Conformed Dimensions và 5 Snapshot Facts theo mô hình Kimball. | Silver Base $\rightarrow$ Silver Dims/Facts |
| **`spark/silver_to_gold.py`** | Job Spark tổng hợp 6 Bảng Gold Business Marts từ tầng Facts/Dims (Zero derived columns). | Silver Facts/Dims $\rightarrow$ Gold Marts |
| **`generated_ddl/all_6_business_views_spark.sql`** | File DDL Spark SQL tạo 6 Views `bi_gold.vw_br0*` trên Apache Zeppelin. | Spark SQL Views |
| **`generated_ddl/all_6_business_views_trino.sql`** | File DDL Trino SQL tạo 6 Views `hive.bi_gold.vw_br0*` cho BI Tools. | Trino / Hive Views |
| **`dbt_semantic/models/br0*`** | 6 models dbt (.sql) và schema (.yml) định nghĩa đầy đủ semantic metrics chuẩn CRM. | dbt Semantic Models |
| **`docs/`** | Thư mục tài liệu kiến trúc, hợp đồng DE vs DA, đặc tả crawl và sổ tay vận hành. | Toàn bộ tài liệu dự án |

---

## 8. SỔ TAY LỆNH THỰC THI VẬN HÀNH TỪNG BƯỚC (OPERATIONAL RUNBOOK CLI)

Mở terminal PowerShell tại thư mục:  
`D:\dataguystory\coding-interview-university\working\crawler-prefecthq-02\crawler-prefecthq`

### Bước 1: Thiết lập môi trường và cấu hình Ambari Knox Gateway
```powershell
# Thiết lập đường dẫn module
$env:PYTHONPATH = "D:\dataguystory\coding-interview-university\working\crawler-prefecthq-02\crawler-prefecthq"
$env:SPARK_LOCAL_IP = "127.0.0.1"

# Khai báo thông tin xác thực Ambari Knox Gateway
$env:AMBARI_USERNAME = "<your_ambari_username>"
$env:AMBARI_PASSWORD = "<your_ambari_password>"
$env:AMBARI_FILES_API = "https://datalake.viettelcyber.com/gateway/ui/ambari/api/v1/views/FILES/versions/1.0.0/instances/FILES/resources/files"
```

### Bước 2: Đẩy Deployments lên Prefect HQ Server
```powershell
prefect deploy --all
```

### Bước 3: Kích hoạt Crawl Dữ liệu vào Bronze
```powershell
# Cào Tasks tăng dần (Incremental 2h/lần)
python prefect_flow.py --endpoint tasks --mode incremental --env prod

# Cào Objectives & Assignments toàn bảng (Full Daily)
python prefect_flow.py --endpoint bsc --mode full --env prod
python prefect_flow.py --endpoint c_assignments --mode full --env prod

# Hoặc kích hoạt Master DAG chạy tất cả 5 bảng theo sóng phụ thuộc:
python prefect_flow.py --all --mode incremental --env prod
```

### Bước 4: Chuyển đổi Bronze -> Silver Base Entities (Làm sạch & Khử trùng lặp)
```powershell
python spark/bronze_to_silver.py --table all --spark-master "local[4]"
```

### Bước 5: Xây dựng 7 Dimensions & 5 Facts (Kimball Model - Phương án B)
```powershell
python spark/build_silver_dims_facts.py --spark-master "local[4]"
```

### Bước 6: Xây dựng 6 Bảng Gold Business Marts từ Facts/Dims
```powershell
python spark/silver_to_gold.py --spark-master "local[4]"
```

### Bước 7: Khởi tạo View trên Trino & Zeppelin
* **Khởi tạo trên Trino CLI:**
  ```powershell
  trino --server http://localhost:8080 --catalog hive --schema bi_gold -f generated_ddl/all_6_business_views_trino.sql
  ```
* **Khởi tạo trên Zeppelin:** Mở notebook `%spark.sql` và chạy toàn bộ nội dung file `generated_ddl/all_6_business_views_spark.sql`.

### Bước 8: Biên dịch và Xác thực dbt Semantic Layer
```powershell
cd D:\dataguystory\coding-interview-university\working\nextgen-bi-dbt\nextgen-bi-dbt\dbt_projects\epm
dbt compile
dbt test --select tag:epm
```
