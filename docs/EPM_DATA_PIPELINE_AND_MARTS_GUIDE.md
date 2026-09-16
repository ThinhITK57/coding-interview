# TÀI LIỆU KIẾN TRÚC VÀ HƯỚNG DẪN KHAI THÁC DỮ LIỆU EPM (DATA PIPELINE & DATA MARTS)

---

## 1. TỔNG QUAN KIẾN TRÚC DỮ LIỆU ĐA TẦNG (MEDALLION + KIMBALL)

Hệ thống dữ liệu EPM được xây dựng theo mô hình **Lakehouse Medallion kết hợp Kimball Star Schema**, đi tuần tự qua 4 tầng:

```
[API Nguồn EPM]
       │
       ▼ (Prefect Crawlers + Docker Compose)
┌────────────────────────────────────────────────────────────────────────┐
│ 1. BRONZE LAYER (epm_raw_snapshot.*)                                   │
│    - HDFS: /opt/datasets/crawlers/vcs-raw/epm_raw/data/{entity}        │
│    - Snapshot Parquet thô kèm metadata: _raw_payload, ingest_date      │
└────────────────────────────────────────────────────────────────────────┘
       │
       ▼ (PySpark Dedup Engine - etls/epm_silver/)
┌────────────────────────────────────────────────────────────────────────┐
│ 2. CONFORMED SILVER LAYER (bi_silver.epm_*)                            │
│    - HDFS: /opt/datasets/crawlers/vcs_silver/bi_silver/data/epm_{entity}│
│    - Khử trùng lặp bản ghi: ROW_NUMBER() OVER (PARTITION BY sysid)     │
│    - Bảng chuẩn: bi_silver.epm_tasks, epm_projects, epm_assignments... │
│    - Alias tương thích ngược: epm_silver.{entity}                      │
└────────────────────────────────────────────────────────────────────────┘
       │
       ▼ (Star Schema Builder - etls/bi_silver/epm/)
┌────────────────────────────────────────────────────────────────────────┐
│ 3. DIMENSIONAL & FACT LAYER (bi_silver.dim_* & bi_silver.fact_*)       │
│    - 6 Conformed Dimensions: dim_epm_department, resource, project...  │
│    - 4 Periodic Snapshot Facts có đầy đủ Foreign Keys trỏ sang Dim     │
└────────────────────────────────────────────────────────────────────────┘
       │
       ▼ (PySpark Data Marts & dbt Trino Semantic Views)
┌────────────────────────────────────────────────────────────────────────┐
│ 4. GOLD DATA MARTS LAYER (bi_gold.*)                                   │
│    - HDFS: /opt/datasets/crawlers/vcs_silver/bi_gold/data/{mart}       │
│    - 6 Bảng nghiệp vụ chuẩn: bsc_yearly, task_report, project_report...│
│    - Fact JOIN Dims: Song hành cả Khóa ID và Tên mô tả (_name)        │
│    - dbt Models: vw_bsc_yearly, vw_task_report...                     │
└────────────────────────────────────────────────────────────────────────┘
```

---

## 2. DATA DOMAIN GLOSSARY: CHUẨN HÓA ĐẶT TÊN DATA-DRIVEN

Loại bỏ toàn bộ các tiền tố viết tắt ngầm hiểu nội bộ (`br01` - `br06`), quy chuẩn sang tên miền dữ liệu rõ nghĩa cho toàn bộ người dùng trong và ngoài tổ chức:

| Mã nội bộ cũ | Tên Data Mart nghiệp vụ mới | Tên Bảng Hive (`bi_gold`) | Tên View dbt (`bi_gold`) | Ý nghĩa nghiệp vụ |
| :--- | :--- | :--- | :--- | :--- |
| `br01_bsc_yearly` | **`bsc_yearly`** | `bi_gold.bsc_yearly` | `vw_bsc_yearly` | Thẻ điểm cân bằng BSC thường niên |
| `br02_dieu_hanh_cvct_klcd` | **`cvct_execution_report`** | `bi_gold.cvct_execution_report` | `vw_cvct_execution_report` | Điều hành công việc trọng tâm & Kết luận chỉ đạo |
| `br03_task_report` | **`task_report`** | `bi_gold.task_report` | `vw_task_report` | Khối lượng & Tiến độ công việc chi tiết |
| `br04_project_report` | **`project_report`** | `bi_gold.project_report` | `vw_project_report` | Tổng quan & Quản trị danh mục dự án |
| `br05_user_access_traffic` | **`user_access_traffic`** | `bi_gold.user_access_traffic` | `vw_user_access_traffic` | Lưu lượng & Tần suất truy cập hệ thống EPM |
| `br06_board_objectives` | **`board_objectives`** | `bi_gold.board_objectives` | `vw_board_objectives` | Mục tiêu chiến lược trình Ban Giám Đốc |

---

## 3. NGUYÊN TẮC SEMANTIC: SONG HÀNH "KHÓA ID" & "TÊN HIỂN THỊ"

Nhằm giải quyết triệt để vấn đề mâu thuẫn giữa kiểm thử dữ liệu (Data Integrity Tests) và trải nghiệm người dùng (BI Dashboards):
1. **Khóa ID gốc được bảo toàn 100%:**
   - Các cột `associated_objective`, `associated_item`, `c_department`, `c_assignee`, `assignor`, `parent_project` luôn lưu trữ mã định danh duy nhất (`SYSID`).
   - Dùng cho dbt tests (foreign key relationship, not null, unique).
2. **Tên hiển thị được làm giàu từ Dimension:**
   - Bổ sung các cột có hậu tố `_name`: `objective_name`, `associated_item_name`, `department_name`, `assignee_name`, `assignor_name`, `project_name`...
   - Lấy từ việc `LEFT JOIN` giữa Fact và các Dimension tương ứng (`dim_epm_department`, `dim_epm_resource`, `dim_epm_project`, `dim_epm_objective`).
   - Phục vụ trực tiếp cho người dùng cuối kéo thả báo cáo trên Metabase, Lightdash, Power BI.

---

## 4. CHI TIẾT DANH MỤC CỘT CỦA 6 DATA MARTS

### 4.1. Bảng `bsc_yearly` (22 cột)
* **Khóa ID:** `associated_objective`, `associated_item`, `parent_target`, `c_department`, `c_assignee`, `assignor`.
* **Tên hiển thị:** `objective_name`, `associated_item_name`, `department_name`, `assignee_name`, `assignor_name`.
* **Thuộc tính & Đo lường:** `target_type`, `name`, `unit`, `target_date_m`, `target_value_m`, `target_date_n`, `target_value_n`, `state`, `status`, `target_result_m`, `target_result_n`.

### 4.2. Bảng `cvct_execution_report` (9 cột)
* **Khóa ID & Tên hiển thị:** `assignee` + `assignee_name`.
* **Thuộc tính & Đo lường:** `resources`, `name`, `status`, `due_date`, `percent_completed`, `target_value_m`, `target_date_m`.

### 4.3. Bảng `task_report` (18 cột)
* **Khóa ID & Tên hiển thị:** `assignee` + `assignee_name`, `department` + `department_name`, `parent_project` + `project_name`.
* **Thuộc tính & Đo lường:** `task_type`, `resources`, `jira_status`, `name`, `description`, `work`, `duration`, `epm_default`, `start_date`, `due_date`, `percent_completed`, `update_description`.
* **Logic Fallback sửa lỗi:**
  - `update_description = COALESCE(c_update_description, overview)`
  - `epm_default = COALESCE(c_epm_default, default_integration_path)`

### 4.4. Bảng `project_report` (15 cột)
* **Khóa ID & Tên hiển thị:** `department` + `department_name`, `assignor` + `assignor_name`, `assignee` + `assignee_name`, `project_manager` + `project_manager_name`.
* **Thuộc tính & Đo lường:** `name`, `project_type`, `due_date`, `state`, `status`, `percent_completed`, `resources`.

### 4.5. Bảng `user_access_traffic` (7 cột)
* `login_date`, `name`, `first_name`, `last_name`, `groups`, `direct_manager`, `job_title`.

### 4.6. Bảng `board_objectives` (22 cột)
* Giữ nguyên quy định tiền tố `c_` của Ban Giám Đốc cho các cột đo lường: `c_target_date_m`, `c_target_date_n`, `c_target_value_m`, `c_target_value_n`, `c_target_result_m`, `c_target_result_n`, `c_assignor`.
* Cung cấp song hành các cột Tên hiển thị: `objective_name`, `associated_item_name`, `department_name`, `assignee_name`, `assignor_name`.

---

## 5. HƯỚNG DẪN VẬN HÀNH PIPELINE (RUNBOOK)

### Bước 1: Khởi tạo Metadata cơ sở dữ liệu
```bash
sh run_job.sh etls/epm_silver/_init.py
```

### Bước 2: Chạy Deduplication tầng Silver
```bash
sh run_job.sh etls/epm_silver/tasks.py
sh run_job.sh etls/epm_silver/projects.py
sh run_job.sh etls/epm_silver/assignments.py
sh run_job.sh etls/epm_silver/objectives.py
sh run_job.sh etls/epm_silver/targets.py
sh run_job.sh etls/epm_silver/user_access_log.py
```

### Bước 3: Dựng Star Schema (Dimensions & Snapshot Facts)
```bash
sh run_job.sh etls/bi_silver/epm/epm_dims.py
sh run_job.sh etls/bi_silver/epm/epm_facts.py
```

### Bước 4: Tạo 6 Bảng Gold Data Marts
```bash
sh run_job.sh etls/bi_silver/epm/mart_bsc_yearly.py
sh run_job.sh etls/bi_silver/epm/mart_cvct_execution.py
sh run_job.sh etls/bi_silver/epm/mart_task_report.py
sh run_job.sh etls/bi_silver/epm/mart_project_report.py
sh run_job.sh etls/bi_silver/epm/mart_user_access_traffic.py
sh run_job.sh etls/bi_silver/epm/mart_board_objectives.py
```

### Bước 5: Biên dịch & Kiểm thử dbt
Toàn bộ 12 model dbt (6 staging models + 6 data mart models) và 12 schema YAML tương ứng đã được chuẩn hóa. Các file cũ có tiền tố `br01` - `br06` đã được dọn dẹp triệt để.

```bash
cd working/nextgen-bi-dbt/nextgen-bi-dbt/dbt_projects/epm
dbt compile
dbt test
```

---

## 6. DANH MỤC FILE DBT MODELS ĐÃ HOÀN THIỆN

### Staging Models (Tầng Silver):
1. `epm_assignments.sql` + `epm_assignments__schema.yml`
2. `epm_objectives.sql` + `epm_objectives__schema.yml`
3. `epm_projects.sql` + `epm_projects__schema.yml`
4. `epm_targets.sql` + `epm_targets__schema.yml`
5. `epm_tasks.sql` + `epm_tasks__schema.yml`
6. `epm_user_access_log.sql` + `epm_user_access_log__schema.yml`

### Data Mart Views (Tầng Gold Semantic):
1. `bsc_yearly.sql` (`vw_bsc_yearly`) + `bsc_yearly__schema.yml`
2. `cvct_execution_report.sql` (`vw_cvct_execution_report`) + `cvct_execution_report__schema.yml`
3. `task_report.sql` (`vw_task_report`) + `task_report__schema.yml`
4. `project_report.sql` (`vw_project_report`) + `project_report__schema.yml`
5. `user_access_traffic.sql` (`vw_user_access_traffic`) + `user_access_traffic__schema.yml`
6. `board_objectives.sql` (`vw_board_objectives`) + `board_objectives__schema.yml`

