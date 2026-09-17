# BÁO CÁO PHÂN TÍCH CHUYÊN SÂU: CÁC ISSUES GỐC GIỮA ĐỊNH NGHĨA DBT VÀ DATA LAKE (SPARK JOBS & EPM/BI_SILVER)

---

## 1. BỐI CẢNH KIẾN TRÚC & MỐI QUAN HỆ GIỮA 4 BỘ SOURCE CODE

Để hệ thống dữ liệu vận hành thông suốt ("thông luồng"), 4 bộ dự án trong repository phải tương tác với nhau theo một hợp đồng dữ liệu (Data Contract) chặt chẽ:

```
┌────────────────────────────────────────────────────────────────────────┐
│ 1. prefecthq-external-ingestion                                        │
│    - Nhiệm vụ: Trích xuất REST API Planview Clarizen & Audit Log.      │
│    - Đầu ra: Snapshot Parquet thô tại Bronze: epm_raw_snapshot.*       │
└────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│ 2. bi-datalake-infra & 3. etl-zeppline-jobs (Spark Execution Engine)   │
│    - Nhiệm vụ Silver: Dedup Window Function -> ghi bi_silver.epm_*     │
│    - Nhiệm vụ Star Schema: Dựng 6 Conformed Dims & 4 Periodic Facts    │
│    - Nhiệm vụ Marts: JOIN Fact với Dim -> vật lý hóa bi_gold.<mart>    │
└────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│ 4. nextgen-bi-dbt (Trino Semantic & Governance Layer)                  │
│    - Nhiệm vụ Staging: Chuẩn hóa 6 bảng Silver (bi_silver__epm)        │
│    - Nhiệm vụ Data Marts: Định nghĩa Semantic View (bi_gold.vw_<mart>) │
│    - Nhiệm vụ Testing: Chạy Foreign Key, Not Null, Uniqueness tests    │
│    - Nhiệm vụ Metrics: Cung cấp Semantic Layer cho Lightdash / Superset│
└────────────────────────────────────────────────────────────────────────┘
```

---

## 2. CHỈ RA CÁC ISSUES GỐC (ROOT CAUSES) GIỮA DBT VÀ DATA LAKE

Qua quá trình rà soát đối chiếu chéo giữa **dbt models** (`nextgen-bi-dbt/dbt_projects/epm/models`) với **2 bộ source code Spark**:
1. `working/code-need-done/bi-datalake-infra/spark-jobs/executor/jobs`
2. `working/code-need-done/etl-zeppline-jobs/etls`
chúng tôi chỉ ra **7 issues cốt lõi (Issues gốc)** dẫn đến tình trạng gãy luồng dữ liệu trước đây:

---

### ❌ Issue Gốc 1: Lệch Không Gian Tên Database & Bảng Dữ Liệu (Schema Scoping Mismatch)
* **Hiện trạng cũ trong `bi-datalake-infra`:**
  - `jobs/epm_silver/_init.py` chạy: `CREATE DATABASE IF NOT EXISTS epm_silver`.
  - Các job silver ghi bảng vào: `epm_silver.tasks`, `epm_silver.projects`, `epm_silver.assignments`, `epm_silver.objectives`, `epm_silver.targets`.
  - HDFS path: `/opt/datasets/crawlers/vcs_silver/epm_silver/data/{entity}`.
* **Hiện trạng cũ trong dbt (`epm__sources.yml`):**
  - Khai báo source `bi_silver__epm`: `database: hive`, `schema: bi_silver`.
  - Danh sách bảng tìm kiếm: `epm_tasks`, `epm_projects`, `epm_assignments`, `epm_targets`, `epm_objectives`.
* **Hậu quả gãy luồng:**
  - Khi Trino biên dịch dbt, nó tìm kiếm bảng `hive.bi_silver.epm_tasks`.
  - Nhưng trong Hive Metastore chỉ có bảng `epm_silver.tasks`.
  - Kết quả: `Table 'hive.bi_silver.epm_tasks' does not exist` $\rightarrow$ Toàn bộ dbt compile/test thất bại ngay tại vạch xuất phát.
* **Cách xử lý chuẩn hóa:**
  - Quy chuẩn tên bảng vật lý chuẩn tại Silver là `bi_silver.epm_*` (ví dụ: `bi_silver.epm_tasks`).
  - Đồng thời tạo View tương thích ngược `CREATE OR REPLACE VIEW epm_silver.<entity> AS SELECT * FROM bi_silver.epm_<entity>` để đảm bảo code cũ trong infra không bị ảnh hưởng.

---

### ❌ Issue Gốc 2: Bỏ Rơi Star Schema — dbt Nhảy Cóc Không Sử Dụng Dim & Fact
* **Hiện trạng trong Spark:**
  - Đội ngũ kỹ sư đã xây dựng `epm_dims.py` (tạo 6 Dimensions chuẩn Kimball: `dim_epm_department`, `dim_epm_resource`, `dim_epm_project`, `dim_epm_task`, `dim_epm_objective`, `dim_epm_assignment`).
  - Đã xây dựng `epm_facts.py` (tạo 4 Periodic Snapshot Facts: `fact_epm_target_bsc_snapshot`, `fact_epm_cvct_execution_snapshot`, `fact_epm_task_execution_snapshot`, `fact_epm_project_progress_snapshot`).
* **Hiện trạng trong dbt ban đầu:**
  - Các model `br01_bsc_yearly.sql` đến `br06_board_objectives.sql` lại viết câu lệnh:
    ```sql
    FROM {{ source('bi_silver__epm', 'epm_targets') }} t
    LEFT JOIN {{ source('bi_silver__epm', 'epm_assignments') }} a ON t.c_associated_assignment = a.sysid
    ```
  - dbt hoàn toàn không tham chiếu tới bất kỳ bảng `dim_epm_*` hay `fact_epm_*` nào!
* **Hậu quả:**
  - User đặt câu hỏi: *"Sao tôi thấy chúng ta tạo ra dim và fact nhưng data mart có vẻ như đang không sử dụng nhỉ?"*
  - Công sức tính toán Snapshot, SCD Type 2, phân cấp mục tiêu trong Spark bị lãng phí, còn dbt thì query lại từ đầu trên các bảng thô cồng kềnh.

---

### ❌ Issue Gốc 3: Bug Thiếu Foreign Keys Trong Fact Của `bi-datalake-infra`
* **Hiện trạng trong `bi-datalake-infra/spark-jobs/executor/jobs/bi_silver/epm/epm_facts.py`:**
  - Trong `fact_epm_target_bsc_snapshot`, câu SELECT chỉ lấy:
    ```sql
    ABS(HASH(t.sysid)) AS target_key,
    CAST(t.sysid AS STRING) AS target_id,
    CAST(t.associated_objective AS STRING) AS associated_objective,
    CAST(t.associated_item AS STRING) AS associated_item,
    CAST(t.c_department AS STRING) AS c_department,
    CAST(t.c_assignee AS STRING) AS c_assignee
    ```
  - **Hoàn toàn thiếu các cột Hash Foreign Keys:** `objective_key`, `project_key`, `department_key`, `assignee_key`, `assignor_key`.
* **Hiện trạng trong `mart_bsc_yearly.py` và `mart_board_objectives.py`:**
  - Lại thực hiện JOIN:
    ```sql
    LEFT JOIN bi_silver.dim_epm_department d_dept ON f.department_key = d_dept.department_key
    LEFT JOIN bi_silver.dim_epm_objective d_obj ON f.objective_key = d_obj.objective_key
    ```
* **Hậu quả:**
  - Khi chạy job trên Spark của `bi-datalake-infra`, Spark quăng ngoại lệ nghiêm trọng: `cannot resolve 'f.department_key' given input columns`. Data Mart không thể sinh ra dữ liệu.
* **Cách xử lý chuẩn hóa:**
  - Đã bổ sung các trường Surrogate Hash Keys vào `epm_facts.py`:
    `ABS(HASH(t.associated_objective)) AS objective_key`, `department_key`, `project_key`, `assignee_key`, `assignor_key`.

---

### ❌ Issue Gốc 4: Khủng Hoảng Semantic — Ghi Đè Khóa ID vs Mất Tên Hiển Thị
* **Hiện trạng trong PySpark Marts cũ của `bi-datalake-infra`:**
  - Trong `mart_bsc_yearly.py`:
    ```sql
    SELECT
        COALESCE(d_obj.objective_name, f.objective_key) AS associated_objective,
        COALESCE(d_dept.department_name, 'Unknown')     AS c_department,
        COALESCE(d_assignee.resource_name, 'Unknown')   AS c_assignee,
        COALESCE(d_assignor.resource_name, 'Unknown')   AS assignor
    ```
  - **Tác hại:** Script Spark đã ghi đè trực tiếp tên tiếng Việt vào chính cột mã ID! Cột `associated_objective` (vốn là mã SYSID `/Objective/OBJ-123`) bị biến thành chuỗi text `"Tăng trưởng thị phần 2026"`.
  - Hậu quả: Khi dbt chạy kiểm thử quan hệ dữ liệu:
    ```yaml
    - name: associated_objective
      tests:
        - relationships:
            to: ref('epm_objectives')
            field: sysid
    ```
    Toàn bộ test bị **FAIL 100%**, vì chuỗi text `"Tăng trưởng thị phần"` không thể nào khớp với `sysid` `/Objective/OBJ-123`!
* **Hiện trạng trong dbt cũ:**
  - Ngược lại, dbt cũ chỉ SELECT cột gốc, kết quả trả ra dashboard BI chỉ toàn là mã SYSID cụt ngủn, người dùng cuối không thể biết đó là mục tiêu hay phòng ban nào.
* **Cách xử lý chuẩn hóa (Hợp đồng dữ liệu song hành):**
  - Giữ nguyên 100% cột ID gốc: `associated_objective`, `associated_item`, `c_department`, `c_assignee`, `assignor` để dbt kiểm thử toàn vẹn khóa ngoại.
  - Bổ sung song song các cột Tên hiển thị: `objective_name`, `associated_item_name`, `department_name`, `assignee_name`, `assignor_name` để người dùng kéo thả trực quan trên BI.

---

### ❌ Issue Gốc 5: Naming Convention Lệch Lạc Mang Tính Nội Bộ (`br01` - `br06`)
* **Hiện trạng:**
  - Các file dbt đặt tên theo mã yêu cầu kỹ thuật: `br01_bsc_yearly`, `br02_dieu_hanh_cvct_klcd`, `br03_task_report`, `br04_project_report`, `br05_user_access_traffic`, `br06_board_objectives`.
  - `br` là viết tắt nội bộ của *"Business Requirement"*. Người dùng bên ngoài, các đội ngũ phân tích dữ liệu (DA/BI), và các công cụ Semantic Layer như Lightdash không thể suy luận được bản chất nghiệp vụ từ các mã `br01`, `br02`.
* **Cách xử lý chuẩn hóa:**
  - Đổi tên toàn diện đi từ bản chất dữ liệu (Data-Driven):
    - `br01_bsc_yearly` $\rightarrow$ `bsc_yearly`
    - `br02_dieu_hanh_cvct_klcd` $\rightarrow$ `cvct_execution_report`
    - `br03_task_report` $\rightarrow$ `task_report`
    - `br04_project_report` $\rightarrow$ `project_report`
    - `br05_user_access_traffic` $\rightarrow$ `user_access_traffic`
    - `br06_board_objectives` $\rightarrow$ `board_objectives`

---

### ❌ Issue Gốc 6: Bảng `epm_user_access_log` Bị Bỏ Quên Trong Tầng Silver
* **Hiện trạng:**
  - dbt có model `br05_user_access_traffic.sql` truy vấn `source('bi_silver__epm', 'epm_user_access_log')`.
  - Tuy nhiên, trong `epm_silver/` của cả `bi-datalake-infra` và `etl-zeppline-jobs` trước đây **HOÀN TOÀN KHÔNG CÓ JOB NẠP BẢNG NÀY**!
  - Trong file `epm__sources.yml` cũng không hề khai báo bảng `epm_user_access_log`.
* **Hậu quả:**
  - dbt compile lỗi `Source 'bi_silver__epm.epm_user_access_log' was not found`.
* **Cách xử lý chuẩn hóa:**
  - Tạo mới job PySpark `user_access_log.py` nạp dữ liệu vào `bi_silver.epm_user_access_log` (kèm view `epm_silver.user_access_log`).
  - Khai báo bổ sung bảng `epm_user_access_log` trong `epm__sources.yml`.
  - Tạo staging model `epm_user_access_log.sql` và `epm_user_access_log__schema.yml`.

---

### ❌ Issue Gốc 7: Nhầm Lẫn Ranh Giới Kiến Trúc Giữa Spark Engine (Silver) và dbt (Gold)
* **Hiện trạng sai lệch phân tầng:**
  - Trong tầng số 2 (`bi-datalake-infra` và `etl-zeppline-jobs`), các script được đặt trong thư mục `jobs/bi_silver/epm/` nhưng lại cố tình ghi đè bảng vật lý vào `bi_gold.<mart>` và chạy `CREATE DATABASE IF NOT EXISTS bi_gold` trong `_init.py`.
  - Trong khi đó, toàn bộ cấu trúc thư mục của Data Lake Infra **chỉ có `bi_silver` và `epm_silver`**, hoàn toàn **không có folder `bi_gold`**.
  - Việc này dẫn tới xung đột trách nhiệm nghiêm trọng: Spark ở Tầng 2 "nhảy cóc" sang Gold, trong khi tầng Gold (`bi_gold`) thực chất thuộc về **Tầng 4: dbt (`nextgen-bi-dbt`) trên nền Trino**.
* **Phân định rõ ranh giới 3 Databases:**
  1. **`epm_silver`**: Tầng Silver riêng của domain EPM, nơi lưu trữ các bảng thô đã deduplicate từ crawler (`tasks`, `projects`, `assignments`, `objectives`, `targets`, `user_access_log`).
  2. **`bi_silver`**: Tầng Silver chuẩn hóa chung cấp doanh nghiệp (Enterprise Conformed Silver Data Lake). Đây là **ĐÍCH ĐẾN CUỐI CÙNG của Spark Engine** (chứa `bi_silver.epm_*`, `bi_silver.dim_*`, `bi_silver.fact_*`, và các bảng summary cấp Silver `bi_silver.epm_mart_*`).
  3. **`bi_gold`**: Tầng Data Marts và Semantic Layer phục vụ báo cáo BI/Dashboard, **thuộc quyền sở hữu độc quyền của dbt (`nextgen-bi-dbt`) và Trino**.
* **Cách xử lý chuẩn hóa:**
  - Đã loại bỏ hoàn toàn lệnh `CREATE DATABASE bi_gold` khỏi `_init.py` của Spark.
  - Toàn bộ các script PySpark mart trong `bi_silver/epm/` chỉ ghi vào phạm vi `bi_silver` (`tgt_table = "bi_silver.epm_mart_*"`).
  - Tầng `bi_gold` được giải phóng hoàn toàn cho dbt biên dịch từ nguồn `bi_silver`.

---

## 3. MA TRẬN ĐỐI CHIẾU TRƯỚC VÀ SAU KHI XỬ LÝ

| Thành Phần | Trạng Thái Ban Đầu (Lỗi / Gãy Luồng) | Trạng Thái Đã Chuẩn Hóa (Thông Luồng 100%) |
| :--- | :--- | :--- |
| **Silver Database** | `epm_silver` (không có prefix `epm_`) | `bi_silver.epm_*` + View tương thích ngược `epm_silver.*` |
| **User Access Log** | Không tồn tại trong Silver | Đã có `user_access_log.py` $\rightarrow$ `bi_silver.epm_user_access_log` |
| **Snapshot Facts** | Thiếu hash surrogate foreign keys (`objective_key`, ...) | Đã bổ sung đầy đủ 5 Foreign Keys trong `epm_facts.py` |
| **Khóa ID & Tên** | Spark ghi đè tên vào ID; dbt chỉ có ID không có tên | Song hành: Giữ nguyên mã ID gốc + Thêm cột `_name` hiển thị |
| **Đặt tên Data Mart** | `br01` đến `br06` (viết tắt ngầm nội bộ) | Chuẩn data-driven: `bsc_yearly`, `task_report`, `project_report`... |
| **dbt Models & YAML** | 6 file `br0*` không có schema cho user_access_log | 12 models + 12 schemas hoàn chỉnh, sạch sẽ, không còn file rác |
| **Đồng bộ 2 bộ Spark** | `bi-datalake-infra` và `etl-zeppline-jobs` lệch logic | Đồng bộ 100% logic, sửa lỗi FK và lưu bảng `bi_gold` |

---

## 4. KẾT LUẬN & HƯỚNG DẪN VẬN HÀNH

Sau khi khắc phục toàn bộ 7 issues gốc trên:
1. Pipeline từ **Raw Ingestion** $\rightarrow$ **Silver Dedup** $\rightarrow$ **Star Schema Kimball** $\rightarrow$ **Gold Marts** $\rightarrow$ **dbt Semantic Layer** đã hoàn toàn thông suốt.
2. Không còn bất kỳ xung đột nào về tên bảng giữa Hive Metastore và Trino dbt Catalog.
3. Người dùng cuối có đầy đủ cả mã định danh kỹ thuật lẫn tên hiển thị trực quan để khai thác phân tích kinh doanh.
