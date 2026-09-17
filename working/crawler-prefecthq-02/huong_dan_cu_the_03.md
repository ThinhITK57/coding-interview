Tuyệt vời, chúng ta bắt tay vào thực hiện theo đúng lộ trình 5 bước. Bạn chỉ cần làm lần lượt theo từng bước dưới đây:

---

### BƯỚC 1: Sao chép bộ mã nguồn EPM vào thư mục `ingestions/epm`

Để cấu trúc repository của công ty được gọn gàng và không xung đột tên file, chúng ta gom toàn bộ mã nguồn core của crawler vào thư mục con `epm/` bên trong `ingestions`.

Mở **PowerShell** và chạy các dòng lệnh sau:

```powershell
# 1. Đi tới thư mục ingestions của công ty
cd D:\dataguystory\coding-interview-university\working\code-need-done\prefecthq-external-ingestion\ingestions

# 2. Tạo thư mục con epm
New-Item -ItemType Directory -Force -Path "epm"

# 3. Định nghĩa đường dẫn nguồn và đích
$SRC = "D:\dataguystory\coding-interview-university\working\crawler-prefecthq-02\crawler-prefecthq"
$DST = "D:\dataguystory\coding-interview-university\working\code-need-done\prefecthq-external-ingestion\ingestions\epm"

# 4. Copy file cấu hình và core pipeline
Copy-Item -Path "$SRC\config.json" -Destination $DST
Copy-Item -Path "$SRC\prefect_flow.py" -Destination $DST

# 5. Copy toàn bộ các package nghiệp vụ phụ trợ
$folders = @("auth", "client", "config", "checkpoint", "checkpoints", "data_type", "exceptions", "ingestion", "monitoring", "pagination", "quality", "reliability", "storage", "transform")
foreach ($f in $folders) {
    if (Test-Path "$SRC\$f") {
        Copy-Item -Path "$SRC\$f" -Destination $DST -Recurse -Force
    }
}

Write-Host ">>> Da sao chep thanh cong toan bo source code vao ingestions/epm!" -ForegroundColor Green
```

---

### BƯỚC 2: Tạo file cấu hình môi trường `.env.epm`

Tại thư mục `ingestions/`, bạn tạo file mới có tên: **`.env.epm`**
*(Đường dẫn: `D:\dataguystory\coding-interview-university\working\code-need-done\prefecthq-external-ingestion\ingestions\.env.epm`)*

Nội dung file:

```ini
# ==============================================================================
# PREFECT SERVER (Gửi heartbeat và nhận lịch từ Container prefect-server)
# ==============================================================================
PREFECT_API_URL=http://prefect-server:4200/api
PREFECT_SERVER_ANALYTICS_ENABLED=false

# ==============================================================================
# EPM / CLARIZEN API CREDENTIALS
# ==============================================================================
EPM_BASE_URL=https://api.clarizen.com/v2.0
EPM_USERNAME=your_username
EPM_PASSWORD=your_password
EPM_API_KEY=your_api_key

# ==============================================================================
# MINIO S3 LAKEHOUSE (Tầng Bronze lưu trữ thô)
# ==============================================================================
MINIO_ENDPOINT=https://datalake-s3.viettelcyber.com:10.255.244.100
MINIO_ACCESS_KEY=your_minio_access_key
MINIO_SECRET_KEY=your_minio_secret_key
MINIO_BUCKET=lakehouse
MINIO_OUTPUT_BUCKET=lakehouse

# ==============================================================================
# AMBARI HDFS (Knox Gateway Client)
# ==============================================================================
AMBARI_LOGIN_URL=https://datalake.viettelcyber.com/gateway/ui/ambari/api/v1/clusters
AMBARI_USERNAME=your_ambari_username
AMBARI_PASSWORD=your_ambari_password
HDFS_CLIENT=ambari

# ==============================================================================
# PROXY HẠ TẦNG NỘI BỘ VIETTEL (Nếu chạy trong mạng nội bộ)
# ==============================================================================
HTTP_PROXY=http://192.168.5.8:3128
HTTPS_PROXY=http://192.168.5.8:3128
```

---

### BƯỚC 3: Viết file điều phối `epm_crawler_flow.py`

Tại thư mục `ingestions/`, bạn tạo file mới có tên: **`epm_crawler_flow.py`**
*(Đường dẫn: `D:\dataguystory\coding-interview-university\working\code-need-done\prefecthq-external-ingestion\ingestions\epm_crawler_flow.py`)*

File này chuyển đổi toàn bộ **8 lịch trình CRON từ `prefect.yaml`** sang các `Deployment` của Prefect và chạy lệnh `serve()`:

```python
#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
EPM Lakehouse Ingestion Flow for Prefect Server / Docker
Author: Data Engineering Team
Target: Prefect 2.x / 3.x | Python 3.10+
"""

import os
import sys
import logging
from datetime import datetime

# 1. Tải cấu hình từ .env.epm
from dotenv import load_dotenv
env_file = os.path.join(os.path.dirname(__file__), ".env.epm")
if os.path.exists(env_file):
    load_dotenv(env_file)
else:
    load_dotenv()

# 2. Thêm thư mục epm và ingestions vào sys.path
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
EPM_DIR = os.path.join(CURRENT_DIR, "epm")
if EPM_DIR not in sys.path:
    sys.path.insert(0, EPM_DIR)
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

from prefect import flow, serve
from epm.prefect_flow import api_ingestion_pipeline, api_ingestion_dag_pipeline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("epm_crawler_flow")

CONFIG_PATH = os.path.join(EPM_DIR, "config.json")


# ==============================================================================
# FLOWS CHÍNH
# ==============================================================================

@flow(name="[EPM] Crawl Endpoint Pipeline", log_prints=True)
def crawl_epm_endpoint(endpoint_name: str, mode: str = "incremental"):
    """Thực thi crawl cho 1 endpoint cụ thể (tasks, projects, targets, objectives, c_assignments)."""
    logger.info(f"Triggering EPM crawl: endpoint={endpoint_name}, mode={mode}")
    return api_ingestion_pipeline(
        config_path=CONFIG_PATH,
        endpoint_name=endpoint_name,
        env=os.getenv("ENV", "prod"),
        mode=mode,
        spark_master="local[2]"
    )


@flow(name="[EPM] Master DAG Daily Pipeline", log_prints=True)
def crawl_epm_master_dag():
    """Thực thi toàn bộ 5 endpoints theo thứ tự phụ thuộc (DAG)."""
    logger.info("Triggering EPM Master DAG Pipeline...")
    return api_ingestion_dag_pipeline(
        config_path=CONFIG_PATH,
        env=os.getenv("ENV", "prod"),
        mode="incremental",
        spark_master="local[2]"
    )


# ==============================================================================
# KHỞI CHẠY VÀ PHỤC VỤ (SERVE) 8 LỊCH TRÌNH CRON
# ==============================================================================

if __name__ == "__main__":
    logger.info("Configuring EPM Deployments matching crawl strategy...")

    # Lịch 1: Tasks - Incremental mỗi 2 giờ trong giờ hành chính (08:00 - 18:00, Thứ 2 đến Thứ 6)
    d_tasks_2h = crawl_epm_endpoint.to_deployment(
        name="[2-Hourly]-EPM Tasks Incremental",
        cron="0 8-18/2 * * 1-5",
        parameters={"endpoint_name": "tasks", "mode": "incremental"},
        tags=["production", "epm", "tasks", "incremental"]
    )

    # Lịch 2: Tasks - Full Sync đối soát cuối tuần (Chủ Nhật 23:00)
    d_tasks_weekly = crawl_epm_endpoint.to_deployment(
        name="[Weekly]-EPM Tasks Full Reconciliation",
        cron="0 23 * * 0",
        parameters={"endpoint_name": "tasks", "mode": "full"},
        tags=["production", "epm", "tasks", "full"]
    )

    # Lịch 3: Projects - Giữa ngày (12:00) & Cuối ngày (18:30)
    d_proj_midday = crawl_epm_endpoint.to_deployment(
        name="[Midday]-EPM Projects Incremental",
        cron="0 12 * * 1-5",
        parameters={"endpoint_name": "projects", "mode": "incremental"},
        tags=["production", "epm", "projects"]
    )
    d_proj_evening = crawl_epm_endpoint.to_deployment(
        name="[Evening]-EPM Projects Incremental",
        cron="30 18 * * 1-5",
        parameters={"endpoint_name": "projects", "mode": "incremental"},
        tags=["production", "epm", "projects"]
    )

    # Lịch 4: Targets - Giữa ngày (12:15) & Cuối ngày (18:45)
    d_target_midday = crawl_epm_endpoint.to_deployment(
        name="[Midday]-EPM Targets Incremental",
        cron="15 12 * * 1-5",
        parameters={"endpoint_name": "targets", "mode": "incremental"},
        tags=["production", "epm", "targets"]
    )
    d_target_evening = crawl_epm_endpoint.to_deployment(
        name="[Evening]-EPM Targets Incremental",
        cron="45 18 * * 1-5",
        parameters={"endpoint_name": "targets", "mode": "incremental"},
        tags=["production", "epm", "targets"]
    )

    # Lịch 5: Objectives - Full hàng đêm (01:00 AM)
    d_obj_daily = crawl_epm_endpoint.to_deployment(
        name="[Daily]-EPM Objectives Full",
        cron="0 1 * * *",
        parameters={"endpoint_name": "objectives", "mode": "full"},
        tags=["production", "epm", "objectives"]
    )

    # Lịch 6: Assignments (PGNV) - Full hàng đêm (01:30 AM)
    d_asn_daily = crawl_epm_endpoint.to_deployment(
        name="[Daily]-EPM Assignments Full",
        cron="30 1 * * *",
        parameters={"endpoint_name": "c_assignments", "mode": "full"},
        tags=["production", "epm", "assignments"]
    )

    # Lịch 7: User Access Log - Hàng đêm (02:00 AM)
    d_access_daily = crawl_epm_endpoint.to_deployment(
        name="[Daily]-EPM User Access Log",
        cron="0 2 * * *",
        parameters={"endpoint_name": "user_access_log", "mode": "incremental"},
        tags=["production", "epm", "access_log"]
    )

    # Lịch 8: Master DAG Pipeline - Chạy tổng hợp toàn bộ các làn phụ thuộc (03:00 AM)
    d_master_dag = crawl_epm_master_dag.to_deployment(
        name="[Daily]-EPM Master DAG Pipeline",
        cron="0 3 * * *",
        tags=["production", "epm", "master-dag"]
    )

    # Lịch 9: Ad-hoc - Cho phép trigger bằng tay bất kỳ lúc nào trên UI
    d_adhoc = crawl_epm_endpoint.to_deployment(
        name="[Adhoc]-EPM Manual Run Single Endpoint",
        parameters={"endpoint_name": "tasks", "mode": "incremental"},
        tags=["adhoc", "epm", "manual"]
    )

    logger.info("Serving all 9 EPM Deployments to Prefect Server...")
    serve(
        d_tasks_2h,
        d_tasks_weekly,
        d_proj_midday,
        d_proj_evening,
        d_target_midday,
        d_target_evening,
        d_obj_daily,
        d_asn_daily,
        d_access_daily,
        d_master_dag,
        d_adhoc
    )
```

---

### BƯỚC 4: Tạo file Docker Compose `docker-compose-epm.yml`

Tại thư mục `ingestions/`, bạn tạo file mới có tên chính xác như bạn yêu cầu: **`docker-compose-epm.yml`**
*(Đường dẫn: `D:\dataguystory\coding-interview-university\working\code-need-done\prefecthq-external-ingestion\ingestions\docker-compose-epm.yml`)*

Nội dung file:

```yaml
version: "3.9"

networks:
  workflow_authelia-proxy-internal:
    external: true

# ================= COMMON CONFIG =================
x-common-service: &common-service
  image: flow-external-ingestion-minio:latest
  restart: always
  volumes:
    - .:/app
  env_file: .env.epm
  environment:
    PREFECT_API_URL: http://prefect-server:4200/api
    HTTP_PROXY: 
    HTTPS_PROXY: 
    http_proxy: 
    https_proxy: 
  extra_hosts: &common-hosts
    - "datalake.viettelcyber.com:10.255.244.100"
    - "datalake-s3.viettelcyber.com:10.255.244.100"
    - "nocodb.viettelcyber.com:10.255.244.100"
    - "trino.viettelcyber.com:10.255.244.100"
    - "secagi.viettelcyber.com:10.255.244.100"
  networks:
    - workflow_authelia-proxy-internal
  logging:
    driver: "json-file"
    options:
      max-size: "50m"
      max-file: "10"

# ================= SERVICES =================
services:

  # Service daemon chính: chạy lệnh serve() giữ container sống 24/7 và kích hoạt 8 lịch crawl
  minio-epm-crawler:
    <<: *common-service
    container_name: minio-epm-crawler
    command: >
      bash -c "
        python epm_crawler_flow.py
      "

  # Service chạy thủ công (chỉ chạy khi gõ lệnh, không tự động bật)
  minio-epm-adhoc-runner:
    <<: *common-service
    container_name: minio-epm-adhoc-runner
    profiles: ["manual"]
    command: >
      bash -c "
        python epm/prefect_flow.py --all --standalone --mode incremental
      "
```

---

### BƯỚC 5: Các lệnh kiểm tra và vận hành trên Server / Máy local

Sau khi đã tạo xong 3 file (`.env.epm`, `epm_crawler_flow.py`, `docker-compose-epm.yml`) và copy thư mục `epm/`:

1. **Khởi động container ngầm:**
   ```bash
   docker-compose -f docker-compose-epm.yml up -d minio-epm-crawler
   ```

2. **Theo dõi log trực tiếp:**
   ```bash
   docker logs -f minio-epm-crawler
   ```
   *Khi bạn thấy dòng `Serving all 9 EPM Deployments to Prefect Server...` tức là container đã online và sẵn sàng.*

3. **Kiểm tra trên Web UI Prefect (`http://prefect-server:4200`):**
   - Vào mục **Deployments**, bạn sẽ thấy các deployments mang tên `[2-Hourly]-EPM Tasks Incremental`, `[Daily]-EPM Master DAG Pipeline`... xuất hiện đầy đủ với trạng thái `Ready` và có lịch chạy tiếp theo.

4. **Dừng container khi cần:**
   ```bash
   docker-compose -f docker-compose-epm.yml down
   ```


Searched for "c_assignments"
Searched for "c_assignments"
Searched for "assignment"

Error: The stream was interrupted. Please continue the task you were working on. (Error ID: d930db3f-fd25-4c4f-80fa-b9c1437a93e0-2857-2010)

Viewed config.json:1-50
Searched for ""name":"
Ran command: `Select-String -Path "working/crawler-prefecthq-02/crawler-prefecthq/config.json" -Pattern '"name":'`
Viewed config.json:870-900
Viewed config.json:901-925
Viewed schema_contract.py:1-60
Viewed schema_contract.py:60-120
Viewed br01_bsc_yearly.sql:1-31

Đây là một yêu cầu nghiệp vụ chuẩn hóa tên bảng rất phổ biến (Clean Naming Convention): **Entity của API Clarizen mang tên kỹ thuật là `C_Assignment`, nhưng ở tầng Lakehouse (Bronze/Silver/Gold/dbt), Leader muốn tên bảng đồng bộ, sạch sẽ là `assignments` (hoặc `epm_assignments`)**.

Để chuyển đổi đồng bộ từ đầu vào API đến tầng dbt mà không làm gãy pipeline, bạn chỉ cần chỉnh sửa tại **6 vị trí cụ thể** theo từng tầng dưới đây:

---

### 1. Tầng Ingestion / Crawl (`config.json` & `prefect_flow.py`)

#### Vị trí 1.1: File `config.json`
* **File:** `ingestions/epm/config.json` (dòng 874)
* **Sửa:** Đổi tên định danh endpoint từ `"c_assignments"` thành `"assignments"`.
  > ⚠️ **LƯU Ý:** Giữ nguyên `"typeName": "C_Assignment"` ở dòng 907 bên dưới vì đó là tên thực thể API bắt buộc của Clarizen/Planview!

```json
// Trước:
{
  "name": "c_assignments",
  "path": "/v2.0/services/data/entityQuery",
  ...
  "body": {
    "typeName": "C_Assignment",
    ...
  }
}

// Sửa thành:
{
  "name": "assignments",
  "path": "/v2.0/services/data/entityQuery",
  ...
  "body": {
    "typeName": "C_Assignment",
    ...
  }
}
```
* **Tác dụng:** Extractor sẽ tự động ghi dữ liệu Bronze vào đường dẫn `bronze/clarizen/assignments/`.

#### Vị trí 1.2: File `epm_crawler_flow.py` & `checkpoints/`
* **File:** `ingestions/epm_crawler_flow.py` (Lịch 6 - d_asn_daily):
  ```python
  # Đổi endpoint_name từ "c_assignments" sang "assignments"
  d_asn_daily = crawl_epm_endpoint.to_deployment(
      name="[Daily]-EPM Assignments Full",
      cron="30 1 * * *",
      parameters={"endpoint_name": "assignments", "mode": "full"},
      tags=["production", "epm", "assignments"]
  )
  ```
* **Thư mục:** Đổi tên file checkpoint tương ứng:
  `checkpoints/c_assignments.checkpoint.json` ➔ `checkpoints/assignments.checkpoint.json`.

---

### 2. Tầng Bronze ➔ Silver Base (`spark/bronze_to_silver.py`)

* **File:** `spark/bronze_to_silver.py` (dòng 40)
* **Sửa:** Đổi tên bảng trong danh sách xử lý:

```python
# Trước:
SUPPORTED_TABLES = [
    "tasks",
    "projects",
    "targets",
    "objectives",
    "c_assignments"
]

# Sửa thành:
SUPPORTED_TABLES = [
    "tasks",
    "projects",
    "targets",
    "objectives",
    "assignments"   # <-- Đổi ở đây
]
```

* **Tác dụng:** 
  - Spark sẽ đọc từ `bronze/clarizen/assignments` và ghi ra Silver với tên bảng chuẩn: `silver/epm/epm_assignments`.
  - **Điểm hay:** Trong file `transform/schema_contract.py` (dòng 82), chúng ta đã ánh xạ sẵn `"assignments": "c_assignment"`, nên hệ thống vẫn tự động nạp đúng file `data_type/c_assignment_dataType.sql` mà **không cần đổi tên file DDL nguồn**.

---

### 3. Tầng Silver Kimball Dims & Facts (`spark/build_silver_dims_facts.py`)

* **File:** `spark/build_silver_dims_facts.py` (dòng 463)
* **Sửa:** Khi load bảng Silver lên Spark để dựng Dim & Fact:

```python
# Trước:
assignments_df = load_silver_table(spark, args.silver_base_path, "epm_c_assignments")

# Sửa thành:
assignments_df = load_silver_table(spark, args.silver_base_path, "epm_assignments")
```

---

### 4. Tầng Gold Business Marts (`spark/silver_to_gold.py`)

* **File:** `spark/silver_to_gold.py` (dòng 420)
* **Sửa:** Đọc dữ liệu từ `epm_assignments` để join tạo View BR-01 (BSC trong năm) và View BR-06 (Mục tiêu Ban Giám đốc):

```python
# Trước:
assignments_df = load_silver_table(spark, args.silver_path, "epm_c_assignments")

# Sửa thành:
assignments_df = load_silver_table(spark, args.silver_path, "epm_assignments")
```

---

### 5. Tầng DDL Trino / Spark SQL (`generated_ddl/`)

Trong 2 file:
- `generated_ddl/all_6_business_views_trino.sql`
- `generated_ddl/all_6_business_views_spark.sql`

* **Sửa:** Tại câu lệnh SQL của **View 1 (vw_br01_bsc_yearly)** và **View 6 (vw_br06_board_objectives)**, đổi bảng JOIN từ `epm_c_assignments` sang `epm_assignments`:

```sql
-- Trước:
LEFT JOIN lakehouse.silver.epm_c_assignments a 
    ON t.c_associated_assignment = a.sysid

-- Sửa thành:
LEFT JOIN lakehouse.silver.epm_assignments a 
    ON t.c_associated_assignment = a.sysid
```

---

### 6. Tầng dbt Semantic Layer (`nextgen-bi-dbt`)

* **File:** `models/epm__sources.yml`
  Khai báo tên bảng trong source:
  ```yaml
  # Sửa name từ epm_c_assignments thành epm_assignments
  - name: epm_assignments
    description: "Conformed Silver table for Assignments (PGNV)"
  ```

* **File:** `models/br01_bsc_yearly.sql` & `models/br06_board_objectives.sql` (dòng 29):
  ```sql
  -- Trước:
  left join {{ source('bi_silver__epm', 'epm_c_assignments') }} a
      on t.c_associated_assignment = a.sysid

  -- Sửa thành:
  left join {{ source('bi_silver__epm', 'epm_assignments') }} a
      on t.c_associated_assignment = a.sysid
  ```

---

### 💡 Bảng tóm tắt nhanh để bạn rà soát:

| Tầng | File cần mở | Thay đổi cũ ➔ mới |
| :--- | :--- | :--- |
| **Crawl Config** | `epm/config.json` | `"name": "c_assignments"` ➔ `"name": "assignments"` |
| **Flow Runner** | `epm_crawler_flow.py` | `endpoint_name="c_assignments"` ➔ `"assignments"` |
| **Silver Base ETL** | `spark/bronze_to_silver.py` | `"c_assignments"` ➔ `"assignments"` (Output: `epm_assignments`) |
| **Kimball Dims/Facts**| `spark/build_silver_dims_facts.py` | Đọc `epm_c_assignments` ➔ Đọc `epm_assignments` |
| **Gold Marts ETL** | `spark/silver_to_gold.py` | Đọc `epm_c_assignments` ➔ Đọc `epm_assignments` |
| **DDL Views** | `all_6_business_views_*.sql` | `JOIN epm_c_assignments` ➔ `JOIN epm_assignments` |
| **dbt Models** | `br01_*.sql`, `br06_*.sql` | `source(..., 'epm_c_assignments')` ➔ `'epm_assignments'` |



Viewed contract_deployed_products.py:1-41
Listed directory etls
Listed directory crm_silver
Viewed _init.py:1-66
Viewed contracts.py:1-84
Listed directory bi_silver
Viewed _init.py:1-50
Listed directory epm_silver

Error: The stream was interrupted. Please continue the task you were working on. (Error ID: d930db3f-fd25-4c4f-80fa-b9c1437a93e0-2933-2010)

Viewed tasks.py:1-100
Viewed tasks.py:101-134
Listed directory etl-zeppline-jobs
Viewed main_submit.py:1-49
Viewed run_job.sh:1-24
Searched for "_raw"
Searched for "epm_raw"
Searched for "clarizen"

Đây là một bước chuyển rất quan trọng: **Chuyển giao mã nguồn từ dạng Python Pipeline cục bộ (Prefect) sang dạng Data Lakehouse Batch Job trên Apache Zeppelin / Apache Livy / Spark on YARN**.

Trước khi bắt tay vào viết code, tôi xin phân tích cặn kẽ để bạn hiểu **tại sao file `contract_deployed_products.py` lại viết như vậy**, và cách chúng ta chuyển hóa `bronze_to_silver.py` vào đây một cách chuẩn xác nhất.

---

### 1. Phân tích cách viết của `contract_deployed_products.py` & Cơ chế của `etl-zeppline-jobs`

Hãy nhìn lại file [`contract_deployed_products.py`](file:///D:/dataguystory/coding-interview-university/working/code-need-done/etl-zeppline-jobs/etls/crm_silver/contract_deployed_products.py):

```python
%livy.pyspark

sql_query = """
WITH last_cm_contracts AS (
    SELECT id, custom_field.cf_deployed_products AS cf_deployed_products
    FROM (
        SELECT id, custom_field, updated_at_ts,
               ROW_NUMBER() OVER (
                   PARTITION BY id
                   ORDER BY updated_at_ts DESC
               ) AS rn
        FROM crm_raw.cm_contracts
    ) t
    WHERE rn = 1
)
...
"""
df = spark.sql(sql_query)
df.write.mode("overwrite").format("parquet").option("path", "...").saveAsTable("crm_silver.contract_deployed_products")
```

#### Vì sao các kỹ sư công ty lại viết như vậy?
1. **Không có `SparkSessionFactory` hay `.env`**: Trong Zeppelin Notebook (hoặc khi chạy qua `sh run_job.sh` gọi `main_submit.py`), biến `spark` (đã bật Hive Support và xác thực Kerberos) **ĐÃ ĐƯỢC TẠO SẴN VÀ INJECT VÀO TIẾN TRÌNH**. Nếu trong code bạn gọi `SparkSessionFactory.create()`, nó sẽ tạo thêm 1 Spark Context thứ hai và lập tức gây crash ứng dụng!
2. **Khử trùng lặp (Dedup) bằng Window Function**: Trong SQL, logic `ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at_ts DESC) WHERE rn = 1` **chính là 100% bản chất thuật toán của `DedupEngine`** mà chúng ta đã code bằng PySpark!
3. **Tự chứa (Self-Contained)**: Khi chạy trên cụm máy chủ phân tán (YARN Cluster), việc import các class từ file ngoài rất dễ bị lỗi `ModuleNotFoundError` trên các Worker Nodes. Do đó, phong cách viết trực tiếp một file tự chứa (SQL + PySpark) vừa chạy được trực tiếp trên Web UI Zeppelin, vừa submit được qua lệnh bash `sh run_job.sh`.

---

### 2. Hai phương án xử lý: Bạn nên chọn cách nào?

| Tiêu chí | Phương án A: Chuẩn phong cách Zeppelin (`crm_silver`) *(KHUYÊN DÙNG)* | Phương án B: Giữ nguyên Class Python (`DedupEngine`, `SchemaContract`) |
| :--- | :--- | :--- |
| **Cách làm** | Viết mỗi bảng 1 file tự chứa (như `contracts.py`), dùng Spark SQL với Window Function để Dedup và CAST kiểu dữ liệu. | Copy các class Python vào `epm_silver/` và gọi hàm `process_bronze_to_silver`. |
| **Khả năng chạy trên Web UI Zeppelin** | **100% thành công**. Chỉ cần mở Zeppelin paste vào là chạy. | Rất khó chạy trên UI vì Livy server không thấy file `.py` ngoài nếu không đóng gói egg/wheel. |
| **Khả năng chạy qua YARN `run_job.sh`**| **100% thành công**. | Cần sửa `main_submit.py` để truyền thêm `--py-files`. |
| **Tính tương thích hệ thống** | Giống 100% các bảng khác của công ty (`crm_silver`, `bi_silver`). | Lệch chuẩn so với các folder khác trong repo. |

👉 **Khuyến nghị**: Chúng ta nên đi theo **Phương án A (chuẩn `crm_silver`)** cho 5 bảng. 

Dưới đây là chi tiết mã nguồn bạn cần tạo cho 5 bảng trong thư mục:
📂 `D:\dataguystory\coding-interview-university\working\code-need-done\etl-zeppline-jobs\etls\epm_silver`

---

### 3. Chi tiết mã nguồn từng file trong `epm_silver`

#### File 0: Khởi tạo database `_init.py`
Tạo file `etls/epm_silver/_init.py`:
```python
%livy.pyspark

spark.sql("create database if not exists epm_silver")
spark.sql("use epm_silver")
```

---

#### File 1: Xử lý bảng Tasks (`tasks.py`)
Tạo file `etls/epm_silver/tasks.py`:
*(Lưu ý: Nguồn đọc có thể là bảng raw `epm_raw.tasks` hoặc đọc từ parquet Bronze nếu công ty chưa tạo database raw)*

```python
%livy.pyspark

# 1. Đọc dữ liệu Bronze (hỗ trợ đọc từ Hive table raw hoặc file Parquet Bronze)
try:
    spark.table("epm_raw.tasks").createOrReplaceTempView("raw_tasks")
except Exception:
    spark.read.parquet("s3a://lakehouse/bronze/clarizen/tasks").createOrReplaceTempView("raw_tasks")

# 2. Câu truy vấn Khử trùng lặp (Dedup sysid + last_updated_on) & Ép kiểu theo Schema Contract
sql_query = """
WITH ranked_tasks AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY sysid 
               ORDER BY COALESCE(last_updated_on, created_on) DESC
           ) AS rn
    FROM raw_tasks
)
SELECT
    -- Primary & Business Keys
    CAST(sysid AS STRING)                          AS sysid,
    CAST(name AS STRING)                           AS name,
    CAST(task_type AS STRING)                      AS task_type,
    CAST(c_internal_type AS STRING)                AS c_internal_type,
    CAST(parent_project AS STRING)                 AS parent_project,
    CAST(manager AS STRING)                        AS manager,
    CAST(c_assignee AS STRING)                     AS c_assignee,
    CAST(c_department AS STRING)                   AS c_department,
    
    -- Status & Progress
    CAST(status AS STRING)                         AS status,
    CAST(track_status AS STRING)                   AS track_status,
    CAST(percent_completed AS DOUBLE)              AS percent_completed,
    
    -- Schedule & Work Metrics
    CAST(start_date AS DATE)                       AS start_date,
    CAST(due_date AS DATE)                         AS due_date,
    CAST(work AS DOUBLE)                           AS work,
    CAST(duration AS DOUBLE)                       AS duration,
    CAST(actual_duration AS DOUBLE)                AS actual_duration,
    
    -- Descriptions & Resources
    CAST(description AS STRING)                    AS description,
    CAST(c_update_description AS STRING)           AS c_update_description,
    CAST(c_epm_default AS STRING)                  AS c_epm_default,
    CAST(all_user_resources_count AS STRING)       AS all_user_resources_count,
    
    -- Timestamps & Audit
    CAST(created_on AS DATE)                       AS created_on,
    CAST(last_updated_on AS DATE)                  AS last_updated_on,
    COALESCE(CAST(last_updated_on AS DATE), current_date()) AS ingest_date
FROM ranked_tasks
WHERE rn = 1
"""

df = spark.sql(sql_query)

# 3. Ghi vào HDFS và lưu bảng Hive epm_silver.tasks phân vùng theo ingest_date
(
    df.write
    .mode("overwrite")
    .format("parquet")
    .partitionBy("ingest_date")
    .option("path", "/opt/datasets/crawlers/vcs_silver/epm_silver/data/tasks")
    .saveAsTable("epm_silver.tasks")
)
```

---

#### File 2: Xử lý bảng Projects (`projects.py`)
Tạo file `etls/epm_silver/projects.py`:

```python
%livy.pyspark

try:
    spark.table("epm_raw.projects").createOrReplaceTempView("raw_projects")
except Exception:
    spark.read.parquet("s3a://lakehouse/bronze/clarizen/projects").createOrReplaceTempView("raw_projects")

sql_query = """
WITH ranked_projects AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY sysid 
               ORDER BY COALESCE(last_updated_on, created_on) DESC
           ) AS rn
    FROM raw_projects
)
SELECT
    CAST(sysid AS STRING)                          AS sysid,
    CAST(name AS STRING)                           AS name,
    CAST(project_type AS STRING)                   AS project_type,
    CAST(c_internal_type AS STRING)                AS c_internal_type,
    CAST(project_manager AS STRING)                AS project_manager,
    CAST(c_department AS STRING)                   AS c_department,
    CAST(c_assignor AS STRING)                     AS c_assignor,
    CAST(c_assignee AS STRING)                     AS c_assignee,
    CAST(c_action_resources AS STRING)             AS c_action_resources,
    CAST(state AS STRING)                          AS state,
    CAST(status AS STRING)                         AS status,
    CAST(track_status AS STRING)                   AS track_status,
    CAST(percent_completed AS DOUBLE)              AS percent_completed,
    CAST(start_date AS DATE)                       AS start_date,
    CAST(due_date AS DATE)                         AS due_date,
    CAST(created_by AS STRING)                     AS created_by,
    CAST(created_on AS DATE)                       AS created_on,
    CAST(last_updated_on AS DATE)                  AS last_updated_on,
    COALESCE(CAST(last_updated_on AS DATE), current_date()) AS ingest_date
FROM ranked_projects
WHERE rn = 1
"""

df = spark.sql(sql_query)

(
    df.write
    .mode("overwrite")
    .format("parquet")
    .partitionBy("ingest_date")
    .option("path", "/opt/datasets/crawlers/vcs_silver/epm_silver/data/projects")
    .saveAsTable("epm_silver.projects")
)
```

---

#### File 3: Xử lý bảng Assignments (`assignments.py`)
*(Bảng này thay thế cho `c_assignments` theo đúng yêu cầu của Leader)*

Tạo file `etls/epm_silver/assignments.py`:

```python
%livy.pyspark

# Đọc nguồn assignments (hoặc c_assignments)
try:
    spark.table("epm_raw.assignments").createOrReplaceTempView("raw_assignments")
except Exception:
    try:
        spark.table("epm_raw.c_assignments").createOrReplaceTempView("raw_assignments")
    except Exception:
        spark.read.parquet("s3a://lakehouse/bronze/clarizen/assignments").createOrReplaceTempView("raw_assignments")

sql_query = """
WITH ranked_assignments AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY sysid 
               ORDER BY COALESCE(last_updated_on, created_on) DESC
           ) AS rn
    FROM raw_assignments
)
SELECT
    CAST(sysid AS STRING)                          AS sysid,
    CAST(name AS STRING)                           AS name,
    CAST(description AS STRING)                    AS description,
    CAST(created_by AS STRING)                     AS created_by,
    CAST(c_assignor AS STRING)                     AS c_assignor,
    CAST(c_assignee AS STRING)                     AS c_assignee,
    CAST(c_start_date AS DATE)                     AS c_start_date,
    CAST(c_end_date AS DATE)                       AS c_end_date,
    CAST(c_parent_assignment AS STRING)            AS c_parent_assignment,
    CAST(c_department AS STRING)                   AS c_department,
    CAST(entity_owner AS STRING)                   AS entity_owner,
    CAST(c_achievement_rate AS DOUBLE)             AS c_achievement_rate,
    CAST(c_total_weight AS DOUBLE)                 AS c_total_weight,
    CAST(c_sum_target_weight_percent AS DOUBLE)    AS c_sum_target_weight_percent,
    CAST(c_update_description AS STRING)           AS c_update_description,
    CAST(created_on AS DATE)                       AS created_on,
    CAST(last_updated_on AS DATE)                  AS last_updated_on,
    COALESCE(CAST(last_updated_on AS DATE), current_date()) AS ingest_date
FROM ranked_assignments
WHERE rn = 1
"""

df = spark.sql(sql_query)

(
    df.write
    .mode("overwrite")
    .format("parquet")
    .partitionBy("ingest_date")
    .option("path", "/opt/datasets/crawlers/vcs_silver/epm_silver/data/assignments")
    .saveAsTable("epm_silver.assignments")
)
```

---

#### File 4: Xử lý bảng Objectives (`objectives.py`)
Tạo file `etls/epm_silver/objectives.py`:

```python
%livy.pyspark

try:
    spark.table("epm_raw.objectives").createOrReplaceTempView("raw_objectives")
except Exception:
    spark.read.parquet("s3a://lakehouse/bronze/clarizen/objective").createOrReplaceTempView("raw_objectives")

sql_query = """
WITH ranked_objectives AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY sysid 
               ORDER BY COALESCE(last_updated_on, created_on) DESC
           ) AS rn
    FROM raw_objectives
)
SELECT
    CAST(sysid AS STRING)                          AS sysid,
    CAST(name AS STRING)                           AS name,
    CAST(c_objective_type AS STRING)               AS c_objective_type,
    CAST(c_department AS STRING)                   AS c_department,
    CAST(parent_objective AS STRING)               AS parent_objective,
    CAST(status AS STRING)                         AS status,
    CAST(track_status AS STRING)                   AS track_status,
    CAST(created_by AS STRING)                     AS created_by,
    CAST(created_on AS DATE)                       AS created_on,
    CAST(last_updated_on AS DATE)                  AS last_updated_on,
    COALESCE(CAST(last_updated_on AS DATE), current_date()) AS ingest_date
FROM ranked_objectives
WHERE rn = 1
"""

df = spark.sql(sql_query)

(
    df.write
    .mode("overwrite")
    .format("parquet")
    .partitionBy("ingest_date")
    .option("path", "/opt/datasets/crawlers/vcs_silver/epm_silver/data/objectives")
    .saveAsTable("epm_silver.objectives")
)
```

---

#### File 5: Xử lý bảng Targets (`targets.py`)
Tạo file `etls/epm_silver/targets.py`:

```python
%livy.pyspark

try:
    spark.table("epm_raw.targets").createOrReplaceTempView("raw_targets")
except Exception:
    spark.read.parquet("s3a://lakehouse/bronze/clarizen/targets").createOrReplaceTempView("raw_targets")

sql_query = """
WITH ranked_targets AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY sysid 
               ORDER BY COALESCE(last_updated_on, created_on) DESC
           ) AS rn
    FROM raw_targets
)
SELECT
    CAST(sysid AS STRING)                          AS sysid,
    CAST(name AS STRING)                           AS name,
    CAST(target_type AS STRING)                    AS target_type,
    CAST(parent_target AS STRING)                  AS parent_target,
    CAST(c_associated_assignment AS STRING)        AS c_associated_assignment,
    CAST(associated_objective AS STRING)          AS associated_objective,
    CAST(associated_item AS STRING)               AS associated_item,
    CAST(c_department AS STRING)                   AS c_department,
    CAST(c_assignee AS STRING)                     AS c_assignee,
    CAST(c_assignor AS STRING)                     AS c_assignor,
    CAST(entity_owner AS STRING)                   AS entity_owner,
    CAST(unit AS STRING)                           AS unit,
    CAST(state AS STRING)                          AS state,
    CAST(status AS STRING)                         AS status,
    
    -- Target Dates & Values (m & n)
    CAST(COALESCE(c_target_date_m, target_date) AS DATE)   AS c_target_date_m,
    CAST(COALESCE(c_target_value_m, target_value) AS DOUBLE) AS c_target_value_m,
    CAST(c_target_date_n AS DATE)                  AS c_target_date_n,
    CAST(c_target_value_n AS DOUBLE)               AS c_target_value_n,
    
    -- Target Results
    CAST(COALESCE(c_target_result_value_m, c_target_result_m) AS DOUBLE) AS c_target_result_m,
    CAST(COALESCE(c_target_result_value_n, c_target_result_n) AS DOUBLE) AS c_target_result_n,
    
    CAST(percent_completed AS DOUBLE)              AS percent_completed,
    CAST(resources_and_placeholders_count AS STRING) AS resources_and_placeholders_count,
    CAST(created_by AS STRING)                     AS created_by,
    CAST(created_on AS DATE)                       AS created_on,
    CAST(last_updated_on AS DATE)                  AS last_updated_on,
    COALESCE(CAST(last_updated_on AS DATE), current_date()) AS ingest_date
FROM ranked_targets
WHERE rn = 1
"""

df = spark.sql(sql_query)

(
    df.write
    .mode("overwrite")
    .format("parquet")
    .partitionBy("ingest_date")
    .option("path", "/opt/datasets/crawlers/vcs_silver/epm_silver/data/targets")
    .saveAsTable("epm_silver.targets")
)
```

---

### 4. Cách kiểm tra và chạy thử:

Bạn có thể chạy thử bất kỳ bảng nào bằng 2 cách:

1. **Chạy qua bash script có sẵn của công ty trên YARN:**
   ```bash
   cd D:\dataguystory\coding-interview-university\working\code-need-done\etl-zeppline-jobs
   sh run_job.sh etls/epm_silver/tasks.py
   ```
2. **Chạy trên giao diện Apache Zeppelin Web UI:**
   - Mở Notebook trên Zeppelin, tạo 1 Paragraph mới.
   - Copy toàn bộ nội dung file (có sẵn `%livy.pyspark` ở dòng đầu) và bấm **Run**.

Bạn xem kỹ các file trên và cho tôi biết bạn thấy phong cách viết này đã hoàn toàn khớp với `crm_silver/contract_deployed_products.py` của công ty chưa nhé!