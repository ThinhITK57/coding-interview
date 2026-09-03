# THIẾT KẾ HỆ THỐNG ETL/ELT, DATA WAREHOUSE & KỸ NĂNG SQL/PYTHON CHO DATA ENGINEER

Tài liệu này tổng hợp toàn bộ kiến thức chuyên sâu về thiết kế hệ thống dữ liệu, mô hình hóa dữ liệu (Data Modeling), tối ưu hóa câu lệnh SQL nâng cao, lập trình Python xử lý dữ liệu lớn, và các bài toán System Design kinh điển trong phỏng vấn Data Engineer.

---

## PHẦN 1: THIẾT KẾ PIPELINE ETL & ELT

### 1. ETL vs ELT: Khi nào chọn cái nào?

```
ETL: Source Data ──► [ Extract ] ──► [ Transform (Staging/Spark) ] ──► [ Load ] ──► Data Warehouse
ELT: Source Data ──► [ Extract ] ──► [ Load (Raw Lake/Table) ] ──► [ Transform (dbt/SQL in DW) ] ──► Data Warehouse
```

| Tiêu chí | ETL (Extract - Transform - Load) | ELT (Extract - Load - Transform) |
| :--- | :--- | :--- |
| **Nơi xử lý** | Trên một công cụ tính toán trung gian độc lập (ví dụ: cụm Apache Spark, Talend, Pentaho). | Trực tiếp bên trong Data Warehouse đích (ví dụ: Snowflake, BigQuery, Redshift). |
| **Tốc độ nạp** | Chậm hơn (vì phải đợi biến đổi xong mới nạp vào đích). | Rất nhanh (chỉ nạp thô vào đích trước rồi biến đổi sau). |
| **Quy mô dữ liệu** | Phù hợp dữ liệu dung lượng vừa, cấu trúc phức tạp. | Phù hợp dữ liệu lớn (Big Data), cấu trúc đơn giản hoặc bán cấu trúc. |
| **Chi phí** | Tốn chi phí hạ tầng cho cụm xử lý trung gian. | Tiết kiệm hơn vì tận dụng sức mạnh tính toán co giãn của Cloud Data Warehouse. |
| **Công cụ phổ biến** | Spark, Airflow, AWS Glue. | **dbt (data build tool)**, Snowflake, BigQuery. |

---

### 2. Các mô hình nạp dữ liệu (Ingestion Patterns)
*   **Full Load**: Nạp lại toàn bộ dữ liệu của bảng nguồn mỗi lần chạy. Đơn giản nhưng rất tốn tài nguyên và thời gian khi dữ liệu tăng lên.
*   **Incremental Load**: Chỉ nạp các bản ghi mới hoặc thay đổi kể từ lần chạy cuối cùng (thường dựa trên các cột `updated_at`, `created_at` hoặc trường ID tăng dần).
*   **Change Data Capture (CDC)**: Theo dõi trực tiếp các hoạt động ghi dữ liệu vào tệp nhật ký (Transaction Log) của CSDL nguồn (như Binlog của MySQL, WAL của PostgreSQL).
    *   *Ưu điểm*: Không làm ảnh hưởng tới hiệu năng của Database nguồn do không chạy câu lệnh query trực tiếp lên bảng; ghi nhận được cả các hành động xóa (`DELETE`) dữ liệu.
    *   *Công cụ*: **Debezium**, **Kafka Connect**, AWS Database Migration Service (DMS).

---

### 3. Xử lý lỗi trong Pipeline (Error Handling)
*   **Dead Letter Queue (DLQ)**: Khi một bản ghi bị lỗi định dạng (ví dụ cột số lượng nhận giá trị chữ `"abc"`), thay vì dừng cả pipeline làm sập hệ thống, bản ghi lỗi này sẽ được tách riêng ra và đẩy vào một thư mục/bảng chứa lỗi (DLQ) để kỹ sư kiểm tra sau. Các dữ liệu sạch vẫn được xử lý tiếp bình thường.
*   **Cơ chế Retry với Exponential Backoff**: Khi gọi API hoặc kết nối DB bị lỗi mạng tạm thời, hệ thống tự động gọi lại sau các khoảng thời gian tăng dần (ví dụ sau 2s, 4s, 8s, 16s) trước khi chính thức báo lỗi.

---

## PHẦN 2: THIẾT KẾ KHO DỮ LIỆU (DATA WAREHOUSE DESIGN)

Đây là phần phỏng vấn quan trọng nhất quyết định bạn có được nhận vào vị trí Mid/Senior Data Engineer hay không.

### 1. Star Schema vs Snowflake Schema

```
Star Schema:                      Snowflake Schema:
      [ Dim_Date ]                      [ Dim_Date ]
          │                                 │
[ Dim_User ] ──► [ Fact_Sales ]       [ Dim_User ] ──► [ Fact_Sales ]
          ▲                                 ▲
      [ Dim_Product ]                       │
                                      [ Dim_Product ] ──► [ Dim_Category ]
```

*   **Star Schema (Sơ đồ hình sao)**:
    *   Gồm một **Fact table** ở giữa chứa các chỉ số đo lường (Metrics/Facts) và các cột khóa ngoại liên kết trực tiếp với các **Dimension tables** xung quanh.
    *   *Đặc trưng*: Các bảng Dimension bị phi chuẩn hóa (**Denormalized** - chấp nhận trùng lặp dữ liệu để giảm phép Join).
    *   *Ưu điểm*: Truy vấn cực kỳ nhanh, cấu trúc đơn giản, dễ hiểu đối với người phân tích dữ liệu (BI Analysts).
*   **Snowflake Schema (Sơ đồ bông tuyết)**:
    *   Biến thể của Star Schema nhưng các bảng Dimension được chuẩn hóa (**Normalized** - tách thành nhiều bảng nhỏ hơn, ví dụ bảng Product liên kết với bảng Category).
    *   *Ưu điểm*: Tiết kiệm dung lượng lưu trữ, tránh trùng lặp thông tin.
    *   *Nhược điểm*: Truy vấn chậm hơn vì phải thực hiện rất nhiều phép JOIN phức tạp.

---

### 2. Thay đổi chiều chậm (Slowly Changing Dimensions - SCD)
Làm thế nào để lưu lịch sử thay đổi của một khách hàng (ví dụ khách hàng thay đổi địa chỉ từ Hà Nội vào TP.HCM)?

*   **SCD Type 1**: Ghi đè (Overwrite). Dữ liệu cũ bị xóa hoàn toàn. Không lưu lại lịch sử.
*   **SCD Type 2**: Thêm dòng mới (Add a new row). Đây là phương pháp phổ biến nhất trong Data Warehouse. Sử dụng các cột chỉ thị trạng thái như `start_date`, `end_date`, và `is_current` (hoặc `active_flag`).

| User_ID | Name | City | Start_Date | End_Date | Is_Current |
| :--- | :--- | :--- | :--- | :--- | :--- |
| 1001 | Nguyễn Văn A | Hà Nội | 2020-01-01 | 2026-07-10 | False |
| 1001 | Nguyễn Văn A | TP.HCM | 2026-07-11 | NULL | True |

*   **SCD Type 3**: Thêm cột mới (Add a new column). Chỉ lưu trạng thái hiện tại và trạng thái gần nhất trước đó (`current_city`, `previous_city`).
*   **SCD Type 4**: Sử dụng bảng lịch sử riêng biệt (History table). Bảng Dimension chính chỉ giữ dữ liệu hiện tại, một bảng phụ lưu toàn bộ lịch sử thay đổi.

---

### 3. Thiết kế Data Warehouse cho Hệ thống Thương mại điện tử (Mẫu phỏng vấn)
**Yêu cầu**: Thiết kế mô hình dữ liệu cho bộ phận bán hàng thương mại điện tử phục vụ báo cáo doanh thu theo khu vực, thời gian, sản phẩm.

#### Thiết kế Star Schema:
*   **Fact_Orders**:
    *   `order_id` (PK)
    *   `customer_key` (FK to Dim_Customers)
    *   `product_key` (FK to Dim_Products)
    *   `date_key` (FK to Dim_Date)
    *   `quantity` (Measure)
    *   `unit_price` (Measure)
    *   `discount_amount` (Measure)
    *   `total_amount` (Measure = quantity * unit_price - discount_amount)
*   **Dim_Customers**:
    *   `customer_key` (Surrogate Key)
    *   `customer_id` (Natural Key từ hệ thống nguồn)
    *   `customer_name`, `email`, `phone`, `gender`
    *   `city`, `country` (Được denormalize vào đây)
*   **Dim_Products**:
    *   `product_key`
    *   `product_id`, `product_name`, `category_name`, `brand`
*   **Dim_Date**:
    *   `date_key` (Ví dụ dạng số: 20260712)
    *   `full_date`, `day_of_week`, `month`, `quarter`, `year`

---

## PHẦN 3: SQL NÂNG CAO CHO DATA ENGINEER

Bạn bắt buộc phải viết trơn tru các câu lệnh SQL sử dụng Window Functions, CTEs và tối ưu hóa câu hỏi phỏng vấn.

### 1. Các hàm Window Functions kinh điển
*   `ROW_NUMBER()`: Đánh số thứ tự tăng dần không trùng lặp cho các dòng trong phân vùng.
*   `RANK()`: Đánh số thứ tự có nhảy bậc nếu giá trị trùng nhau (ví dụ: 1, 2, 2, 4).
*   `DENSE_RANK()`: Đánh số thứ tự liên tục không nhảy bậc (ví dụ: 1, 2, 2, 3).
*   `LAG(col, N)`: Lấy giá trị của cột đó ở N dòng phía trước dòng hiện tại.
*   `LEAD(col, N)`: Lấy giá trị của cột đó ở N dòng phía sau dòng hiện tại.

**Bài toán**: Tìm giao dịch có số tiền lớn nhất của mỗi khách hàng trong ngày hôm nay.

```sql
WITH ranked_transactions AS (
    SELECT 
        user_id,
        amount,
        transaction_date,
        -- Phân vùng theo user và sắp xếp số tiền giảm dần
        DENSE_RANK() OVER(PARTITION BY user_id ORDER BY amount DESC) as rank_num
    FROM transactions
    WHERE DATE(transaction_date) = CURRENT_DATE
)
SELECT 
    user_id,
    amount,
    transaction_date
FROM ranked_transactions
WHERE rank_num = 1;
```

---

### 2. Recursive CTE (Truy vấn đệ quy)
Thường dùng để duyệt các dữ liệu có cấu trúc phân cấp (Cây thư mục, sơ đồ tổ chức nhân viên - quản lý).

**Bài toán**: Tìm toàn bộ cấp dưới của một người quản lý có ID = 1.

```sql
WITH RECURSIVE org_chart AS (
    -- Cú pháp Anchor: Điểm bắt đầu đệ quy
    SELECT employee_id, name, manager_id, 1 as level
    FROM employees
    WHERE employee_id = 1
    
    UNION ALL
    
    -- Cú pháp Recursive: Join lại với org_chart
    SELECT e.employee_id, e.name, e.manager_id, o.level + 1
    FROM employees e
    INNER JOIN org_chart o ON e.manager_id = o.employee_id
)
SELECT * FROM org_chart;
```

---

### 3. Tối ưu hóa truy vấn SQL (Query Optimization)
Làm thế nào để tăng tốc một câu lệnh SQL chạy chậm trong Data Warehouse?
1.  **Partition Pruning**: Lọc dữ liệu theo trường phân vùng trong câu lệnh `WHERE` (ví dụ lọc theo ngày tháng `WHERE date = '2026-07-12'`). Data Warehouse sẽ chỉ quét đúng thư mục chứa partition đó, giảm 99% lượng IOPS đọc đĩa.
2.  **Tránh dùng `SELECT *`**: Chỉ chọn các cột cần thiết. BigQuery/Snowflake lưu trữ dạng cột (Columnar format) nên chọn ít cột sẽ giảm lượng dữ liệu đọc và tiết kiệm chi phí.
3.  **Tránh join trên các trường không được Index/Sort Key**: Đảm bảo các khóa join là các khóa chính, khóa phụ hoặc các trường định danh dạng số (Integer) thay vì dạng String dài.
4.  **Hạn chế dùng hàm trong điều kiện Join/Filter**: Tránh viết `WHERE YEAR(date_col) = 2026` vì DB sẽ phải scan toàn bộ bảng và áp dụng hàm `YEAR()` cho từng dòng (Table Scan). Hãy đổi thành `WHERE date_col >= '2026-01-01' AND date_col <= '2026-12-31'`.
5.  **Dùng `EXPLAIN` hoặc `EXPLAIN ANALYZE`**: Đọc kế hoạch thực thi (Query Plan) để tìm các điểm nghẽn như: Table Scan (quét toàn bộ bảng), Hash Join lớn bị tràn bộ nhớ tạm ra đĩa cứng.

---

## PHẦN 4: PYTHON CHO DATA ENGINEERING

### 1. Xử lý file Parquet hiệu quả với PyArrow & Pandas
Khi làm việc với các file dữ liệu lớn, việc đọc toàn bộ file vào bộ nhớ RAM bằng Pandas thông thường dễ gây lỗi OOM. Ta cần áp dụng kỹ thuật đọc theo lô (chunks) và chỉ lấy các cột cần thiết.

```python
import pyarrow.parquet as pq
import pandas as pd

def process_large_parquet(file_path):
    # Sử dụng PyArrow để chỉ đọc các cột cần thiết mà không load toàn bộ file
    parquet_file = pq.ParquetFile(file_path)
    
    # In thông tin metadata của file Parquet
    print(f"Schema: {parquet_file.schema}")
    print(f"Number of row groups: {parquet_file.num_row_groups}")
    
    # Đọc dữ liệu theo từng Row Group (Batch processing)
    for i in range(parquet_file.num_row_groups):
        # Chỉ lấy cột 'user_id' và 'amount'
        df_group = parquet_file.read_row_group(i, columns=['user_id', 'amount']).to_pandas()
        
        # Xử lý tính toán trên Pandas
        df_agg = df_group.groupby('user_id').sum()
        
        # Lưu kết quả tạm thời hoặc ghi đè
        yield df_agg
```

---

### 2. Kiểm thử dữ liệu tự động với PyTest
Viết Unit Test để kiểm tra logic làm sạch dữ liệu trước khi golive pipeline.

```python
# file: clean_data.py
def clean_amount(val):
    if val is None:
        return 0
    try:
        val_float = float(val)
        return val_float if val_float >= 0 else 0
    except ValueError:
        return 0

# file: test_clean_data.py
import pytest
from clean_data import clean_amount

def test_clean_amount_valid():
    assert clean_amount("150.5") == 150.5

def test_clean_amount_null():
    assert clean_amount(None) == 0

def test_clean_amount_negative():
    assert clean_amount("-50") == 0

def test_clean_amount_invalid_string():
    assert clean_amount("abc") == 0
```
*Chạy test với lệnh:* `pytest test_clean_data.py`

---

### 3. Xác thực Schema dữ liệu đầu vào bằng Pydantic
Pydantic rất mạnh để kiểm tra chất lượng dữ liệu (Data Validation) khi nhận qua API REST.

```python
from pydantic import BaseModel, Field, field_validator
from datetime import datetime

class TransactionSchema(BaseModel):
    transaction_id: int
    user_id: int
    amount: float = Field(gt=0) # amount phải lớn hơn 0
    transaction_date: str

    @field_validator('transaction_date')
    def validate_date_format(cls, v):
        try:
            datetime.strptime(v, '%Y-%m-%d')
            return v
        except ValueError:
            raise ValueError('transaction_date must be in YYYY-MM-DD format')

# Test thử validator
try:
    bad_data = TransactionSchema(
        transaction_id=1,
        user_id=42,
        amount=-100.0, # Sẽ báo lỗi vì <= 0
        transaction_date="2026/07/12" # Sẽ báo lỗi vì sai format
    )
except Exception as e:
    print(f"Validation failed: {e}")
```

---

## PHẦN 5: DOCKER COMPOSE CHO MÔI TRƯỜNG ETL HOÀN CHỈNH

Dưới đây là một hệ sinh thái đầy đủ cho Data Engineer bao gồm: **Airflow** (Orchestrator) + **Spark** (Computing) + **PostgreSQL** (Source/DW) + **Redis** (Celery Broker).

```yaml
version: '3.8'

services:
  # 1. CSDL PostgreSQL dùng chung làm DW và Source DB
  postgres:
    image: postgres:13
    container_name: local-postgres
    ports:
      - "5432:5432"
    environment:
      POSTGRES_USER: dev_user
      POSTGRES_PASSWORD: dev_password
      POSTGRES_DB: local_dw
    volumes:
      - pgdata:/var/lib/postgresql/data

  # 2. Redis làm Broker cho Celery Executor của Airflow
  redis:
    image: redis:6.2-alpine
    container_name: local-redis
    ports:
      - "6379:6379"

  # 3. Apache Spark Master Node
  spark-master:
    image: bitnami/spark:3.4.1
    container_name: local-spark-master
    ports:
      - "8080:8080"
      - "7077:7077"
    environment:
      - SPARK_MODE=master

  # 4. Apache Spark Worker Node
  spark-worker:
    image: bitnami/spark:3.4.1
    container_name: local-spark-worker
    depends_on:
      - spark-master
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://spark-master:7077
      - SPARK_WORKER_CORES=2
      - SPARK_WORKER_MEMORY=2g

volumes:
  pgdata:
```

---

## PHẦN 6: THIẾT KẾ HỆ THỐNG PHỎNG VẤN (SYSTEM DESIGN INTERVIEWS)

### 1. Bài toán: Thiết kế hệ thống nạp dữ liệu gần thời gian thực (CDC Pipeline) từ OLTP Database sang Cloud OLAP Data Warehouse.

#### Kiến trúc Đề xuất:
```
[ OLTP Database (PostgreSQL) ]
              │
              ▼ (WAL logs)
[ Debezium (CDC Engine) on Kafka Connect ]
              │
              ▼ (Publishes raw events)
[ Apache Kafka Cluster ]
              │
              ▼ (Consumes & writes micro-batches in Parquet)
[ Apache Spark Streaming (on K8s/EMR) ]
              │
              ▼ (Merge updates/deletes into target tables)
[ Delta Lake / Iceberg (on S3) ]
              │
              ▼ (External table schema catalog)
[ AWS Glue Catalog ]
              │
              ▼ (Queries data via SQL interface)
[ Cloud Data Warehouse (Snowflake / AWS Redshift / Athena) ]
```

#### Các điểm then chốt cần trình bày với người phỏng vấn:
1.  **Tại sao dùng CDC thay vì Batch Query?**: Để tránh làm tải DB giao dịch (OLTP), Debezium chỉ đọc file log tuần tự của DB để lấy thay đổi.
2.  **Tại sao cần Kafka ở giữa?**: Kafka đóng vai trò là bộ đệm (Message Queue) chịu tải cực lớn. Nếu Spark Streaming bị sập tạm thời hoặc Data Warehouse bảo trì, dữ liệu thay đổi vẫn nằm an toàn trên Kafka chờ được tiêu thụ lại mà không bị mất.
3.  **Cách xử lý thay đổi schema (Schema Evolution)**: Sử dụng **Confluent Schema Registry** kết hợp cùng Kafka. Nếu DB nguồn thay đổi kiểu dữ liệu hoặc thêm cột, Schema Registry sẽ phát hiện và cập nhật schema tự động, tránh làm lỗi các downstream consumer.
4.  **Hỗ trợ ACID Transactions**: Delta Lake / Iceberg lưu trữ định dạng Parquet trên Object Storage (S3/GCS) và quản lý metadata bằng file json log. Nó cho phép thực hiện lệnh `MERGE INTO` (Upsert dữ liệu CDC từ Kafka) một cách nguyên tử (Atomicity), hỗ trợ đọc ghi đồng thời mà không bị hỏng file dữ liệu.
