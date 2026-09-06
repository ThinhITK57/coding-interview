# ==============================================================================
# APACHE ZEPPELIN INTERACTIVE TEST SUITE (%livy.spark)
# SPARK 2.3.2 + TRINO HIVE LAKEHOUSE INTERACTION
# ==============================================================================
# Hướng dẫn sử dụng:
# Mỗi khối mã dưới đây bắt đầu bằng `%livy.spark`. Bạn chỉ cần copy từng khối
# và paste vào một Paragraph trên Apache Zeppelin để chạy thử nghiệm độc lập.
# ==============================================================================

# ==============================================================================
# [PARAGRAPH 1]: KIỂM TRA MÔI TRƯỜNG LIVY & SPARK 2.3.2 VÀ HIVE METASTORE
# ==============================================================================
# %livy.spark
import sys
from datetime import datetime
import pyspark.sql.functions as F
from pyspark.sql.types import *

print(f"Python Version: {sys.version}")
print(f"Spark Version: {spark.version}")
print(f"Spark Master: {spark.sparkContext.master}")

# Kiểm tra danh sách databases hiện có trong Hive Metastore
try:
    databases = [row.databaseName for row in spark.sql("SHOW DATABASES").collect()]
    print(f"Accessible Databases in Hive Metastore: {databases}")
except Exception as e:
    print(f"Notice: Cannot query SHOW DATABASES (standalone mode): {e}")


# ==============================================================================
# [PARAGRAPH 2]: MẪU CẤU HÌNH BẢNG DỮ LIỆU TỪ BA (NGÀY MAI CHỈ CẦN PASTE VÀO ĐÂY)
# ==============================================================================
# %livy.spark
# Ngày mai khi BA cung cấp danh sách bảng & trường, bạn chỉ cần cập nhật dict này:
TABLE_CONFIG = {
    "table_name": "tasks",
    "entity_type": "Task",
    "primary_key": "sysid",
    "watermark_field": "last_modified",
    "fields": [
        "SYSID",
        "Name",
        "State",
        "PercentCompleted",
        "StartDate",
        "DueDate",
        "LastModified",
        "C_Project"
    ],
    "partition_col": "ingest_date"
}

print(f"Config loaded for table: {TABLE_CONFIG['table_name']}")
print(f"Target fields to extract ({len(TABLE_CONFIG['fields'])} fields): {TABLE_CONFIG['fields']}")


# ==============================================================================
# [PARAGRAPH 3]: TẠO DỮ LIỆU THỬ NGHIỆM MÔ PHỎNG API VÀ NẠP VÀO SPARK
# ==============================================================================
# %livy.spark
# Dữ liệu mẫu giả lập trả về từ API endpoint
sample_records = [
    {
        "SYSID": "TASK-1001",
        "Name": "Thiết lập hạ tầng Data Lakehouse Trino",
        "State": "In Progress",
        "PercentCompleted": 75.5,
        "StartDate": "2026-09-01T08:00:00Z",
        "DueDate": "2026-09-10T17:00:00Z",
        "LastModified": "2026-09-06T15:30:00Z",
        "C_Project": "PRJ-9901"
    },
    {
        "SYSID": "TASK-1002",
        "Name": "Tích hợp Apache Zeppelin Livy Interpreter",
        "State": "Completed",
        "PercentCompleted": 100.0,
        "StartDate": "2026-09-02T09:00:00Z",
        "DueDate": "2026-09-06T18:00:00Z",
        "LastModified": "2026-09-06T17:45:00Z",
        "C_Project": "PRJ-9901"
    }
]

# Chuyển thành Spark DataFrame
rdd = spark.sparkContext.parallelize(sample_records)
raw_json_df = spark.read.json(rdd)

# Chuẩn hóa tên cột thành snake_case theo chuẩn Enterprise
clean_df = raw_json_df
for col_name in clean_df.columns:
    clean_df = clean_df.withColumnRenamed(col_name, col_name.lower().replace(".", "_").replace(" ", "_"))

# Bổ sung các trường metadata cho Data Lakehouse
today_str = datetime.utcnow().strftime("%Y-%m-%d")
clean_df = (
    clean_df
    .withColumn("_batch_id", F.lit("BATCH_ZEPPELIN_LIVY_TEST_01"))
    .withColumn("_ingest_timestamp", F.current_timestamp())
    .withColumn(TABLE_CONFIG["partition_col"], F.to_date(F.lit(today_str)))
)

clean_df.printSchema()
clean_df.show(truncate=False)


# ==============================================================================
# [PARAGRAPH 4]: THỬ NGHIỆM GHI VÀO 3 NƠI (TRI-STORAGE SINK)
# ==============================================================================
# %livy.spark
# Đường dẫn lưu trữ (chỉnh theo đường dẫn S3A hoặc thư mục lakehouse của công ty)
S3_BASE = "s3a://lakehouse/warehouse"
LOCAL_FALLBACK_BASE = "./data/warehouse"

target_base = S3_BASE  # hoặc LOCAL_FALLBACK_BASE

# 1. Ghi vào Trino DB 1: personal_raw (bảng sandbox)
path_db1 = f"{target_base}/personal_raw/{TABLE_CONFIG['table_name']}"
(
    clean_df.write
    .mode("overwrite")
    .format("parquet")
    .option("compression", "snappy")
    .partitionBy(TABLE_CONFIG["partition_col"])
    .save(path_db1)
)
print(f"[OK] Successfully written to DB 1 (personal_raw): {path_db1}")

# 2. Ghi vào Trino DB 2: global_clean (bảng clean tổng)
path_db2 = f"{target_base}/global_clean/{TABLE_CONFIG['table_name']}"
(
    clean_df.write
    .mode("overwrite")
    .format("parquet")
    .option("compression", "snappy")
    .partitionBy(TABLE_CONFIG["partition_col"])
    .save(path_db2)
)
print(f"[OK] Successfully written to DB 2 (global_clean): {path_db2}")


# ==============================================================================
# [PARAGRAPH 5]: SINH CÂU LỆNH SQL DDL CHO TRINO.EXE
# ==============================================================================
# %livy.spark
# Tự động ánh xạ kiểu dữ liệu từ Spark 2.3.2 sang Trino SQL
TYPE_MAP = {
    "string": "VARCHAR",
    "integer": "INTEGER",
    "int": "INTEGER",
    "long": "BIGINT",
    "bigint": "BIGINT",
    "double": "DOUBLE",
    "float": "REAL",
    "boolean": "BOOLEAN",
    "timestamp": "TIMESTAMP",
    "date": "DATE"
}

table_name = TABLE_CONFIG["table_name"]
partition_col = TABLE_CONFIG["partition_col"]

cols = []
for f in clean_df.schema.fields:
    if f.name != partition_col:
        t_name = TYPE_MAP.get(f.dataType.simpleString(), "VARCHAR")
        cols.append(f"    {f.name} {t_name}")
cols.append(f"    {partition_col} DATE")

col_str = ",\n".join(cols)

trino_ddl_sql = f"""
-- ==============================================================================
-- CÂU LỆNH DDL DÙNG ĐỂ CHẠY TRÊN TRINO.EXE:
-- ==============================================================================

-- 1. Bảng Sandbox cá nhân (DB 1)
CREATE SCHEMA IF NOT EXISTS hive.personal_raw;

CREATE TABLE IF NOT EXISTS hive.personal_raw.{table_name} (
{col_str}
)
WITH (
    format = 'PARQUET',
    partitioned_by = ARRAY['{partition_col}'],
    external_location = '{S3_BASE}/personal_raw/{table_name}'
);

-- 2. Bảng Clean tổng (DB 2)
CREATE SCHEMA IF NOT EXISTS hive.global_clean;

CREATE TABLE IF NOT EXISTS hive.global_clean.{table_name} (
{col_str}
)
WITH (
    format = 'PARQUET',
    partitioned_by = ARRAY['{partition_col}'],
    external_location = '{S3_BASE}/global_clean/{table_name}'
);

-- 3. Cập nhật phân vùng trên Trino sau khi Spark ghi:
CALL hive.system.sync_partition_metadata('personal_raw', '{table_name}', 'ADD');
CALL hive.system.sync_partition_metadata('global_clean', '{table_name}', 'ADD');
"""

print(trino_ddl_sql)


# ==============================================================================
# [PARAGRAPH 6]: TRUY VẤN KIỂM TRA LẠI DỮ LIỆU VỪA GHI QUA SPARK SQL
# ==============================================================================
# %livy.spark
read_back_df = spark.read.parquet(path_db2)
print(f"Total rows read back: {read_back_df.count()}")
read_back_df.show(5, truncate=False)
