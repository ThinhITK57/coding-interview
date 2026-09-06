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


# ==============================================================================
# [PARAGRAPH 7]: TEST THUẬT TOÁN DEDUP TRÊN SPARK 2.3.2 (WINDOW RANKING)
# ==============================================================================
# %livy.spark
# Giả lập dữ liệu trùng lặp có nhiều phiên bản theo thời gian
duplicate_records = [
    {"sysid": "T-101", "name": "Task Ban Đầu", "last_modified": "2026-09-01T10:00:00Z", "status": "Draft"},
    {"sysid": "T-101", "name": "Task Cập Nhật Lần 1", "last_modified": "2026-09-03T12:00:00Z", "status": "In Progress"},
    {"sysid": "T-101", "name": "Task Hoàn Thành Cuối Cùng", "last_modified": "2026-09-06T18:00:00Z", "status": "Done"},
    {"sysid": "T-102", "name": "Task Độc Lập", "last_modified": "2026-09-05T09:00:00Z", "status": "New"},
]
dup_df = spark.read.json(spark.sparkContext.parallelize(duplicate_records))
print(f"Số bản ghi trước khi khử trùng lặp: {dup_df.count()}")

# Thuật toán Window Ranking Spark 2.3.2
from pyspark.sql.window import Window
w_spec = Window.partitionBy("sysid").orderBy(F.col("last_modified").desc())

deduped_test_df = (
    dup_df
    .withColumn("_rank", F.row_number().over(w_spec))
    .filter(F.col("_rank") == 1)
    .drop("_rank")
)

print(f"Số bản ghi sau khi khử trùng lặp: {deduped_test_df.count()}")
deduped_test_df.show(truncate=False)


# ==============================================================================
# [PARAGRAPH 8]: XỬ LÝ RACE CONDITION FACT-DIM (INFERRED DIMENSION PATTERN)
# ==============================================================================
# %livy.spark
# Giả sử Fact Tasks đến trước, có chứa Project ID chưa từng xuất hiện trong Dim Project
fact_with_fk = [
    {"task_id": "T-201", "c_project": "PRJ-9999_CHUA_CO"},
    {"task_id": "T-202", "c_project": "PRJ-9999_CHUA_CO"},
    {"task_id": "T-203", "c_project": "PRJ-EXISTING_DA_CO"},
]
fact_df = spark.read.json(spark.sparkContext.parallelize(fact_with_fk))

# Trích xuất các Project ID và sinh bản ghi Stub Inferred
distinct_projects = fact_df.select(F.col("c_project").alias("sysid")).distinct()
inferred_dim_stubs = (
    distinct_projects
    .withColumn("name", F.concat(F.lit("Inferred Stub ["), F.col("sysid"), F.lit("]")))
    .withColumn("last_modified", F.lit("1970-01-01T00:00:00Z"))
    .withColumn("_is_inferred", F.lit(True))
)

print("Inferred Dimension Stubs tự động sinh ra để chống Race Condition:")
inferred_dim_stubs.show(truncate=False)


# ==============================================================================
# [PARAGRAPH 9]: PHÂN LUỒNG RECORD LỖI VÀO DLQ (DEAD LETTER QUEUE)
# ==============================================================================
# %livy.spark
# Giả lập dữ liệu có dòng hợp lệ và dòng lỗi (thiếu sysid hoặc percent âm)
dirty_records = [
    {"sysid": "VALID-01", "percent_completed": 50.0},
    {"sysid": None, "percent_completed": 80.0},          # Lỗi: Null sysid
    {"sysid": "INVALID-02", "percent_completed": 150.0},  # Lỗi: Vượt quá 100%
]
dirty_df = spark.read.json(spark.sparkContext.parallelize(dirty_records))

# Gắn nhãn lỗi bằng single-pass concat_ws
error_tags = F.concat_ws(";",
    F.when(F.col("sysid").isNull(), F.lit("sysid:null")).otherwise(F.lit("")),
    F.when(F.col("percent_completed") > 100, F.lit("percent_completed:out_of_range")).otherwise(F.lit(""))
)

tagged_df = dirty_df.withColumn("_error_tags", error_tags)

valid_output = tagged_df.filter(F.length(F.col("_error_tags")) == 0).drop("_error_tags")
dlq_output = tagged_df.filter(F.length(F.col("_error_tags")) > 0)

print(f"Số bản ghi hợp lệ đi vào Clean Warehouse: {valid_output.count()}")
valid_output.show()

print(f"Số bản ghi lỗi bị cách ly vào Dead Letter Queue (DLQ): {dlq_output.count()}")
dlq_output.show(truncate=False)


# ==============================================================================
# [PARAGRAPH 10]: SINH KNOWLEDGE CONTEXT PACK CHO GENBI (AI LLM CHỮ + BIỂU ĐỒ)
# ==============================================================================
# %livy.spark
# Trích xuất metadata ngữ nghĩa phục vụ GenBI và AI LLM
import json

genbi_columns = {}
for field in clean_df.schema.fields:
    col_name = field.name
    t = field.dataType.simpleString()
    role = "dimension"
    agg = "none"
    synonyms = [col_name, col_name.replace("_", " ")]

    if any(k in col_name for k in ["percent", "rate", "ratio"]):
        role, agg = "y_axis", "avg"
        synonyms.extend(["tiến độ", "phần trăm hoàn thành", "tỷ lệ"])
    elif any(k in col_name for k in ["budget", "cost", "duration"]):
        role, agg = "y_axis", "sum"
        synonyms.extend(["ngân sách", "chi phí", "thời lượng"])
    elif any(k in col_name for k in ["state", "status"]):
        role, agg = "x_axis", "count"
        synonyms.extend(["trạng thái", "tình trạng"])
    elif any(k in col_name for k in ["date", "time"]):
        role = "x_axis"
        synonyms.extend(["thời gian", "ngày"])

    genbi_columns[col_name] = {
        "name": col_name,
        "data_type": t,
        "chart_role": role,
        "aggregation_type": agg,
        "synonyms": synonyms
    }

genbi_pack = {
    "table_name": "tasks",
    "trino_table_ref": "hive.global_clean.tasks",
    "semantic_layer": genbi_columns,
    "recommended_chart": {
        "type": "bar",
        "title": "Tiến độ công việc trung bình theo trạng thái",
        "echarts_spec": {
            "tooltip": {"trigger": "axis"},
            "xAxis": {"type": "category", "data": ["In Progress", "Completed", "Draft"]},
            "yAxis": {"type": "value", "name": "Tiến độ (%)"},
            "series": [{"data": [75.5, 100.0, 0.0], "type": "bar"}]
        }
    },
    "llm_few_shot": {
        "question": "Thống kê tiến độ các công việc hiện tại",
        "expected_sql": "SELECT state, AVG(percent_completed) FROM hive.global_clean.tasks GROUP BY state",
        "text_insights": "Hệ thống ghi nhận 100% công việc hoàn thành và các công việc đang chạy đạt 75.5%"
    }
}

print("=== GENBI CONTEXT PACK HOÀN TẤT CHO AI LLM ===")
print(json.dumps(genbi_pack, indent=2, ensure_ascii=False))


