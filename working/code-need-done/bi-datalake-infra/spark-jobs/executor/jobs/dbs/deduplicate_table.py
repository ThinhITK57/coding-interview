from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F
import re

table_name = "crm_raw.business_types"
key_col = "id"
ts_col = "updated_at_ts"  # đổi theo schema của bạn

# ==== 1. Lấy location từ Hive ====
ddl = spark.sql(f"SHOW CREATE TABLE {table_name}").collect()[0][0]

print(ddl)

# Get location
m = re.search(r"(external_location|location)\s*=\s*'([^']+)'", ddl, re.I)
if not m:
    raise ValueError(f"{table_name} không có location")
location = m.group(2)

print(f"Table location: {location}")

# ==== 2. Read data ====
df = spark.table(table_name)

# ==== 3. Deduplicate ====
if ts_col:
    window = Window.partitionBy(key_col).orderBy(F.col(ts_col).desc())

    df_dedup = (
        df.withColumn("rn", F.row_number().over(window))
          .filter(F.col("rn") == 1)
          .drop("rn")
    )
else:
    df_dedup = df.dropDuplicates([key_col])

# ==== 4. Repartition về 1 file ====
df_dedup = df_dedup.coalesce(1)

# ==== 5. Ghi lại S3 location ====
# ⚠️ overwrite trực tiếp, rủi ro mất dữ liệu
df_dedup.write \
    .mode("overwrite") \
    .format("parquet") \
    .format("parquet") \
    .option("path", location) \
    .saveAsTable(table_name)

print("Done dedup + write back")