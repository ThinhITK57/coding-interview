%livy.pyspark
target_path  = "/opt/datasets/crawlers/vcs_silver/crm_silver/data/deal_allocated_records"
target_table = "crm_silver.deal_allocated_records"


spark.sql("REFRESH TABLE crm_raw.deals ")
spark.sql("REFRESH TABLE crm_raw.deleted_deals ")


from pyspark.sql.functions import (
    col, row_number, from_json, explode, lit, coalesce
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, DoubleType, IntegerType, BooleanType, ArrayType
)


import unicodedata
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

def normalize_vietnamese(value):
    if value is None:
        return None
    return unicodedata.normalize("NFC", value)

normalize_udf = F.udf(normalize_vietnamese, StringType())

# ------------------------------------------------------------------
# 1. LẤY DEAL MỚI NHẤT
# ------------------------------------------------------------------

df_last_deals = spark.sql("""
SELECT
    ss.*,
    ROW_NUMBER() OVER (
        PARTITION BY id
        ORDER BY updated_at_ts DESC, id DESC
    ) AS rn
FROM crm_raw.deals ss
WHERE ss.is_deleted = false
  AND NOT EXISTS (
      SELECT 1
      FROM crm_raw.deleted_deals d
      WHERE CAST(d.id AS BIGINT) = ss.id
  )
""").filter(col("rn") == 1)

# ------------------------------------------------------------------
# 2. SCHEMA CHO allocated_records (ARRAY JSON)
# ------------------------------------------------------------------

allocated_record_schema = StructType([
    StructField("id", LongType(), True),
    StructField("pid", LongType(), True),
    StructField("name", StringType(), True),

    StructField("category", StringType(), True),
    StructField("version", StringType(), True),
    StructField("territory", StringType(), True),
    StructField("recurring", BooleanType(), True),

    StructField("totalVcsValue", DoubleType(), True),
    StructField("totalValue", DoubleType(), True),
    StructField("count", IntegerType(), True),

    StructField("currency", StringType(), True),
    StructField("period", StringType(), True),

    StructField("forecastStart", StringType(), True),
    StructField("actualStart", StringType(), True),

    StructField("vcsValue", DoubleType(), True),
    StructField("forecastValue", DoubleType(), True),
    StructField("actualValue", DoubleType(), True),

    StructField("allocationOverrides", ArrayType(StringType()), True)
])

allocated_records_schema = ArrayType(allocated_record_schema)

# ------------------------------------------------------------------
# 3. PARSE JSON + EXPLODE
# ------------------------------------------------------------------

df_allocated_records = (
    df_last_deals
        .withColumn(
            "allocated_records_array",
            from_json(
                coalesce(col("custom_field.cf__allocated_records"), lit("[]")),
                allocated_records_schema
            )
        )
        .withColumn("rec", explode(col("allocated_records_array")))
        .select(
            col("id").cast("string").alias("deal_id"),

            col("rec.id").alias("record_index"),
            col("rec.pid").alias("product_id"),
            col("rec.name").alias("product_name"),

            col("rec.category").alias("product_category"),
            col("rec.version").alias("product_version"),
            col("rec.territory").alias("territory"),
            col("rec.recurring").alias("is_recurring"),

            col("rec.totalVcsValue").alias("total_vcs_value"),
            col("rec.totalValue").alias("total_value"),
            col("rec.count").alias("allocation_count"),

            col("rec.currency").alias("currency"),
            col("rec.period").alias("period"),

            col("rec.forecastStart").alias("forecast_start_date"),
            col("rec.actualStart").alias("actual_start_date"),

            col("rec.vcsValue").alias("vcs_value"),
            col("rec.forecastValue").alias("forecast_value"),
            col("rec.actualValue").alias("actual_value"),
            col("rec.allocationOverrides").alias("allocation_overrides"),
        )
)

# ------------------------------------------------------------------
# 4. GHI RA SILVER TABLE
# ------------------------------------------------------------------

df_allocated_records = df_allocated_records.withColumn(
    "product_category",
    normalize_udf(F.col("product_category"))
)

df_allocated_records \
    .repartition(1) \
    .write \
    .mode("overwrite") \
    .format("parquet") \
    .option(
        "path",
        target_path
    ) \
    .saveAsTable(target_table)

print("DONE: crm_silver.deal_allocated_records")
