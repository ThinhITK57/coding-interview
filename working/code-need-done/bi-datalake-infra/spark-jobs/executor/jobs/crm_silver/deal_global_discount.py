# %livy.pyspark


from pyspark.sql.functions import (
    col, row_number, from_json, lit, coalesce
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField,
    DoubleType, StringType
)

# ------------------------------------------------------------------
# 1. LẤY BẢN GHI DEAL MỚI NHẤT
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
# 2. SCHEMA CHO global_discount
# {"value":0,"type":"percent"}
# ------------------------------------------------------------------

global_discount_schema = StructType([
    StructField("value", DoubleType(), True),
    StructField("type", StringType(), True)
])

# ------------------------------------------------------------------
# 3. PARSE JSON + SELECT OUTPUT
# ------------------------------------------------------------------

df_global_discount = (
    df_last_deals
        .withColumn(
            "global_discount_struct",
            from_json(
                coalesce(col("custom_field.cf__global_discount"), lit('{}')),
                global_discount_schema
            )
        )
        .select(
            col("id").cast("string").alias("deal_id"),
            col("global_discount_struct.value").alias("global_discount_value"),
            col("global_discount_struct.type").alias("global_discount_type")
        )
)

# ------------------------------------------------------------------
# 4. GHI RA HIVE TABLE (TUỲ CHỌN)
# ------------------------------------------------------------------

df_global_discount \
    .repartition(1) \
    .write \
    .mode("overwrite") \
    .format("parquet") \
    .option(
        "path",
        "s3a://vcs-silver/crm-silver/deal_global_discount"
    ) \
    .saveAsTable("crm_silver.deal_global_discounts")

print("DONE: crm_silver.deal_global_discount")

