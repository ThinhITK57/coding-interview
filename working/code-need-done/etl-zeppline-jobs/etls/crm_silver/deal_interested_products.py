%livy.pyspark

from pyspark.sql.functions import (
    col, row_number, split, explode, trim
)
from pyspark.sql.window import Window

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
# 2. SPLIT CSV + EXPLODE
# ------------------------------------------------------------------

df_interested_products = (
    df_last_deals
        .withColumn(
            "interested_product",
            explode(
                split(col("custom_field.cf_interested_products"), ";")
            )
        )
        .select(
            col("id").cast("string").alias("deal_id"),
            trim(col("interested_product")).alias("interested_product_category")
        )
)

# ------------------------------------------------------------------
# 3. GHI RA SILVER TABLE
# ------------------------------------------------------------------

df_interested_products \
    .repartition(5) \
    .write \
    .mode("overwrite") \
    .format("parquet") \
    .option(
        "path",
        "/opt/datasets/crawlers/vcs_silver/crm_silver/data/deal_interested_products"
    ) \
    .saveAsTable("crm_silver.deal_interested_products")

print("DONE: crm_silver.deal_interested_products")
