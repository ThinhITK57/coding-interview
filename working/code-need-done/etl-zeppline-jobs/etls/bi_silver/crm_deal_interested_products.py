%livy.pyspark
target_path = "/opt/datasets/crawlers/vcs_silver/bi_silver/data/crm_deal_interested_products"
target_table = "bi_silver.crm_deal_interested_products"

from pyspark.sql.functions import (
    col, row_number, split, explode, trim
)
from pyspark.sql.window import Window

# ------------------------------------------------------------------
# 1. LẤY DEAL MỚI NHẤT
# ------------------------------------------------------------------
# Làm mới danh mục file nguồn
spark.sql("REFRESH TABLE crm_raw.deals")
spark.sql("REFRESH TABLE crm_raw.deleted_deals")
spark.catalog.clearCache()

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
            col("created_at").cast("timestamp").alias("created_at"),
            trim(col("interested_product")).alias("interested_product_category")
        )
)

# ------------------------------------------------------------------
# 3. GHI RA SILVER TABLE
# ------------------------------------------------------------------

(df_interested_products \
    .repartition(1) 
    .write \
    .mode("overwrite") \
    .format("parquet") \
    .save(target_path)
    # .option("path", target_path) \
    # .saveAsTable(target_table)
)

print("DONE: bi_silver.crm_deal_interested_products")
