# %livy.pyspark
target_path = "s3a://vcs-silver/crm-silver/deal_products"
target_table = "crm_silver.deal_products"

spark.sql("REFRESH TABLE crm_raw.deals ")
spark.sql("REFRESH TABLE crm_raw.deleted_deals ")


df_last_deals = spark.sql("""
WITH latest_deals AS (
    SELECT * FROM 
            ( SELECT
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
      )
    WHERE rn = 1
),
last_deal_stages AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY id
                   ORDER BY updated_at_ts DESC, id DESC
               ) rn
        FROM crm_raw.deal_stages
    ) t
    WHERE rn = 1
)

SELECT
    s.id as deal_id,
    CAST(s.expected_close as DATE) as expected_close_date,
    CAST(s.closed_date as DATE) as closed_date,
    s.probability,
    s.custom_field.cf_channel as channel,
    s.custom_field.cf_presales as presales_name,
    s.custom_field.cf_project_manager as project_manager,
    s.custom_field.cf_partner as partner,
    s.custom_field.cf__territory as territory_name,
    trim(b.name)             AS deal_stage_name,

    s.custom_field.cf__products AS json_products
FROM latest_deals s
JOIN last_deal_stages b    ON s.deal_stage_id = b.id

""")


from pyspark.sql.types import (
    StructType, StructField,
    LongType, StringType, DoubleType, ArrayType, IntegerType, BooleanType
)

product_schema = ArrayType(
    StructType([
        StructField("id", LongType(), True),
        StructField("pid", LongType(), True),
        StructField("name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("version", StringType(), True),
        StructField("spdvType", StringType(), True),
        StructField("license", StringType(), True),
        StructField("quantitative", IntegerType(), True),
        StructField("min", IntegerType(), True),
        StructField("max", IntegerType(), True),
        StructField("unit", StringType(), True),
        StructField("package", StringType(), True),
        StructField("priceType", StringType(), True),
        StructField("duration", IntegerType(), True),
        StructField("allocationDuration", IntegerType(), True),
        StructField("allocationValue", DoubleType(), True),
        StructField("isQuantityBased", BooleanType(), True),
        StructField("maxDiscount", DoubleType(), True),
        StructField("vat", DoubleType(), True),
        StructField("discount", DoubleType(), True),
        StructField("discountType", StringType(), True),
        StructField("basePrice", DoubleType(), True),
        StructField("finalTotal", DoubleType(), True),
        StructField("currency", StringType(), True)
    ])
)

from pyspark.sql.functions import from_json, col, explode, coalesce, lit

df_products = (
    df_last_deals
        .withColumn(
            "products",
            from_json(
                coalesce(col("json_products"), lit("[]")),
                product_schema
            )
        )
        .withColumn("p", explode("products"))
        .select(
            col("deal_id").cast("string").alias("deal_id"),
            col("expected_close_date"),
            col("probability"),
            col("channel"),
            col("presales_name"),
            col("project_manager"),
            col("partner"),
            col("territory_name"),
            col("deal_stage_name"),
            col("p.id").alias("item_index"),
            col("p.pid").alias("product_id"),
            col("p.name").alias("product_name"),
            col("p.category").alias("category"),
            col("p.version").alias("version"),
            col("p.spdvType").alias("spdv_type"),
            col("p.license").alias("license"),
            col("p.quantitative").alias("quantitative"),
            col("p.min").cast("decimal(18,2)").alias("min_value"),
            col("p.max").cast("decimal(18,2)").alias("max_value"),
            col("p.unit").alias("product_unit"),
            col("p.package").alias("product_package"),
            col("p.priceType").alias("product_price_type"),
            col("p.duration").alias("duration"),
            col("p.allocationDuration").alias("allocation_duration"),
            col("p.allocationValue").cast("decimal(18,2)").alias("allocation_value"),
            col("p.isQuantityBased").alias("is_quantity_based"),
            col("p.maxDiscount").cast("decimal(18,2)").alias("max_discount"),
            col("p.vat").alias("vat"),
            col("p.discount").cast("decimal(18,2)").alias("discount"),
            col("p.discountType").alias("discount_type"),
            col("p.basePrice").cast("decimal(18,2)").alias("base_price"),
            col("p.finalTotal").cast("decimal(18,2)").alias("final_total"),
            col("p.currency").alias("currency")
        )
)

df_products \
    .repartition(1) \
    .write \
    .mode("overwrite") \
    .format("parquet") \
    .save(target_path)

    # .option("path", target_path) \
    # .saveAsTable(target_table)

print(target_table)