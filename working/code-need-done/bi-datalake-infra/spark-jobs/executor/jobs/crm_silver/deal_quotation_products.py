# %livy.pyspark
target_path = "s3a://vcs-silver/crm-silver/deal_quotation_products"
target_table = "crm_silver.deal_quotation_products"

from pyspark.sql.types import *

product_schema = StructType([
    StructField("name", StringType(), True),
    StructField("basePrice", DoubleType(), True),
    StructField("quantitative", IntegerType(), True),
    StructField("unit", StringType(), True),
    StructField("duration", IntegerType(), True),
    StructField("package", StringType(), True),
    StructField("vat", IntegerType(), True),
    StructField("discount", DoubleType(), True),
    StructField("discountType", StringType(), True),
    StructField("finalTotal", DoubleType(), True),
    StructField("baseTotal", DoubleType(), True)
])

quotation_schema = ArrayType(
    StructType([
        StructField("quotation_id", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("status", StringType(), True),
        StructField("tLang", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("global_discount", DoubleType(), True),
        StructField("global_discountType", StringType(), True),
        StructField("products", ArrayType(product_schema), True)
    ])
)

from pyspark.sql.functions import from_json, col, explode

df_raw = spark.table("crm_raw.deals")

df_parsed = df_raw.withColumn(
    "quotation_json",
    from_json(col("custom_field.cf__quotations"), quotation_schema)
)

df_quotation = df_parsed.withColumn(
    "quotation",
    explode(col("quotation_json"))
)

df_products = df_quotation.withColumn(
    "product",
    explode(col("quotation.products"))
)

df_result = df_products.select(
    col("id").alias("deal_id"),
    # quotation level
    col("quotation.quotation_id"),
    # product level
    col("product.name").alias("product_name"),
    col("product.basePrice").alias("base_price"),
    col("product.quantitative"),
    col("product.unit"),
    col("product.duration"),
    col("product.package"),
    col("product.vat"),
    col("product.discount"),
    col("product.discountType").alias("discount_type"),
    col("product.finalTotal").alias("final_total_amount"),
    col("product.baseTotal").alias("base_total_amount")
)
# spark.sql("DROP TABLE IF EXISTS crm_silver.deal_quotation_products")

df_result.write \
    .mode("overwrite") \
    .format("parquet") \
    .option(
        "path",
        target_path
    ) \
    .saveAsTable(target_table)
