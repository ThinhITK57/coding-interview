%livy.pyspark
from pyspark.sql.types import *
from pyspark.sql.functions import from_json, col, explode, expr, to_timestamp


tgt_table = "bi_silver.crm_deal_quotations"
tgt_path  = "/opt/datasets/crawlers/vcs_silver/bi_silver/data/crm_deal_quotations"


spark.sql("REFRESH TABLE crm_raw.deals")

# -------------------------
# 1) Define schema
# -------------------------
product_schema = StructType([
    StructField("name", StringType(), True),
    StructField("basePrice", DecimalType(18,2), True),
    StructField("quantitative", IntegerType(), True),
    StructField("unit", StringType(), True),
    StructField("duration", IntegerType(), True),
    StructField("package", StringType(), True),
    StructField("vat", IntegerType(), True),
    StructField("discount", DecimalType(18,2), True),
    StructField("discountType", StringType(), True),
    StructField("finalTotal", DecimalType(18,2), True),
    StructField("baseTotal", DecimalType(18,2), True)
])

quotation_schema = ArrayType(
    StructType([
        StructField("quotation_id", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("status", StringType(), True),
        StructField("tLang", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("global_discount", DecimalType(18,2), True),
        StructField("global_discountType", StringType(), True),
        StructField("products", ArrayType(product_schema), True)
    ])
)

# -------------------------
# 2) Read + parse JSON
# -------------------------
df_raw = spark.table("crm_raw.deals")

df_parsed = df_raw.withColumn(
    "quotation_json",
    from_json(col("custom_field.cf__quotations"), quotation_schema)
)

df_quotation = df_parsed.withColumn(
    "quotation",
    explode(col("quotation_json"))
)

# currency mapping
df_quotation = df_quotation.withColumn(
    "currency_code",
    expr("""
        CASE
            WHEN lower(trim(quotation.currency)) IN ('đ', 'vnd') THEN 'VND'
            WHEN trim(quotation.currency) IN ('$', 'usd', 'USD') THEN 'USD'
            ELSE 'UNKNOWN'
               END
    """)
)

# cast created_at (string -> timestamp). Nếu format là '16:11:02 06/12/2025' thì dùng pattern này:
df_quotation = df_quotation.withColumn(
    "quotation_created_at",
    to_timestamp(col("quotation.created_at"), "HH:mm:ss dd/MM/yyyy")
)

df_result = df_quotation.select(
    col("id").cast("string").alias("deal_id"),

    col("quotation.quotation_id").alias("quotation_id"),
    col("quotation_created_at").alias("created_at"),
    col("quotation.status").alias("status"),
    col("quotation.tLang").alias("tLang"),
    col("currency_code"),
    expr("CAST( coalesce(quotation.global_discount, 0) AS DECIMAL(18,2))").alias("global_discount"),
    col("quotation.global_discountType").alias("global_discount_type")
).filter(
    col("quotation.quotation_id").isNotNull()
)

# -------------------------
# 3) Write (drop đúng table + hard delete path chống double)
# -------------------------


df_result.repartition(1).write \
    .mode("overwrite") \
    .format("parquet") \
    .save(tgt_path)
    # .option("path", tgt_path) \
    # .saveAsTable(tgt_table)

spark.catalog.refreshTable(tgt_table)

print(tgt_table)
print("Rows:", df_result.count())
