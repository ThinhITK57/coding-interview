%livy.pyspark
from pyspark.sql.types import *
from pyspark.sql.functions import from_json, col, explode, expr, to_timestamp, row_number, coalesce, lit
from pyspark.sql.window import Window

# -------------------------
# 5) Write (drop + hard delete path để CHỐNG DOUBLE)
# -------------------------
tgt_table = "bi_silver.crm_deal_quotation_products"
tgt_path  = "/opt/datasets/crawlers/vcs_silver/bi_silver/data/crm_deal_quotation_products"

spark.sql("REFRESH TABLE crm_raw.deals")
spark.sql("REFRESH TABLE crm_raw.deals")

# -------------------------
# 1) Schemas
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
# 2) Read + parse
# -------------------------
# Làm mới danh mục file nguồn
spark.sql("REFRESH TABLE crm_raw.deals")
spark.catalog.clearCache()

df_raw = spark.table("crm_raw.deals")

df_parsed = df_raw.withColumn(
    "quotation_json",
    from_json(col("custom_field.cf__quotations"), quotation_schema)
)

df_quotation = df_parsed.withColumn("quotation", explode(col("quotation_json")))

df_quotation = df_quotation.withColumn(
    "currency_code",
    expr("""
      CASE
        WHEN lower(trim(quotation.currency)) IN ('đ', 'vnd') THEN 'VND'
        WHEN lower(trim(quotation.currency)) IN ('$', 'usd') THEN 'USD'
        ELSE 'UNKNOWN'
      END
    """)
)

df_quotation = df_quotation.withColumn(
    "quotation_created_at",
    to_timestamp(col("quotation.created_at"), "HH:mm:ss dd/MM/yyyy")
)

df_products = df_quotation.withColumn("product", explode(col("quotation.products")))

# -------------------------
# 3) Tạo keys trước + window row_number TRƯỚC khi select
# -------------------------
df_products2 = df_products \
    .withColumn("deal_id", col("id").cast("string")) \
    .withColumn("quotation_id", col("quotation.quotation_id"))

w = Window.partitionBy("deal_id", "quotation_id").orderBy(
    coalesce(col("product.name"), lit("")).asc(),
    coalesce(col("product.basePrice").cast("double"), lit(-1e18)).desc(),
    coalesce(col("product.quantitative").cast("int"), lit(-2147483648)).desc()
)

df_products2 = df_products2.withColumn("product_line_no", row_number().over(w))

# -------------------------
# 4) Select output + sinh id hash
# -------------------------
df_result = df_products2.select(
    "deal_id",
    "quotation_id",
    "product_line_no",
    lit("-1").alias("product_id"),

    col("quotation_created_at").alias("quotation_created_at"),
    col("quotation.status").alias("quotation_status"),
    col("quotation.tLang").alias("quotation_lang"),
    coalesce(col("currency_code"), lit('VND')).alias("currency_code"),
    expr("CAST( coalesce(quotation.global_discount, 0) AS DECIMAL(18,2))").alias("global_discount"),
    coalesce(col("quotation.global_discountType"), lit('N/A')).alias("global_discount_type"),

    coalesce(col("product.name"), lit('N/A')).alias("product_name"),
    expr("CAST( coalesce(product.basePrice, 0) AS DECIMAL(18,2))").alias("base_price"),
    coalesce(col("product.quantitative"), lit(0)).cast("int").alias("quantitative"),
    col("product.unit").alias("unit"),
    col("product.unit").alias("product_unit"),
    coalesce(col("product.duration"), lit(-1)).cast("int").alias("duration"),
    col("product.package").alias("package"),
    coalesce(col("product.vat"), lit(0)).cast("int").alias("vat"),
    expr("CAST( coalesce(product.discount, 0) AS DECIMAL(18,2))").alias("discount_amount"),
    col("product.discountType").alias("discount_type"),
    expr("CAST( coalesce(product.finalTotal, 0) AS DECIMAL(18,2))").alias("final_total_amount"),
    expr("CAST( coalesce(product.baseTotal, 0) AS DECIMAL(18,2))").alias("base_total_amount"),
).withColumn(
    "id",
    expr("sha2(concat_ws('||', deal_id, quotation_id, cast(product_line_no as string)), 256)")
)



# spark.sql("DROP TABLE IF EXISTS " + tgt_table)

# 3) Write
df_result.repartition(1).write \
  .mode("overwrite") \
  .format("parquet") \
  .save(tgt_path)
#   .option("path", tgt_path) \
#   .saveAsTable(tgt_table)


spark.catalog.refreshTable(tgt_table)

print(tgt_table)
print("Rows:", df_result.count())
spark.sql("""
  SELECT deal_id, quotation_id, product_line_no, product_name, base_price
  FROM bi_silver.crm_deal_quotation_products
  LIMIT 10
""").show(truncate=False)
