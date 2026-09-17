# %livy.pyspark
# OLD: crm_deals_allocation
# NEW: crm_contract_allocations

tgt_table = "bi_silver.crm_contract_allocations"
tgt_path  = "s3a://bi-silver/crm_contract_allocations"


# spark.sql("DROP TABLE IF EXISTS bi_silver.crm_deals_allocation")
# spark.sql("DROP TABLE IF EXISTS bi_silver.crm_contract_allocations")
# with abc as (select distinct contract_id, pricebook_id,invoice_activation_date, period_index from  hive.bi_silver.crm_contract_allocations )
# SELECT count(1) FROM abc;


from pyspark.sql import functions as F
from pyspark.sql.functions import (
    from_json, col, coalesce, when, trim, lit, current_date, datediff,
    to_date,to_timestamp, expr, lower, year, month
)
from pyspark.sql.types import ArrayType, StringType, StructType, StructField
from pyspark.sql.window import Window

# =========================
# 0) Helpers
# =========================
def norm_key(c):
    return F.regexp_replace(F.lower(F.trim(c)), r"\s+", " ")

def parse_flexible_ts(c):
    return coalesce(
        to_timestamp(to_date(c, "dd/MM/yyyy")),
        to_timestamp(to_date(c, "yyyy-MM-dd")),
        to_timestamp(to_date(c))
    )
# =========================
# 1) Base: latest deals + filter deleted (LEFT ANTI) + signed only
# =========================
df_raw_base = spark.sql("""
WITH deleted_ids AS (
  SELECT DISTINCT CAST(id AS BIGINT) AS id
  FROM crm_raw.deleted_deals
),
ranked_deals AS (
  SELECT
    s.*,
    ROW_NUMBER() OVER (
      PARTITION BY s.id
      ORDER BY s.updated_at_ts DESC, s.id DESC
    ) AS rn
  FROM crm_raw.deals s
  LEFT ANTI JOIN deleted_ids d
    ON s.id = d.id
  WHERE s.is_deleted = false
),
latest_deals AS (
  SELECT * FROM ranked_deals WHERE rn = 1
),
last_deal_stages AS (
  SELECT * FROM (
    SELECT
      ss.*,
      ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at_ts DESC, id DESC) rn
    FROM crm_raw.deal_stages ss
  ) t WHERE rn = 1
),
last_currencies AS (
  SELECT id, currency_code, exchange_rate
  FROM crm_raw.currencies
  WHERE is_active = true
),
last_cm_contracts AS (
  SELECT
    custom_field.cf__opp_id,
    custom_field.cf_sign_date,
    custom_field.cf_type
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY id
        ORDER BY updated_at_ts DESC, id DESC
      ) AS rn
    FROM crm_raw.cm_contracts
  ) t
  WHERE rn = 1
)

SELECT

  CAST(s.id AS STRING) AS deal_id,
  s.name AS deal_name,
  COALESCE(s.custom_field.cf_contract, 'N/A') as contract_id,
  lower(trim(b.name)) AS deal_stage_name,

  s.custom_field.cf_sale_admin as sale_admin,
  s.custom_field.cf__company AS cf_company,
  sha2(lower(trim(CAST(s.custom_field.cf_alias AS STRING))),256) as company_id,
  s.custom_field.cf_alias as company_alias,
  COALESCE(s.custom_field.cf__territory, 'Global Sale') AS territory_name,

  ROUND(CAST(s.base_currency_amount AS DOUBLE) / NULLIF(CAST(s.amount AS DOUBLE), 0), 6) AS change_rate,
  s.custom_field.cf__am AS cf_am,
  s.custom_field.cf__fac_date AS fac_date,

  s.custom_field.cf__products AS raw_cf_products,
  s.custom_field.cf__allocated_products AS raw_cf_allocated_products,
  s.custom_field.cf__allocated_records AS raw_cf_allocated_records,

  s.custom_field.cf__currency AS currency_code,
  COALESCE(TO_TIMESTAMP(TO_DATE(s.custom_field.cf__expire_date)), TO_TIMESTAMP(TO_DATE(s.closed_date))) as due_date,

  CAST(s.created_at as TIMESTAMP) as created_at,
  CAST(s.closed_date as TIMESTAMP) as closed_date,
  CAST(s.updated_at as TIMESTAMP) as updated_at,

  s.custom_field.cf_budget as budget_amount,
  
  CASE WHEN s.custom_field.cf_group  IS NULL or s.custom_field.cf_group = '' THEN 'Chưa xác định' ELSE trim(s.custom_field.cf_group)   END AS customer_group,

  CASE WHEN s.custom_field.cf_segment IS NULL THEN 'UNKNOWN'       ELSE s.custom_field.cf_segment END AS customer_segment_l1,
  COALESCE(s.custom_field.cf_segment2, '-')    AS customer_segment_l2,
  CASE WHEN s.custom_field.cf_segment3 IS NULL THEN 'UNKNOWN'      ELSE s.custom_field.cf_segment3 END AS customer_segment_l3,
  
  COALESCE(s.custom_field.cf_usd_to_vnd, 0) AS usd_to_vnd,
  CAST(lc.exchange_rate AS DOUBLE) AS vnd_to_usd,
  i.cf_type as contract_type,

  s.custom_field.cf_channel as channel

FROM latest_deals s
JOIN last_deal_stages b   ON s.deal_stage_id = b.id
LEFT JOIN last_currencies lc ON s.currency_id = lc.id
LEFT JOIN last_cm_contracts i ON CAST(s.id AS STRING) = CAST(i.cf__opp_id AS STRING)
WHERE b.name = 'Signed / Ký hợp đồng'
  AND s.custom_field.cf__fac_date IS NOT NULL
  AND TRIM(s.custom_field.cf__fac_date) <> ''
  AND s.custom_field.cf_contract IS NOT NULL
  AND TRIM(s.custom_field.cf_contract) <> ''
""")

# =========================
# 2) Schemas
# =========================
record_schema = ArrayType(StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("pid", StringType(), True),

    StructField("category", StringType(), True),
    StructField("territory", StringType(), True),

    StructField("totalVcsValue", StringType(), True),
    StructField("totalValue", StringType(), True),
    # StructField("vcsValue", StringType(), True),
    StructField("count", StringType(), True),

    StructField("forecastValue", StringType(), True),
    StructField("actualValue", StringType(), True),
    StructField("recurring", StringType(), True),
    StructField("allocationOverrides", StringType(), True),

    StructField("forecastStart", StringType(), True),
    StructField("actualStart", StringType(), True),
    StructField("period", StringType(), True),
    StructField("currency", StringType(), True)
]))

allo_schema = ArrayType(StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("pid", StringType(), True),

    StructField("category", StringType(), True),
    StructField("allocationValue", StringType(), True),
    StructField("type", StringType(), True),
    StructField("allocationDuration", StringType(), True),
    StructField("coefficient", StringType(), True),
    StructField("forecastDate", StringType(), True),
    StructField("actualDate", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("productType", StringType(), True),
    StructField("spdvType", StringType(), True),
    StructField("region", StringType(), True)
]))

prod_schema = ArrayType(StructType([
    StructField("id", StringType(), True), StructField("name", StringType(), True),
    StructField("pid", StringType(), True),
    StructField("category", StringType(), True), StructField("license", StringType(), True),
    StructField("quantitative", StringType(), True), StructField("unit", StringType(), True),
    StructField("package", StringType(), True), StructField("priceType", StringType(), True),
    StructField("duration", StringType(), True), StructField("isQuantityBased", StringType(), True),
    StructField("vat", StringType(), True), StructField("discount", StringType(), True),
    StructField("discountType", StringType(), True), StructField("basePrice", StringType(), True),
    StructField("baseTotal", StringType(), True), StructField("finalTotal", StringType(), True)
]))


# =========================
# 3) Canonical rec (dedup nhẹ trước join)
# =========================

# Danh sách sản phẩm trong giỏ hàng người dùng đã mua
deal_items_table = (
    df_raw_base
      .withColumn("prod_arr", from_json(col("raw_cf_products"), prod_schema))
      .select("deal_id", F.explode_outer("prod_arr").alias("items"))
      .withColumn("item_id", trim(col("items.id")))
      .withColumn("item_pid", trim(col("items.pid")))
      .withColumn("item_name", trim(col("items.name")))
      .withColumn("item_vat", trim(col("items.vat")))
      .withColumn("k_desc", norm_key(col("items.name")))
      .dropDuplicates(["deal_id", "item_id", "item_pid", "k_desc"])
)

# Danh sách sản phẩm và thông tin số lượng theo từng ITEMs
allocated_product_table = (
    df_raw_base
      .withColumn("allo_arr", from_json(col("raw_cf_allocated_products"), allo_schema))
      .select("deal_id", F.explode_outer("allo_arr").alias("item_detail"))
      .withColumn("item_id", trim(col("item_detail.id")))
      .withColumn("item_pid", trim(col("item_detail.pid")))
      .withColumn("item_name", trim(col("item_detail.name")))
      .withColumn("k_desc", norm_key(col("item_detail.name")))
      .withColumn(
          "alloc_key",
          F.concat_ws(
              "|",
              F.coalesce(col("deal_id").cast("string"), lit("")),
              F.coalesce(col("item_id").cast("string"), lit("")),
              F.coalesce(col("item_pid").cast("string"), lit(""))
          )
      )
      .dropDuplicates(["alloc_key"])
)

# Lần thanh toán đầu tiên
# allocated_first_product_record_table = (
#     df_raw_base
#       .withColumn("rec_arr", from_json(col("raw_cf_allocated_records"), record_schema))
#       .withColumn("first_product_record_table", F.explode_outer("rec_arr"))
#       .withColumn("product_category_index", col("first_product_record_table.id"))
#       .withColumn("product_description", col("first_product_record_table.name"))
#       .withColumn("k_id", norm_key(col("first_product_record_table.id")))
#       .withColumn("k_desc", norm_key(col("first_product_record_table.name")))
#       .filter(col("product_category_index").isNotNull())
#       .filter(trim(col("product_category_index")) != "")
#       .filter(col("product_description").isNotNull())
#       .filter(trim(col("product_description")) != "")
#       .dropDuplicates(["deal_id","k_id","k_desc"])   # CHỐNG nhân dòng từ rec trùng
# )
# Xử lý phân rã allocated_record
allocated_record_table = (
    df_raw_base
      .withColumn("rec_arr", from_json(col("raw_cf_allocated_records"), record_schema))
      .select(
          "deal_id",
          "deal_name",
          "deal_stage_name",
          "contract_id",
          "usd_to_vnd",
          "vnd_to_usd",
          "contract_type",
          "customer_segment_l1",
          "customer_segment_l2",
          "customer_segment_l3",
          "customer_group",
          "company_id",
          "cf_company",
          "company_alias",
          "currency_code",
          "cf_am",
          "due_date",
          "fac_date",
          "created_at",
          "updated_at",
          "closed_date",
          "sale_admin",
          "territory_name",
          F.explode_outer("rec_arr").alias("rec")
      )
      .withColumn("product_category_index", trim(col("rec.id")))
      .withColumn("product_description", trim(col("rec.name")))
      .withColumn("record_pid", trim(col("rec.pid")))
      .withColumn("k_desc", norm_key(col("rec.name")))
      .withColumn(
          "alloc_key",
          F.concat_ws(
              "|",
              F.coalesce(col("deal_id").cast("string"), lit("")),
              F.coalesce(col("product_category_index").cast("string"), lit("")),
              F.coalesce(col("record_pid").cast("string"), lit(""))
          )
      )
      .filter(col("product_category_index").isNotNull())
      .filter(trim(col("product_category_index")) != "")
      .filter(col("product_description").isNotNull())
      .filter(trim(col("product_description")) != "")
)
# =========================
# 4) Final
# =========================
df_final = (
    allocated_record_table.alias("r")
      .join(
          allocated_product_table.alias("a"),
          col("r.alloc_key") == col("a.alloc_key"),
          "left"
      )
      .join(
          deal_items_table.alias("p"),
          (
              (col("r.deal_id") == col("p.deal_id")) &
              (
                  (
                      col("r.product_category_index").eqNullSafe(col("p.item_id")) &
                      col("r.record_pid").eqNullSafe(col("p.item_pid"))
                  ) |
                  (
                      col("r.product_category_index").eqNullSafe(col("p.item_id")) &
                      col("r.k_desc").eqNullSafe(col("p.k_desc"))
                  )
              )
          ),
          "left"
      )
      .select(
          col("r.deal_id"),
          col("r.deal_name"),
          col("r.deal_stage_name"),
          col("r.contract_id"),

          col("r.usd_to_vnd"),
          col("r.vnd_to_usd"),
          col("r.contract_type"),

          col("r.customer_segment_l1"),
          col("r.customer_segment_l2"),
          col("r.customer_segment_l3"),
          col("r.customer_group"),

          col("r.company_id"),
          col("r.cf_company").alias("company_name"),
          col("r.company_alias"),

          col("r.currency_code"),
          col("r.cf_am").alias("am_username"),
          col("r.due_date"),

          parse_flexible_ts(col("r.fac_date")).alias("fac_date"),

          F.trim(
              coalesce(
                  col("r.record_pid").cast("string"),
                  col("a.item_pid").cast("string"),
                  col("p.item_pid").cast("string")
              )
          ).alias("pricebook_id"),

          col("r.product_category_index"),
          col("r.product_description"),
          col("r.rec.category").alias("product_category"),

          coalesce(col("a.item_detail.productType"), lit("N/A")).alias("product_type"),
          coalesce(col("a.item_detail.spdvType"), lit("N/A")).alias("deployment_type"),
          coalesce(expr("CAST(p.item_vat AS DECIMAL(18,2))"), lit(0).cast("decimal(18,2)")).alias("vat"),

          coalesce(expr("CAST(r.rec.recurring AS BOOLEAN)"), lit(False)).alias("is_recurring"),

          col("p.item_id").alias("product_index"),

          coalesce(col("r.territory_name"), lit("Global Sale")).alias("territory_name"),

          expr("CAST(coalesce(r.rec.totalVcsValue, 0) AS DECIMAL(18,2))").alias("product_total_value"),
          expr("CAST(coalesce(r.rec.totalValue, 0) AS DECIMAL(18,2))").alias("sales_performance_value"),
          # Bỏ logic này
          # expr("CAST(coalesce(r.rec.vcsValue, 0) AS DECIMAL(18,2))").alias("period_value"),
          F.floor(
            coalesce(expr("CAST(r.rec.totalVcsValue AS DECIMAL(18,2))"), lit(0).cast("decimal(18,2)")) /
                F.greatest(coalesce(expr("CAST(r.rec.count AS INT)"), lit(1)), lit(1))
            ).cast("BIGINT").alias("period_value"),

          coalesce(expr("CAST(r.rec.count AS INT)"), lit(1)).alias("period_number"),

          coalesce(col("r.rec.period"), lit("month")).alias("period_name"),
          # Chuẩn hóa actualStart từ dd/mm/yyyy sang timestamp và đặt tên là first_payment_date
          coalesce(
              parse_flexible_ts(col("r.rec.actualStart")), 
              parse_flexible_ts(col("r.fac_date"))
          ).alias("first_payment_date"),

          coalesce(
              parse_flexible_ts(col("r.rec.actualStart")),
              parse_flexible_ts(col("r.rec.forecastStart")),
              parse_flexible_ts(col("a.item_detail.actualDate")),
              parse_flexible_ts(col("a.item_detail.forecastDate"))
          ).alias("invoice_activation_date"),

          col("r.created_at"),
          col("r.updated_at"),
          col("r.closed_date"),
          datediff(current_date(), col("r.updated_at")).alias("days_since_last_update"),
          coalesce(col("r.sale_admin"), lit("-")).alias("sale_admin")
      )
)
# fac_date : Final Active contract date
# df_final = df_final.withColumn("first_payment_date", col("fac_date"))
# canonical dedup
# df_final = df_final.dropDuplicates(["deal_id","product_category_index","product_description"])
# lowercase columns
df_final = df_final.toDF(*[c.lower() for c in df_final.columns])

df_final = df_final.withColumn(
    "id",
    F.sha2(
        F.concat_ws(
            "||",
            F.coalesce(F.col("deal_id").cast("string"), F.lit("")),
            F.coalesce(
                F.col("product_index").cast("string"),
                F.col("product_category_index").cast("string"),
                F.lit("")
            ),
            F.coalesce(F.col("pricebook_id").cast("string"), F.lit("")),
            F.coalesce(F.col("product_description").cast("string"), F.lit("")),
            F.coalesce(F.col("invoice_activation_date").cast("string"), F.lit(""))
        ),
        256
    )
)
# Tạo danh sách hóa đơn cần thu theo kỳ  (tháng, quý, năm)
w_pos = Window.partitionBy(
    "deal_id",
    "product_category_index",
    "pricebook_id"
).orderBy(
    col("invoice_activation_date").asc(),
    col("product_description").asc()
)
df_final = df_final.withColumn("period_index", F.row_number().over(w_pos))

# =========================
# 5) Write: DROP + HARD DELETE PATH (CHỐNG DOUBLE FILE)
# =========================


# Write
df_final.repartition(1).write \
  .mode("overwrite") \
  .format("parquet") \
  .option("path", tgt_path) \
  .saveAsTable(tgt_table)

spark.catalog.refreshTable(tgt_table)
print(tgt_table)
print("Rows:", df_final.count())
