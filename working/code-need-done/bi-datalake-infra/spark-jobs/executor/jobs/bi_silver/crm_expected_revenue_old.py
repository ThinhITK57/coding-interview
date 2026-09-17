# %livy.pyspark
tgt_table = "bi_silver.crm_expected_revenue"
tgt_path  = "s3a://bi-silver/crm_expected_revenue"

# -- =========================================================
# -- Payment allocation logic
# -- =========================================================
# -- 1. Nếu period_number = 1:
# --    -> chỉ tạo 1 kỳ duy nhất tại first_payment_date
# --
# -- 2. Nếu period_number > 1:
# --    -> period_number được hiểu là số tháng phân bổ
# --    Trường hợp first_payment_date không phải ngày đầu tháng:
# --      -> tạo thêm 1 kỳ partial đầu tiên
# --    Ví dụ:
# --      first_payment_date = 2019-12-17
# --      period_number      = 12
# --
# --      Kỳ 1  : 2019-12-17 -> 2019-12-31
# --      Kỳ 2  : 2020-01-01 -> 2020-01-31
# --      ...
# --      Kỳ 12 : 2020-11-01 -> 2020-11-30
# --      Kỳ 13 : 2020-12-01 -> 2020-12-16
# --
# -- 3. Ngày kết thúc phân bổ:
# --
# --      contract_end_date =
# --          ADD_MONTHS(first_payment_date, period_number) - 1 day
# --
# -- 4. Tổng số ngày phân bổ:
# --
# --      contract_total_days =
# --          DATEDIFF(contract_end_date, first_payment_date) + 1
# --
# -- 5. Giá trị được chia theo số ngày thực tế:
# --
# --      daily_value =
# --          product_total_value / contract_total_days
# --
# --      used_days =
# --          DATEDIFF(period_end_date, period_start_date) + 1

# --      period_value =
# --          daily_value * used_days
# -- 6. Kỳ cuối sẽ nhận phần giá trị còn lại:
# --      last_period_value =
# --          total_value - SUM(previous_period_values)
# --    để đảm bảo:
# --      SUM(all periods) = product_total_value
#  -- =========================================================


from pyspark.sql import functions as F
from pyspark.sql.functions import (
    from_json, col, coalesce, when, trim, lit, current_date, datediff,
    to_date, to_timestamp, expr, year as f_year
)
from pyspark.sql.types import ArrayType, StringType, StructType, StructField
from pyspark.sql.window import Window

# =========================================================
# 0) Config
# =========================================================
spark.catalog.clearCache()
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

spark.sql("REFRESH TABLE crm_raw.deals")
spark.sql("REFRESH TABLE crm_raw.deleted_deals")
spark.sql("REFRESH TABLE crm_raw.deal_stages")
spark.sql("REFRESH TABLE crm_raw.cm_contracts")
spark.sql("REFRESH TABLE crm_raw.currencies")
spark.sql("REFRESH TABLE crm_raw.territories")
spark.sql("REFRESH TABLE bi_silver.crm_users")

# =========================================================
# 1) Helpers
# =========================================================
def norm_key(c):
    return F.regexp_replace(F.lower(F.trim(c)), r"\s+", " ")

def cut_item_id(c):
    # "8-1" -> "8", "8" -> "8"
    return F.split(F.trim(c.cast("string")), "-").getItem(0)

def parse_flexible_ts(c):
    return coalesce(
        to_timestamp(c, "dd/MM/yyyy HH:mm:ss"),
        to_timestamp(c, "yyyy-MM-dd HH:mm:ss"),
        to_timestamp(c, "yyyy-MM-dd'T'HH:mm:ss"),
        to_timestamp(to_date(c, "dd/MM/yyyy")),
        to_timestamp(to_date(c, "yyyy-MM-dd")),
        to_timestamp(to_date(c))
    )

# =========================================================
# 2) Base: latest deals + filter deleted + signed only
# =========================================================
df_raw_base = spark.sql("""
WITH deleted_ids AS (
  SELECT DISTINCT CAST(id AS BIGINT) AS id
  FROM crm_raw.deleted_deals
),
user_team AS (
    SELECT
        CAST(user_id AS STRING) AS am_user_id,
        CONCAT_WS(',', SORT_ARRAY(COLLECT_SET(TRIM(team_name)))) AS team_name
    FROM bi_silver.crm_users
    WHERE team_name IS NOT NULL
      AND TRIM(team_name) <> ''
    GROUP BY CAST(user_id AS STRING)
)                     
,
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
      ROW_NUMBER() OVER (
        PARTITION BY id
        ORDER BY updated_at_ts DESC, id DESC
      ) rn
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
    custom_field.cf_type,
    custom_field.cf_contract_id
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
  COALESCE(s.custom_field.cf_contract, 'N/A') AS contract_id,
  s.probability as probability,
  LOWER(TRIM(b.name)) AS deal_stage_name,

  s.custom_field.cf_sale_admin AS sale_admin,
  s.custom_field.cf__company AS cf_company,
  s.sales_account_id AS company_id,
  s.custom_field.cf_alias AS company_alias,

  COALESCE(s.custom_field.cf__territory, 'Global Sale') AS territory_name,

  ROUND(CAST(s.base_currency_amount AS DOUBLE) / NULLIF(CAST(s.amount AS DOUBLE), 0), 6) AS change_rate,
  s.custom_field.cf__am AS cf_am,
  s.owner_id AS am_user_id, 
  COALESCE(cu.team_name, '-') AS team_name,                      
  s.custom_field.cf__fac_date AS fac_date,
  s.closed_date AS sign_date,
  TO_TIMESTAMP(TO_DATE(s.custom_field.cf__expire_date)) AS contract_expire_date,                      

  s.custom_field.cf__products AS raw_cf_products,
  s.custom_field.cf__allocated_products AS raw_cf_allocated_products,
  s.custom_field.cf__allocated_records AS raw_cf_allocated_records,

  COALESCE(lc.currency_code, 'N/A') AS currency_code,
  COALESCE(
    TO_TIMESTAMP(TO_DATE(s.custom_field.cf__expire_date)),
    TO_TIMESTAMP(TO_DATE(s.closed_date))
  ) AS due_date,

  CAST(s.created_at AS TIMESTAMP) AS created_at,
  CAST(s.expected_close AS TIMESTAMP) AS expected_close_date,
  CAST(s.closed_date AS TIMESTAMP) AS closed_date,
  CAST(s.updated_at AS TIMESTAMP) AS updated_at,

  s.custom_field.cf_budget AS budget_amount,

  CASE
    WHEN s.custom_field.cf_group IS NULL OR s.custom_field.cf_group = '' THEN 'Unknown'
    ELSE TRIM(s.custom_field.cf_group)
  END AS customer_group,

  CASE
    WHEN s.custom_field.cf_segment IS NULL THEN 'UNKNOWN'
    ELSE s.custom_field.cf_segment
  END AS customer_segment_l1,

  COALESCE(s.custom_field.cf_segment2, '-')  AS customer_segment_l2,

  CASE
    WHEN s.custom_field.cf_segment3 IS NULL THEN '-'
    ELSE s.custom_field.cf_segment3
  END AS customer_segment_l3,

  COALESCE(s.custom_field.cf_usd_to_vnd, 0) AS usd_to_vnd,
  CAST(lc.exchange_rate AS DOUBLE) AS vnd_to_usd,
  i.cf_type AS contract_type,
  i.cf_contract_id AS contract_number,

  s.custom_field.cf_channel AS channel,
  COALESCE(internal_am_dictionary.internal_am_group, 
        CASE WHEN s.custom_field.cf_segment3 IN ( 'GOV', 'BFSI','Năng lượng', 'DNL' ) THEN s.custom_field.cf_segment3  ELSE null END
        ) as am_group,
  s.custom_field.cf__deal_type as deal_type
  , territories.name as am_territory_name
  , s.forecast_category as forecast_category
  ,CASE
    WHEN lower(trim(b.name)) in ('lost', 'new', 'quotation sent / báo giá', 'negotiation / đàm phán' ) THEN CAST(s.expected_close AS TIMESTAMP)
    WHEN lower(trim(b.name)) = 'signed / ký hợp đồng' AND ( s.custom_field.cf__fac_date is null OR s.custom_field.cf__fac_date = '')  THEN CAST(s.closed_date AS TIMESTAMP)
    WHEN lower(trim(b.name)) = 'signed / ký hợp đồng' AND s.custom_field.cf__fac_date is not null AND s.custom_field.cf__fac_date != ''  THEN CAST(s.custom_field.cf__fac_date AS TIMESTAMP)
    ELSE CAST(s.expected_close AS TIMESTAMP)
  END AS first_payment_date

FROM latest_deals s
JOIN last_deal_stages b   ON s.deal_stage_id = b.id 
LEFT JOIN last_currencies lc   ON s.currency_id = lc.id
LEFT JOIN last_cm_contracts i    ON CAST(s.id AS STRING) = CAST(i.cf__opp_id AS STRING)
LEFT JOIN user_team cu   ON CAST(s.owner_id AS STRING) = cu.am_user_id
LEFT JOIN crm_raw.internal_am_dictionary ON s.custom_field.cf_alias = internal_am_dictionary.company_alias
LEFT JOIN crm_raw.territories ON s.territory_id = territories.id

WHERE LOWER(TRIM(b.name)) NOT IN ('lost', 'signed / ký hợp đồng')
""")

# =========================================================
# 3) JSON Schemas
# =========================================================
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
    StructField("version", StringType(), True),
    StructField("spdvType", StringType(), True),
    StructField("region", StringType(), True)
]))

prod_schema = ArrayType(StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("pid", StringType(), True),
    StructField("category", StringType(), True),
    StructField("license", StringType(), True),
    StructField("quantitative", StringType(), True),
    StructField("unit", StringType(), True),
    StructField("package", StringType(), True),
    StructField("priceType", StringType(), True),
    StructField("duration", StringType(), True),
    StructField("isQuantityBased", StringType(), True),
    StructField("vat", StringType(), True),
    StructField("discount", StringType(), True),
    StructField("discountType", StringType(), True),
    StructField("basePrice", StringType(), True),
    StructField("baseTotal", StringType(), True),
    StructField("finalTotal", StringType(), True)
]))

# =========================================================
# 4) Canonical rec (dedup trước join)
# =========================================================
deal_items_table = (
    df_raw_base
      .withColumn("prod_arr", from_json(col("raw_cf_products"), prod_schema))
      .select("deal_id", F.explode_outer("prod_arr").alias("items"))

      .withColumn("item_id", cut_item_id(col("items.id")))
      .withColumn("item_pid", trim(col("items.pid")))

      .withColumn("item_name", trim(col("items.name")))
      .withColumn("item_vat", trim(col("items.vat")))
      .withColumn("k_desc", norm_key(col("items.name")))

      # JOIN KEY
      .withColumn(
          "alloc_key",
          F.concat_ws(
              "|",
              F.coalesce(col("deal_id").cast("string"), lit("")),
              F.coalesce(col("item_id").cast("string"), lit(""))
          )
      )

      # DEDUP KEY
      .dropDuplicates([
          "deal_id",
          "item_id",
          "item_pid"
      ])
)

allocated_product_table = (
    df_raw_base
      .withColumn("allo_arr", from_json(col("raw_cf_allocated_products"), allo_schema))
      .select("deal_id", F.explode_outer("allo_arr").alias("item_detail"))

      .withColumn("item_id", cut_item_id(col("item_detail.id")))
      .withColumn("item_pid", trim(col("item_detail.pid")))

      .withColumn("item_name", trim(col("item_detail.name")))
      .withColumn("product_version", trim(col("item_detail.version")))
      .withColumn("k_desc", norm_key(col("item_detail.name")))

      # JOIN KEY
      .withColumn(
          "alloc_key",
          F.concat_ws(
              "|",
              F.coalesce(col("deal_id").cast("string"), lit("")),
              F.coalesce(col("item_id").cast("string"), lit(""))
          )
      )

      # DEDUP KEY
      .dropDuplicates([
          "deal_id",
          "item_id",
          "item_pid"
      ])
)

allocated_record_table = (
    df_raw_base
      .withColumn("rec_arr", from_json(col("raw_cf_allocated_records"), record_schema))
      .select(
          "deal_id",
          "deal_name",
          "deal_stage_name",
          "probability",
          "contract_id",
          "contract_number",
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
          "team_name",
          "due_date",
          "fac_date",
          "sign_date",
          "contract_expire_date",
          "created_at",
          "expected_close_date",
          "updated_at",
          "closed_date",
          "sale_admin",
          "territory_name",
          "am_group",
          "deal_type",
          "channel",
          "am_territory_name",
          "forecast_category",
          "first_payment_date",
          F.explode_outer("rec_arr").alias("rec")
      )
      .withColumn("raw_product_category_index", trim(col("rec.id")))
      .withColumn("product_category_index", cut_item_id(col("rec.id")))
      .withColumn("product_description", trim(col("rec.name")))
      .withColumn("record_pid", trim(col("rec.pid")))
      .withColumn("k_desc", norm_key(col("rec.name")))
      .withColumn(
          "alloc_key",
          F.concat_ws(
              "|",
              F.coalesce(col("deal_id").cast("string"), lit("")),
              F.coalesce(col("product_category_index").cast("string"), lit(""))
          )
      )
      .filter(col("raw_product_category_index").isNotNull())
      .filter(trim(col("raw_product_category_index")) != "")
      .filter(col("product_category_index").isNotNull())
      .filter(trim(col("product_category_index")) != "")
      .filter(col("product_description").isNotNull())
      .filter(trim(col("product_description")) != "")
      .dropDuplicates([
          "deal_id",
          "raw_product_category_index",
          "record_pid"
      ])
)

# =========================================================
# 5) Build contract allocation in-memory only
# =========================================================
df_contract_alloc = (
    allocated_record_table.alias("r")
      .join(
          allocated_product_table.alias("a"),
          col("r.alloc_key") == col("a.alloc_key"),
          "left"
      )
      .join(
          deal_items_table.alias("p"),
            col("r.alloc_key") == col("p.alloc_key"),
          "left"
      )
      .select(
          col("r.deal_id"),
          col("r.deal_name"),
          col("r.deal_stage_name"),
          col("r.probability"),
          col("r.contract_id"),
          col("r.contract_number"),

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
          col("r.team_name").alias("team_name"),
          col("r.due_date"),

          parse_flexible_ts(col("r.fac_date")).alias("fac_date"),
          parse_flexible_ts(col("r.sign_date")).alias("sign_date"),
          col("r.contract_expire_date"),

          F.trim(
              coalesce(
                  col("r.record_pid").cast("string"),
                  col("a.item_pid").cast("string"),
                  col("p.item_pid").cast("string")
              )
          ).alias("pricebook_id"),

          col("r.product_category_index"),
          col("r.raw_product_category_index"),
          col("r.product_description"),
          col("r.rec.category").alias("product_category"),

          coalesce(col("a.item_detail.productType"), lit("-")).alias("product_type"),
          coalesce(col("a.product_version"), lit("-")).alias("product_version"),
          coalesce(col("a.item_detail.spdvType"), lit("-")).alias("deployment_type"),
          coalesce(expr("CAST(p.item_vat AS DECIMAL(18,2))"), lit(0).cast("decimal(18,2)")).alias("vat"),

          coalesce(expr("CAST(r.rec.recurring AS BOOLEAN)"), lit(False)).alias("is_recurring"),

          col("p.item_id").alias("product_index"),

          coalesce(col("r.territory_name"), lit("Global Sale")).alias("territory_name"),

          expr("CAST(coalesce(r.rec.totalVcsValue, 0) AS DECIMAL(18,2))").alias("product_total_value"),
          expr("CAST(coalesce(r.rec.totalValue, 0) AS DECIMAL(18,2))").alias("sales_performance_value"),
          # Bỏ logic này
          # expr("CAST(coalesce(r.rec.vcsValue, 0) AS DECIMAL(18,2))").alias("period_value"),
          coalesce(expr("CAST(r.rec.count AS INT)"), lit(1)).alias("period_number"),
          coalesce(col("r.rec.period"), lit("month")).alias("period_name"),

        #   Doanh thu dự kiến
          coalesce(
              parse_flexible_ts(col("r.rec.forecastStart"))
              , col("first_payment_date")
              ,parse_flexible_ts(col("r.rec.actualStart"))
          ).alias("first_payment_date"),
          when(
              parse_flexible_ts(col("r.rec.actualStart")).isNotNull() |
              parse_flexible_ts(col("r.fac_date")).isNotNull(),
              lit("Actual_revenue")
          ).otherwise(lit("Forecast_revenue")).alias("revenue_type"),
          
         coalesce(
              parse_flexible_ts(col("r.rec.forecastStart"))
              ,col("first_payment_date")
              ,parse_flexible_ts(col("r.rec.actualStart"))
          ).alias("carry_over_date"),
         
          parse_flexible_ts(col("r.rec.actualStart")).alias("actual_start_date"),
          parse_flexible_ts(col("r.rec.forecastStart")).alias("forecast_start_date"),

          coalesce(
              parse_flexible_ts(col("r.rec.actualStart")),
              parse_flexible_ts(col("r.rec.forecastStart")),
              parse_flexible_ts(col("a.item_detail.actualDate")),
              parse_flexible_ts(col("a.item_detail.forecastDate"))
          ).alias("invoice_activation_date"),

          col("r.channel"),
          col("r.am_group"),
          col("r.am_territory_name"),
          col("r.forecast_category"),
          col("r.deal_type"),
          col("r.created_at"),
          col("r.expected_close_date"),
          col("r.updated_at"),
          col("r.closed_date"),
          datediff(current_date(), col("r.updated_at")).alias("days_since_last_update"),
          coalesce(col("r.sale_admin"), lit("-")).alias("sale_admin")
      )
)

df_contract_alloc = df_contract_alloc.toDF(*[c.lower() for c in df_contract_alloc.columns])

df_contract_alloc = df_contract_alloc.withColumn(
    "id",
    F.sha2(
        F.concat_ws(
            "||",
            F.coalesce(F.col("deal_id").cast("string"), F.lit("")),
            F.coalesce(
                F.col("raw_product_category_index").cast("string"),
                F.col("product_index").cast("string"),
                F.col("product_category_index").cast("string"),
                F.lit("")
            ),
            F.coalesce(F.col("pricebook_id").cast("string"), F.lit("")),
            F.coalesce(F.col("product_description").cast("string"), F.lit("")),
            F.coalesce(F.col("first_payment_date").cast("string"), F.lit(""))
        ),
        256
    )
)

w_pos = Window.partitionBy(
    "deal_id",
    "raw_product_category_index",
    "pricebook_id"
).orderBy(
    col("invoice_activation_date").asc(),
    col("product_description").asc()
)
df_contract_alloc = df_contract_alloc.withColumn("period_index", F.row_number().over(w_pos))

# =========================================================
# 6) Expand directly to final payment forecast (Spark 2 compatible)
# =========================================================
df_contract_alloc.createOrReplaceTempView("tmp_contract_alloc")

query_payment_forecast = """
WITH base_1 AS (
    SELECT
        *,
        TO_DATE(first_payment_date) AS contract_start_date,
        TO_DATE(due_date) AS contract_due_date,

        CASE
            WHEN UPPER(TRIM(currency_code)) = 'USD'
                THEN CASE
                    WHEN CAST(usd_to_vnd AS DOUBLE) IS NULL OR CAST(usd_to_vnd AS DOUBLE) = 0
                        THEN 26000D
                    ELSE CAST(usd_to_vnd AS DOUBLE)
                END
            ELSE 1D
        END AS exchange_rate_fix,

        CASE
            WHEN period_number IS NULL OR period_number < 1 THEN 1
            ELSE CAST(period_number AS INT)
        END AS allocation_duration_fix
    FROM tmp_contract_alloc
    WHERE first_payment_date IS NOT NULL
      --AND due_date IS NOT NULL
      AND product_total_value IS NOT NULL
),

base AS (
    SELECT
        *,
        CASE
            WHEN allocation_duration_fix = 1 THEN 1
            WHEN DAY(contract_start_date) = 1 THEN allocation_duration_fix
            ELSE allocation_duration_fix + 1
        END AS total_payment_periods_fix
    FROM base_1
),

expanded AS (
    SELECT
        b.*,
        CAST(pos + 1 AS INT) AS payment_index
    FROM base b
    LATERAL VIEW POSEXPLODE(
        SPLIT(REPEAT(',', total_payment_periods_fix - 1), ',')
    ) e AS pos, val
),

periodized AS (
    SELECT
        *,

        CASE
            WHEN allocation_duration_fix = 1
                THEN contract_start_date
            WHEN payment_index = 1
                THEN contract_start_date
            ELSE ADD_MONTHS(TRUNC(contract_start_date, 'MM'), payment_index - 1)
        END AS payment_period_start_date,

        CASE
            WHEN allocation_duration_fix = 1
                THEN contract_start_date
            WHEN payment_index = total_payment_periods_fix
                THEN DATE_SUB(ADD_MONTHS(contract_start_date, allocation_duration_fix), 1)
            ELSE LAST_DAY(ADD_MONTHS(TRUNC(contract_start_date, 'MM'), payment_index - 1))
        END AS payment_period_end_date,

        CASE
            WHEN allocation_duration_fix = 1
                THEN 1
            ELSE DATEDIFF(
                DATE_SUB(ADD_MONTHS(contract_start_date, allocation_duration_fix), 1),
                contract_start_date
            ) + 1
        END AS contract_total_days
    FROM expanded
),
calc AS (
    SELECT
        *,

        CASE
            WHEN payment_period_end_date < payment_period_start_date THEN 0
            ELSE DATEDIFF(payment_period_end_date, payment_period_start_date) + 1
        END AS used_days,

        CAST(product_total_value AS DOUBLE) AS total_currency_amount,

        CASE
            WHEN UPPER(TRIM(currency_code)) = 'USD'
                THEN CAST(product_total_value AS DOUBLE) * exchange_rate_fix
            WHEN UPPER(TRIM(currency_code)) = 'VND'
                THEN CAST(product_total_value AS DOUBLE)
            ELSE CAST(product_total_value AS DOUBLE)
        END AS total_vnd_amount
    FROM periodized
    WHERE contract_total_days > 0
),

prorated AS (
    SELECT
        *,

        total_currency_amount / contract_total_days AS daily_vcs_base,
        total_vnd_amount / contract_total_days AS daily_vnd_base,

        SUM(ROUND((total_currency_amount / contract_total_days) * used_days, 2))
            OVER (
                PARTITION BY id
                ORDER BY payment_index
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ) AS prev_currency_amount,

        SUM(ROUND((total_vnd_amount / contract_total_days) * used_days, 2))
            OVER (
                PARTITION BY id
                ORDER BY payment_index
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ) AS prev_vnd_amount
    FROM calc
),

final_calc AS (
    SELECT
        *,

        CASE
            WHEN payment_index = total_payment_periods_fix
                THEN total_currency_amount - COALESCE(prev_currency_amount, 0D)
            ELSE ROUND(daily_vcs_base * used_days, 2)
        END AS currency_amount_prorated,

        CASE
            WHEN payment_index = total_payment_periods_fix
                THEN total_vnd_amount - COALESCE(prev_vnd_amount, 0D)
            ELSE ROUND(daily_vnd_base * used_days, 2)
        END AS vnd_amount_prorated
    FROM prorated
)

SELECT
    CAST(payment_period_start_date AS TIMESTAMP) AS payment_date,
    CAST(payment_period_start_date AS TIMESTAMP) AS payment_period_start_date,
    CAST(payment_period_end_date AS TIMESTAMP) AS payment_period_end_date,

    CAST(deal_id AS STRING) AS deal_id,
    CURRENT_TIMESTAMP() AS update_at,
    product_total_value,

    CAST(COALESCE(currency_amount_prorated, 0) AS DECIMAL(18,2)) AS currency_amount,
    CAST(COALESCE(vnd_amount_prorated, 0) AS DECIMAL(18,2)) AS vnd_amount,

    CASE
        when YEAR(carry_over_date) = YEAR(payment_period_start_date)
        THEN TRUE
        ELSE FALSE
    END AS is_in_year,
    carry_over_date,
    actual_start_date,
    forecast_start_date,

    CAST(due_date AS TIMESTAMP) AS due_date,
    CAST(sign_date AS TIMESTAMP) AS sign_date,
    CAST(expected_close_date AS TIMESTAMP) AS expected_close_date,
    CAST(fac_date AS TIMESTAMP) AS fac_date,
    CAST(contract_expire_date AS TIMESTAMP) AS contract_expire_date,

    CAST(vat AS DECIMAL(10,2)) AS vat,

    CAST(product_type AS STRING) AS product_type,
    CAST(product_version AS STRING) AS product_version,
    CAST(deployment_type AS STRING) AS deployment_type,
    CAST(product_category AS STRING) AS product_category,
    CAST(pricebook_id AS STRING) AS pricebook_id,
    CAST(is_recurring AS BOOLEAN) AS is_recurring,
    COALESCE(territory_name, 'Others') AS territory_name,
    CAST(product_index AS STRING) AS product_index,

    CAST(company_id AS STRING) AS customer_id,
    COALESCE(company_name, 'N/A') AS customer_name,
    COALESCE(company_alias, 'N/A') AS company_alias,
    COALESCE(customer_group, 'Chưa xác định') AS customer_group,

    CAST(raw_product_category_index AS STRING) AS product_category_index,
    UPPER(TRIM(currency_code)) AS currency_code,
    CAST(exchange_rate_fix AS DECIMAL(18,2)) AS exchange_rate,

    CAST(deal_stage_name AS STRING) AS deal_stage_name,
    CAST(probability AS DOUBLE) AS probability,
    COALESCE(am_username, '-') AS am_username,
    COALESCE(team_name, '-') AS team_name,
    CAST(contract_id AS STRING) AS contract_id,
    contract_number,

    CAST(payment_index AS BIGINT) AS payment_index,
    CAST(used_days AS BIGINT) AS used_days,
    CAST(contract_total_days AS BIGINT) AS contract_total_days,

    CAST(revenue_type AS STRING) AS revenue_type,

    COALESCE(customer_segment_l1, '-') AS customer_segment_l1,
    COALESCE(customer_segment_l2, '-') AS customer_segment_l2,
    COALESCE(customer_segment_l3, '-') AS customer_segment_l3,
    
    COALESCE(channel, 'N/A') AS channel_name,
    
    COALESCE(sale_admin, '-') AS sales_admin_username,
    CAST(id AS STRING) AS contract_allocation_id,
    am_group,
    deal_type
    ,am_territory_name
    ,forecast_category

FROM final_calc
"""

df_payment_forecast = spark.sql(query_payment_forecast)

# =========================================================
# 7) Keep exact final column order
# =========================================================
final_cols = [
    "payment_date",
    "payment_period_start_date",
    "payment_period_end_date",
    "deal_id",
    "update_at",
    "product_total_value",
    "currency_amount",
    "vnd_amount",
    "currency_code",
    "payment_index",
    "used_days",
    "exchange_rate",
    "carry_over_date",
    "actual_start_date",
    "forecast_start_date",
    "is_in_year",
    "due_date",
    "sign_date",
    "expected_close_date",
    "contract_expire_date",
    "fac_date",
    "vat",
    "product_type",
    "product_version",
    "deployment_type",
    "product_category",
    "product_category_index",
    "pricebook_id",
    "is_recurring",
    "territory_name",
    "product_index",
    "customer_id",
    "customer_name",
    "company_alias",
    "customer_group",
    "deal_stage_name",
    "probability",
    "am_username",
    "team_name",
    "contract_id",
    "contract_number",
    "contract_total_days",
    "revenue_type",
    "customer_segment_l1",
    "customer_segment_l2",
    "customer_segment_l3",
    "sales_admin_username",
    "contract_allocation_id",
    "channel_name",
    "am_group",
    "deal_type",
    "am_territory_name",
    "forecast_category",
]

df_payment_forecast = df_payment_forecast.select(*final_cols)

# =========================================================
# 8) Write final table only
# =========================================================
df_payment_forecast.write \
    .mode("overwrite") \
    .format("parquet") \
    .save(tgt_path)
    # .option("path", tgt_path) \
    # .saveAsTable(tgt_table)

spark.catalog.refreshTable(tgt_table)

print(tgt_table)
print("--------------------------------------------------")
print("THANH CONG: crm_expected_revenue da duoc cap nhat")
print("Rows:", df_payment_forecast.count())
print("--------------------------------------------------")