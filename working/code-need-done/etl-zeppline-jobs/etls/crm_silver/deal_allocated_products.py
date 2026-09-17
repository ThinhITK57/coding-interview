%livy.pyspark
target_path = "/opt/datasets/crawlers/vcs_silver/crm_silver/data/deal_allocated_records"
target_table = "crm_silver.deal_allocated_products"

import unicodedata
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

def normalize_vietnamese(value):
    if value is None:
        return None
    return unicodedata.normalize("NFC", value)

normalize_udf = F.udf(normalize_vietnamese, StringType())

spark.sql("REFRESH TABLE crm_raw.deals ")
spark.sql("REFRESH TABLE crm_raw.deleted_deals ")

from pyspark.sql.functions import (
    col, row_number, from_json, explode, lit, coalesce
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, DoubleType, IntegerType, ArrayType
)
from pyspark.sql.functions import regexp_replace

import json

SOC_PRODUCTS = set([
    "VCS-AJIANT",
    "VCS-NSM",
    "VCS-CYM",
])

def get_soc_group(allocated_products):
    if not allocated_products:
        return None

    try:
        products = json.loads(allocated_products)

        if not isinstance(products, list):
            return None

        categories = set(
            str(item.get("category")).strip().upper()
            for item in products
            if isinstance(item, dict) and item.get("category")
        )

        if any("MSS" in category for category in categories):
            return True

        if SOC_PRODUCTS.issubset(categories):
            return True

        return False

    except Exception:
        return False


get_soc_group_udf = F.udf(
    get_soc_group,
    StringType()
)



# ------------------------------------------------------------------
# 1. LẤY DEAL MỚI NHẤT
# ------------------------------------------------------------------

df_last_deals = spark.sql("""
WITH latest_deals AS (
    SELECT * FROM 
     (SELECT
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
      ))
      WHERE  rn = 1
),
last_cm_contracts AS (
  SELECT
    custom_field.cf__opp_id,
    custom_field.cf_sign_date,
    custom_field.cf_type,
    custom_field.cf_contract_id AS contract_number,
    custom_field.cf_expire_date
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
),
last_sales_accounts AS (
  SELECT
    id,
    custom_field.cf_tax_code AS cf_tax_code, name
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY id
        ORDER BY updated_at DESC, created_at DESC
      ) AS rn
    FROM crm_raw.sales_accounts
  ) t
  WHERE rn = 1
)

SELECT
    s.id as deal_id,
    CAST(s.expected_close as DATE) as expected_close_date,
    CAST(s.closed_date as DATE) as closed_date,
    CAST(TO_DATE(i.cf_sign_date) as TIMESTAMP) AS sign_date,
    CASE 
        WHEN trim(b.name) = 'Signed / Ký hợp đồng' THEN  CAST(TO_DATE(i.cf_sign_date) AS TIMESTAMP)
        ELSE  CAST(s.expected_close as DATE) 
    END AS buy_date,
    CASE 
        WHEN trim(b.name) = 'Signed / Ký hợp đồng' THEN  CAST(i.cf_expire_date as TIMESTAMP)
        WHEN s.custom_field.cf__expire_date IS NULL   THEN CAST(s.expected_close AS TIMESTAMP)
        WHEN s.expected_close IS NULL   THEN CAST(s.custom_field.cf__expire_date AS TIMESTAMP)
        ELSE GREATEST( CAST(s.custom_field.cf__expire_date as TIMESTAMP)  , CAST(s.expected_close as TIMESTAMP) )
    END AS expire_date,
    s.probability,
    s.custom_field.cf_channel as channel,
    s.custom_field.cf_presales as presales_name,
    s.custom_field.cf_project_manager as project_manager,
    s.custom_field.cf_partner as partner,
    s.custom_field.cf__territory as territory_name,
    trim(b.name)             AS deal_stage_name,
    
    CAST(s.sales_account_id AS STRING) AS company_id,
    s.custom_field.cf_alias AS company_alias,
    COALESCE(s.custom_field.cf__company, 'N/A')  AS company_name,
    sa.cf_tax_code AS company_tax_code,
    s.name AS deal_name,
    i.contract_number AS contract_number,

    s.custom_field.cf__allocated_products AS cf__allocated_products
FROM latest_deals s
LEFT JOIN last_deal_stages b    ON s.deal_stage_id = b.id
LEFT JOIN last_cm_contracts i ON CAST(s.id AS STRING) = CAST(i.cf__opp_id AS STRING)
LEFT JOIN last_sales_accounts sa ON CAST(s.sales_account_id AS STRING) = CAST(sa.id AS STRING)

""")

df_last_deals  = df_last_deals .withColumn(
    "is_soc_qualified",
    get_soc_group_udf(
        F.col("cf__allocated_products")
    )
)


# ------------------------------------------------------------------
# 2. SCHEMA CHO allocated_products (ARRAY JSON)
# ------------------------------------------------------------------

allocated_product_schema = StructType([
    StructField("id", LongType(), True),
    StructField("pid", LongType(), True),
    StructField("name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("version", StringType(), True),

    StructField("allocationValue", DoubleType(), True),
    StructField("allocationDuration", IntegerType(), True),

    StructField("forecastDate", StringType(), True),
    StructField("actualDate", StringType(), True),

    StructField("coefficient", StringType(), True),
    StructField("type", StringType(), True),

    StructField("productType", StringType(), True),
    StructField("spdvType", StringType(), True),

    StructField("region", StringType(), True),
    StructField("currency", StringType(), True)
])

allocated_products_schema = ArrayType(allocated_product_schema)

# ------------------------------------------------------------------
# 3. PARSE JSON + EXPLODE
# ------------------------------------------------------------------

df_allocated_products = (
    df_last_deals
        .withColumn(
            "allocated_products_array",
            from_json(
                coalesce(col("cf__allocated_products"), lit("[]")),
                allocated_products_schema
            )
        )
        .withColumn("p", explode(col("allocated_products_array")))
        .select(
            col("deal_id").cast("string").alias("deal_id"),
            col("expected_close_date"),
            col("closed_date"),
            col("sign_date"),
            col("buy_date"),
            col("expire_date"),
            col("probability"),
            col("channel"),
            col("presales_name"),
            col("project_manager"),
            col("partner"),
            col("territory_name"),
            col("company_id"),
            col("company_alias"),
            col("company_name"),
            col("company_tax_code"),
            col("contract_number"),
            col("deal_name"),
            col("deal_stage_name"),
            col("is_soc_qualified"),
            col("p.id").alias("item_index"),
            col("p.pid").alias("product_id"),
            col("p.name").alias("product_name"),
            col("p.category").alias("product_category"),
            col("p.version").alias("product_version"),

            col("p.allocationValue").cast("decimal(18,2)").alias("allocation_value"),
            col("p.allocationDuration").alias("allocation_duration"),

            col("p.forecastDate").alias("forecast_date"),
            col("p.actualDate").alias("actual_date"),

            regexp_replace(col("p.coefficient"), "%", "").cast("double").alias("coefficient_percent"),
            col("p.type").alias("allocation_type"),

            col("p.productType").alias("product_type"),
            col("p.spdvType").alias("spdv_type"),

            col("p.region").alias("region"),
            col("p.currency").alias("currency")
        )
)

# ------------------------------------------------------------------
# 4. GHI RA SILVER TABLE
# ------------------------------------------------------------------

df_allocated_products = df_allocated_products.withColumn(
    "product_category",
    normalize_udf(F.col("product_category"))
)

df_allocated_products.createOrReplaceTempView("allocated_products")

df2 = spark.sql("""
   select t1.* ,
    t2.product_service_group_vcs,
    t2.product_service_group,
    t2.product_category_code
    
   from allocated_products t1
   LEFT JOIN bi_silver.crm_product_service_group_mapping t2 on lower(t2.product_category) = lower(t1.product_category)
""")

df2.createOrReplaceTempView("group_spdv_allocated_products")

df = spark.sql("""
WITH base AS (

    -- Chuẩn hóa transaction: dùng contract_number, nếu chưa có thì dùng deal_id
    SELECT
        deal_id,
        expected_close_date,
        closed_date,
        sign_date,
        buy_date,
        expire_date,
        probability,
        channel,
        presales_name,
        project_manager,
        partner,
        territory_name,
        company_id,
        company_alias,
        company_name,
        company_tax_code,
        contract_number,
        deal_name,
        deal_stage_name,
        is_soc_qualified,
        item_index,
        product_id,
        product_name,
        product_category,
        product_version,
        allocation_value,
        allocation_duration,
        forecast_date,
        actual_date,
        coefficient_percent,
        allocation_type,
        spdv_type,
        region,
        currency,
        product_service_group_vcs,
        product_service_group,
        product_category_code,

        COALESCE(
            NULLIF(TRIM(contract_number), ''),
            CONCAT(
                'DEAL_',
                CAST(deal_id AS STRING)
            )
        ) AS transaction_id

    FROM group_spdv_allocated_products

),


transaction_product AS (

    -- Loại duplicate product trong cùng transaction
    SELECT
        company_id,
        transaction_id,
        product_category,
        MIN(buy_date) AS buy_date,
        MAX(expire_date) AS expire_date

    FROM base

    GROUP BY
        company_id,
        transaction_id,
        product_category
),


transactions AS (

    -- Gom danh sách sản phẩm theo transaction
    SELECT
        company_id,
        transaction_id,
        MIN(buy_date) AS transaction_buy_date,
        MAX(expire_date) AS transaction_expire_date,
        collect_set(product_category) AS transaction_products

    FROM transaction_product

    GROUP BY
        company_id,
        transaction_id
),


ordered_transactions AS (

    -- Lấy transaction gần nhất trước đó của công ty
    SELECT
        company_id,
        transaction_id,
        transaction_buy_date,
        transaction_expire_date,
        transaction_products,

        LAG(transaction_id) OVER (
            PARTITION BY company_id
            ORDER BY transaction_buy_date, transaction_id
        ) AS previous_transaction_id,

        LAG(transaction_buy_date) OVER (
            PARTITION BY company_id
            ORDER BY transaction_buy_date, transaction_id
        ) AS previous_transaction_buy_date,

        LAG(transaction_expire_date) OVER (
            PARTITION BY company_id
            ORDER BY transaction_buy_date, transaction_id
        ) AS previous_transaction_expire_date,

        LAG(transaction_products) OVER (
            PARTITION BY company_id
            ORDER BY transaction_buy_date, transaction_id
        ) AS previous_transaction_products

    FROM transactions
),


product_history AS (

    -- Lấy lịch sử mua của cùng company + product
    SELECT
        company_id,
        transaction_id,
        product_category,
        buy_date,
        expire_date,

        LAG(buy_date) OVER (
            PARTITION BY company_id, product_category
            ORDER BY buy_date, transaction_id
        ) AS previous_product_buy_date,

        LAG(expire_date) OVER (
            PARTITION BY company_id, product_category
            ORDER BY buy_date, transaction_id
        ) AS previous_product_expire_date,

        LAG(transaction_id) OVER (
            PARTITION BY company_id, product_category
            ORDER BY buy_date, transaction_id
        ) AS previous_product_transaction_id

    FROM transaction_product
),


product_context AS (

    -- Xác định lịch sử transaction và product của từng record
    SELECT
        b.*,

        ot.previous_transaction_id,
        ot.previous_transaction_buy_date,
        ot.previous_transaction_expire_date,
        ot.previous_transaction_products,

        ph.previous_product_buy_date,
        ph.previous_product_expire_date,
        ph.previous_product_transaction_id,

        -- Product có nằm trong transaction trước hay không
        CASE
            WHEN ot.previous_transaction_products IS NOT NULL
             AND array_contains(
                    ot.previous_transaction_products,
                    b.product_category
                 )
            THEN 1
            ELSE 0
        END AS is_in_previous_transaction

    FROM base b

    LEFT JOIN ordered_transactions ot
        ON  b.company_id = ot.company_id
        AND b.transaction_id = ot.transaction_id

    LEFT JOIN product_history ph
        ON  b.company_id = ph.company_id
        AND b.transaction_id = ph.transaction_id
        AND b.product_category = ph.product_category
),


company_expire AS (

    -- Lấy ngày hết hạn gần nhất của các sản phẩm trước đó trong công ty
    SELECT
        pc.company_id,
        pc.transaction_id,
        pc.product_category,

        MAX(tp.expire_date) AS nearest_company_expire_date

    FROM product_context pc

    LEFT JOIN transaction_product tp
        ON  pc.company_id = tp.company_id
        AND tp.buy_date < pc.buy_date
        AND tp.expire_date < pc.buy_date

    GROUP BY
        pc.company_id,
        pc.transaction_id,
        pc.product_category
),


date_context AS (

    -- Tính khoảng cách từ buy_date đến expire_date gần nhất
    SELECT
        pc.*,

        ce.nearest_company_expire_date,

        -- Khoảng cách với expire của chính product
        CASE
            WHEN pc.previous_product_expire_date IS NOT NULL
            THEN DATEDIFF(
                TO_DATE(pc.buy_date),
                TO_DATE(pc.previous_product_expire_date)
            )
        END AS days_from_product_expire,

        -- Khoảng cách với expire gần nhất của công ty
        CASE
            WHEN ce.nearest_company_expire_date IS NOT NULL
            THEN DATEDIFF(
                TO_DATE(pc.buy_date),
                TO_DATE(ce.nearest_company_expire_date)
            )
        END AS days_from_company_expire

    FROM product_context pc

    LEFT JOIN company_expire ce
        ON  pc.company_id = ce.company_id
        AND pc.transaction_id = ce.transaction_id
        AND pc.product_category = ce.product_category
),


classified AS (

    SELECT
        dc.*,

        CASE

            -- Có ở transaction trước và mua lại trong vòng 1 năm: Renew
            WHEN is_in_previous_transaction = 1
             AND days_from_product_expire BETWEEN 0 AND 365
            THEN 'Renew'


            -- Không có ở transaction trước nhưng đang trong chu kỳ 1 năm: Upsales
            WHEN is_in_previous_transaction = 0
             AND previous_transaction_id IS NOT NULL
             AND days_from_company_expire BETWEEN 0 AND 365
            THEN 'Upsales'


            -- Đã từng mua product nhưng cách lần hết hạn trước hơn 1 năm: New
            WHEN previous_product_transaction_id IS NOT NULL
             AND days_from_product_expire > 365
            THEN 'New'


            -- Chưa từng mua product nhưng công ty đang trong chu kỳ 1 năm: Upsales
            WHEN previous_product_transaction_id IS NULL
             AND previous_transaction_id IS NOT NULL
             AND days_from_company_expire BETWEEN 0 AND 365
            THEN 'Upsales'


            -- Công ty chưa có transaction trước: New
            WHEN previous_transaction_id IS NULL
            THEN 'New'


            -- Đã quá 1 năm kể từ lần hết hạn gần nhất: New
            WHEN days_from_company_expire > 365
            THEN 'New'


            -- Không xác định được lịch sử: New
            ELSE 'New'

        END AS calculated_product_type

    FROM date_context dc
)


-- Giữ nguyên toàn bộ dữ liệu gốc và overwrite product_type
SELECT
    deal_id,
    expected_close_date,
    closed_date,
    sign_date,
    buy_date,
    expire_date,
    probability,
    channel,
    presales_name,
    project_manager,
    partner,
    territory_name,
    company_id,
    company_alias,
    company_name,
    company_tax_code,
    contract_number,
    deal_name,
    deal_stage_name,
    is_soc_qualified,
    item_index,
    product_id,
    product_name,
    product_category,
    product_version,
    allocation_value,
    allocation_duration,
    forecast_date,
    actual_date,
    coefficient_percent,
    allocation_type,

    -- Overwrite product_type bằng kết quả phân loại mới
    calculated_product_type AS product_type,

    spdv_type,
    region,
    currency,
    product_service_group_vcs,
    product_service_group,
    product_category_code

FROM classified
""")

df \
    .repartition(1) \
    .write \
    .mode("overwrite") \
    .format("parquet") \
    .save(target_path)


    # .option("path",target_path ) \
    # .saveAsTable(target_table)
    # .save(target_path)

print("DONE: crm_silver.deal_allocated_products")
