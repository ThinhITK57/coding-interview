%livy.pyspark

sql_query = """
WITH base AS (
  SELECT
      *,
      row_number() OVER (
        PARTITION BY company_id
        ORDER BY updated_at DESC, created_at DESC
      ) AS rn
  FROM bi_silver.crm_deals
  WHERE company_id IS NOT NULL
),
latest AS (
  SELECT * FROM base WHERE rn = 1
),
agg_flags AS (
  SELECT
    company_id,
    max(CAST(is_signed AS INT)) AS has_signed,
    max(CAST(is_state_owned AS INT)) AS is_state_owned,
    max(CAST(is_private_enterprise AS INT)) AS is_private_enterprise,
    max(CAST(is_banking_group AS INT)) AS is_banking_group,
    max(CAST(is_international_client AS INT)) AS is_international_client,
    max(CAST(is_internal_client AS INT)) AS is_internal_client,
    max(CAST(is_vip_customer AS INT)) AS is_vip_customer,
    max(CAST(is_enterprise_customer AS INT)) AS is_enterprise_customer,
    max(created_at) AS last_created_at,
    max(updated_at) AS last_updated_at,
    min(created_at) AS first_created_at
  FROM bi_silver.crm_deals
  WHERE company_id IS NOT NULL
  GROUP BY company_id
)
SELECT
    l.company_id,

    -- latest snapshot fields (đúng theo bản ghi mới nhất)
    l.company_alias,
    l.customer_group,
    CAST(f.is_vip_customer AS INT) AS is_vip_customer,
    CAST(f.is_enterprise_customer AS INT) AS is_enterprise_customer,
    l.segment_l1,
    l.segment_l2,
    l.segment_l3,
    l.company_name,
    l.company_address,
    l.company_province,
    l.company_country,
    l.company_email,
    l.company_phone,
    l.company_contact,

    -- timestamps governance
    CAST(f.last_updated_at AS TIMESTAMP) AS updated_at,
    CAST(f.first_created_at AS TIMESTAMP) AS created_at,

    -- flags aggregated theo lịch sử
    CAST(f.is_state_owned AS INT) AS is_state_owned,
    CAST(f.has_signed AS INT) AS has_signed,
    CAST(f.is_private_enterprise AS INT) AS is_private_enterprise,
    CAST(f.is_banking_group AS INT) AS is_banking_group,
    CAST(f.is_international_client AS INT) AS is_international_client,
    CAST(f.is_internal_client AS INT) AS is_internal_client

FROM latest l
JOIN agg_flags f
  ON l.company_id = f.company_id
"""

df = spark.sql(sql_query)

# spark.sql("DROP TABLE IF EXISTS bi_gold.crm_companies")

tgt_path = "/opt/datasets/crawlers/vcs_gold/bi_gold/data/crm_companies"

# # Hard delete folder to avoid parquet doubles
# hconf = spark._jsc.hadoopConfiguration()
# fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hconf)
# path = spark._jvm.org.apache.hadoop.fs.Path(tgt_path)

# if fs.exists(path):
#     fs.delete(path, True)

df.repartition(1).write \
    .mode("overwrite") \
    .format("parquet") \
    .option("path", tgt_path) \
    .saveAsTable("bi_gold.crm_companies")
