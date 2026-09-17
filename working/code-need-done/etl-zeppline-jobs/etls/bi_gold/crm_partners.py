%livy.pyspark

sql_query = """
WITH base AS (
  SELECT
    *,
    CAST(effective_date AS TIMESTAMP)  AS effective_ts,
    CAST(expiration_date AS TIMESTAMP) AS expiration_ts
  FROM bi_silver.crm_partners
),
norm AS (
  SELECT
    id,
    name,
    document_number,
    company_alias,
    address,
    country,
    contact_name,
    contact_position,
    contact_mobile,
    contact_email,
    effective_ts  AS effective_date,
    expiration_ts AS expiration_date,
    parter_type,
    document_format,
    status,
    parter_type_vi,
    document_format_vi,
    status_vi,

    -- 1) lower + trim
    LOWER(TRIM(company_alias)) AS alias_lc,

    -- 2) bỏ ký tự không phải chữ/số -> space, rồi gom space
    TRIM(REGEXP_REPLACE(
      REGEXP_REPLACE(LOWER(TRIM(company_alias)), '[^0-9a-zA-Z\\u00C0-\\u1EF9]+', ' '),
      '\\\\s+', ' '
    )) AS alias_clean
  FROM base
),
final AS (
  SELECT
    *,
    -- 3) bỏ hậu tố pháp lý phổ biến (bạn có thể mở rộng list)
    TRIM(REGEXP_REPLACE(
      alias_clean,
      '(\\b(cty|cong ty|company|co\\.|ltd|llc|inc|corp|corporation|jsc|js|joint stock|tnhh|mtv|t\\s*n\\s*h\\s*h)\\b)',
      ''
    )) AS company_alias_norm
  FROM norm
)
SELECT
  id, name, document_number, company_alias,
  company_alias_norm,
  address, country, contact_name, contact_position, contact_mobile, contact_email,
  effective_date, expiration_date,
  parter_type, document_format, status,
  parter_type_vi, document_format_vi, status_vi
FROM final
"""

df = spark.sql(sql_query)

# spark.sql("DROP TABLE IF EXISTS bi_gold.crm_partners")

tgt_path = "/opt/datasets/crawlers/vcs_gold/bi_gold/data/crm_partners"
# hconf = spark._jsc.hadoopConfiguration()
# fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hconf)
# path = spark._jvm.org.apache.hadoop.fs.Path(tgt_path)
# if fs.exists(path):
#     fs.delete(path, True)

(df.repartition(1).write
  .mode("overwrite")
  .format("parquet")
  .option("path", tgt_path)
  .saveAsTable("bi_gold.crm_partners")
)

print("DONE")
