%livy.pyspark

sql_query = """
WITH latest_contract AS (
  SELECT *
  FROM (
    SELECT
      company_alias,
      contract_type,
      signing_method,
      row_number() OVER (
        PARTITION BY company_alias
        ORDER BY sign_date DESC
      ) AS rn
    FROM bi_silver.crm_contracts
  )
  WHERE rn = 1
)

SELECT
    md5(concat(r.company_alias, r.product_category, cast(r.report_date as string))) AS id,
    CAST(r.report_date AS TIMESTAMP) AS report_date,
    r.report_year,
    r.report_month,
    r.am_username,

    r.company_alias as customer_name,
    r.product_category,
    r.revenue_amount,
    r.currency_code,

    CAST(r.is_internal_client AS BOOLEAN) as is_internal,
    CAST(r.is_international_client AS BOOLEAN) as is_international,
    CAST(r.is_banking_group AS BOOLEAN) as is_banking,
    CAST(r.is_soc AS BOOLEAN) as is_soc,

    c.contract_type,
    c.signing_method

FROM bi_silver.biz_business_results r
LEFT JOIN latest_contract c
  ON r.company_alias = c.company_alias
"""

df = spark.sql(sql_query)

# # Drop + hard delete path
# spark.sql("DROP TABLE IF EXISTS bi_gold.biz_sales_fact")

tgt_path = "/opt/datasets/crawlers/vcs_gold/bi_gold/data/biz_sales_fact"
# hconf = spark._jsc.hadoopConfiguration()
# fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hconf)
# path = spark._jvm.org.apache.hadoop.fs.Path(tgt_path)
# if fs.exists(path):
#     fs.delete(path, True)

df.repartition(1).write \
    .mode("overwrite") \
    .format("parquet") \
    .option("path", tgt_path) \
    .saveAsTable("bi_gold.biz_sales_fact")
