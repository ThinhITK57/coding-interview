%livy.pyspark

# OLD: crm_deals_allocation
# NEW: crm_contract_allocations

# spark.sql("DROP TABLE IF EXISTS bi_silver.crm_deals_allocation")

sql_query = """
SELECT DISTINCT
id,
deal_id,
deal_name,
company_id,
contract_id,
currency_code,
deal_stage_name,
deal_stage_code,
is_partner,
is_direct,
am_username,
territory_name,
CAST(due_date AS TIMESTAMP) as due_date,
CAST(fac_date AS TIMESTAMP) as fac_date,
CAST(first_payment_date AS TIMESTAMP) as first_payment_date,
first_payment_year,
first_payment_month,
allocation_duration,
CAST(forecast_date as TIMESTAMP) as forecast_date,
created_at,
updated_at,
days_since_last_update,
pos,
product_category_index,
product_description,
product_category,
pricebook_id,
total_vcs_value,
vcs_value,
is_soc_ecosystem,
is_recurring
FROM bi_silver.crm_contract_allocations
"""

df = spark.sql(sql_query)

# spark.sql("DROP TABLE IF EXISTS bi_gold.crm_contract_allocations")

tgt_path = "/opt/datasets/crawlers/vcs_gold/bi_gold/data/crm_contract_allocations"

# Hard delete folder to avoid parquet doubles
# hconf = spark._jsc.hadoopConfiguration()
# fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hconf)
# path = spark._jvm.org.apache.hadoop.fs.Path(tgt_path)

# if fs.exists(path):
#     fs.delete(path, True)

df.repartition(1).write \
    .mode("overwrite") \
    .format("parquet") \
    .option("path", tgt_path) \
    .saveAsTable("bi_gold.crm_contract_allocations")
