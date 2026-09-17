%livy.pyspark

# OLD: crm_payment_records
# NEW: crm_payment_forecast

# spark.sql("DROP TABLE IF EXISTS bi_silver.crm_payment_records")

sql_query = """
SELECT 
    md5(concat_ws('|',
        deal_id,
        cast(payment_date as string),
        cast(payment_index as string),
        product_id
    )) AS id,

    deal_id,
    CAST(payment_date AS TIMESTAMP) AS payment_date,
    YEAR(payment_date) as payment_year,
    MONTH(payment_date) as payment_month,

    currency_amount,
    vnd_amount,
    is_actual,

    CAST(due_date AS TIMESTAMP) AS due_date,

    product_category,
    product_type,
    product_id as pricebook_id,
    deployment_type,
    territory_name,
    is_recurring,
    is_partner,
    is_direct,
    customer_group,
    is_vip_customer,
    is_enterprise_customer,
    is_state_owned,
    is_private_enterprise,
    is_banking_group,
    is_international_client,
    is_internal_client,
    product_category_index,
    allocation_duration,
    currency_code,
    am_username,
    deal_stage_code,
    deal_stage_name,
    contract_id,
    payment_index,
    segment_l1,
    segment_l2,
    segment_l3

FROM bi_silver.crm_payment_forecast
"""

df = spark.sql(sql_query)

#spark.sql("DROP TABLE IF EXISTS bi_gold.crm_payment_forecast")

tgt_path = "/opt/datasets/crawlers/vcs_gold/bi_gold/data/crm_payment_forecast"

# hconf = spark._jsc.hadoopConfiguration()
# fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hconf)
# path = spark._jvm.org.apache.hadoop.fs.Path(tgt_path)

# if fs.exists(path):
#     fs.delete(path, True)

df.repartition(1).write \
    .mode("overwrite") \
    .format("parquet") \
    .option("path", tgt_path) \
    .saveAsTable("bi_gold.crm_payment_forecast")
