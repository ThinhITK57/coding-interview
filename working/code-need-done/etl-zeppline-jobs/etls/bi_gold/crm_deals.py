%livy.pyspark

sql_query = """
SELECT
    deal_id AS id,   -- stable primary key

    deal_id,
    deal_name,
    currency_amount,
    vnd_amount,

    CAST(expected_close_date AS TIMESTAMP) AS expected_close_date,
    CAST(closed_date AS TIMESTAMP) AS closed_date,
    CAST(stage_updated_time AS TIMESTAMP) AS stage_updated_time,

    days_to_close,
    days_since_last_update,
    lock_revenue,
    usd_to_vnd,

    CAST(get_live_date AS TIMESTAMP) AS get_live_date,
    periodicity,
    is_select_viettel,
    has_budget,

    company_id,
    budget_amount,
    channel,
    is_partner,
    is_direct,
    weight,
    bidding_required,

    presales_name,
    project_manager,
    sale_admin,

    partner_id,
    contract_id,

    initial_source,
    warm_up_source,
    territory_name,

    CAST(expire_date AS TIMESTAMP) AS expire_date,
    currency_code,
    quotation_status,

    CAST(fac_date AS TIMESTAMP) AS fac_date,
    am_username,
    check_change,

    CAST(updated_at AS TIMESTAMP) AS updated_at,
    CAST(created_at AS TIMESTAMP) AS created_at,

    recent_note,
    CAST(next_scheduled_activity_time AS TIMESTAMP) AS next_scheduled_activity_time,
    CAST(last_assigned_at AS TIMESTAMP) AS last_assigned_at,

    last_contacted_activity_status,
    CAST(last_contacted_activity_time AS TIMESTAMP) AS last_contacted_activity_time,

    expected_deal_value,
    signing_delay_days,

    am_user_id,
    sales_account_id,
    deal_type_id,
    deal_reason_id,
    currency_id,

    vnd_to_usd,
    deal_payment_status,

    deal_stage_name,
    deal_stage_code,

    CAST(sign_date AS TIMESTAMP) AS sign_date,
    CAST(due_date AS TIMESTAMP) AS due_date,

    contract_duration_month,
    is_created_contract,
    is_signed,
    probability,
    probability_status

FROM bi_silver.crm_deals
"""

df = spark.sql(sql_query)

# spark.sql("DROP TABLE IF EXISTS bi_gold.crm_deals")

tgt_path = "/opt/datasets/crawlers/vcs_gold/bi_gold/data/crm_deals"

# # Hard delete folder
# hconf = spark._jsc.hadoopConfiguration()
# fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hconf)
# path = spark._jvm.org.apache.hadoop.fs.Path(tgt_path)

# if fs.exists(path):
#     fs.delete(path, True)

df.repartition(1).write \
    .mode("overwrite") \
    .format("parquet") \
    .option("path", tgt_path) \
    .saveAsTable("bi_gold.crm_deals")
