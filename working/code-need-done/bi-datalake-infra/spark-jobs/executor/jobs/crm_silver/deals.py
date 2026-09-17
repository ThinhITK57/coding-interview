# %livy.pyspark

sql_query = """
WITH last_deals AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY id
                   ORDER BY updated_at_ts DESC, id DESC
               ) rn
        FROM crm_raw.deals ss
        where is_deleted = false 
        	AND NOT EXISTS (
		    SELECT 1
		    FROM crm_raw.deleted_deals d
		    WHERE cast(d.id as bigint) = ss.id
		)
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
last_cm_contracts AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY id
                   ORDER BY updated_at_ts DESC, id DESC
               ) rn
        FROM crm_raw.cm_contracts
    ) t
    WHERE rn = 1
),
last_currencies as (
	SELECT id,currency_code, exchange_rate FROM crm_raw.currencies where is_active  = true
)
select
    CAST(s.id AS STRING) AS deal_id,
    s.name AS deal_name,
    s.amount  as currency_amount,
    s.base_currency_amount as vnd_anmount,
	-- ROUND(CAST(s.base_currency_amount AS DOUBLE) / NULLIF(CAST(s.amount AS DOUBLE), 0), 2) AS change_rate,
    s.expected_close as expected_close_date,
    s.closed_date,
    s.probability,
    s.forecast_category,
    s.deal_prediction,
    s.last_deal_prediction,
    s.custom_field.cf_interested_products as deal_interested_products,
    s.custom_field.cf_alias as company_alias,
    s.custom_field.cf_budget as budget_amount,
	CASE
		WHEN trim(s.custom_field.cf_group) = 'Khách hàng VIP, VVIP' THEN 'VIP'
		WHEN trim(s.custom_field.cf_group) = 'Khách hàng lớn' THEN 'ENTERPRISE'
		ELSE 'NORMAL'
	END AS customer_group,
	CASE WHEN trim(s.custom_field.cf_group) = 'Khách hàng VIP, VVIP' THEN 1 ELSE 0 END AS is_vip_customer,
	CASE WHEN trim(s.custom_field.cf_group) = 'Khách hàng lớn' THEN 1 ELSE 0 END AS is_enterprise_customer,
    s.custom_field.cf_segment as segment_l1,
    s.custom_field.cf_segment2 as segment_l2,
    s.custom_field.cf_segment3 as segment_l3,
    s.custom_field.cf_channel as channel,
    s.custom_field.cf_presales as presales_name,
    s.custom_field.cf_project_manager as project_manager,
    s.custom_field.cf_partner as partner,
    s.custom_field.cf_contract as contract_id2,
    s.custom_field.cf__territory as territory_name,
	s.custom_field.cf__duration AS contract_duration_month,
	s.custom_field.cf__company as company_name,
	s.custom_field.cf__address as address,
	s.custom_field.cf__province as province,
	s.custom_field.cf__country as country,
	s.custom_field.cf__email as email,
	s.custom_field.cf__am AS acount_manager,
	s.custom_field.cf__fac_date AS fact_date,
	s.sales_account_id AS sale_account_id,
	CAST(s.expected_deal_value AS BIGINT) as expected_deal_value,
	to_timestamp(s.upcoming_activities_time) as next_scheduled_activity_time,
	to_timestamp(s.last_assigned_at) AS last_assigned_at,
	UPPER(COALESCE(s.last_contacted_sales_activity_mode, 'unknown')) as last_contacted_activity_status,
	to_timestamp(s.last_contacted_via_sales_activity) as last_contacted_activity_time,
--	s.custom_field.cf__currency as currency_code,
	
    c.custom_field.cf_contract_id AS contract_id,
    s.deal_stage_id,
	trim(split(b.name, '/')[0])             AS deal_stage_name,
	trim(split(b.name, '/')[1])             AS deal_stage_name_vi,
    b.forecast_type AS deal_stage_forecast_type,
    lc.currency_code,
	CAST(lc.exchange_rate AS DOUBLE) as vnd_to_usd,
    s.deal_type_id,
    cdt.name  as deal_type_name,
    s.deal_pipeline_id,
    cdpl.name as deal_pipeline_name,
    s.deal_reason_id,
    cdre.name as deal_reason_name,
	CASE
	  WHEN s.custom_field.cf_segment3 IN (
		'GOV',
		'Province',
		'Energy',
		'doanh nghiệp nhà nước',
		'tập đoàn nhà nước',
		'cơ quan nhà nước',
		'Doanh nghiệp 100% vốn Nhà nước'
	 )
	 THEN 1
	  ELSE 0
	END AS is_state_owned,
	CASE
	  WHEN trim(b.name) IN (
		'signed / ký hợp đồng'
	 )
	 THEN 1
	  ELSE 0
	END AS is_signed,
	CASE
	  WHEN s.probability = 100 THEN 'SUCCESS'
	  WHEN s.probability > 10 AND s.probability < 80 THEN 'POTENTIAL'
	  WHEN s.probability < 10 THEN 'FAILED'
	  ELSE 'UNCERTAIN'
	END AS probability_status,
	CASE
		WHEN s.custom_field.cf_segment = 'Khách hàng ngoài'
		 AND (s.custom_field.cf_segment3 IS NULL
			  OR s.custom_field.cf_segment3 NOT IN ('GOV', 'Province', 'Energy'))
		THEN 1
		ELSE 0
	END AS is_private_enterprise,
	CASE
		WHEN upper(s.custom_field.cf_segment3) = 'BFSI'
		THEN 1
		ELSE 0
	END AS is_banking_group,
	CASE
		WHEN s.custom_field.cf_segment = 'International'
		THEN 1
		ELSE 0
	END AS is_international_revenue,
	CASE
		WHEN s.custom_field.cf_segment = 'Nội bộ'
		THEN 1
		ELSE 0
	END AS is_internal_revenue,
	CASE
		WHEN s.custom_field.cf_channel = 'Partner'
		THEN 1
		ELSE 0
	END AS is_partner
	
FROM last_deals s
JOIN last_deal_stages b    ON s.deal_stage_id = b.id
LEFT JOIN last_cm_contracts c  ON s.id = c.custom_field.cf_opportunity
LEFT JOIN last_currencies lc  ON s.currency_id = lc.id
LEFT JOIN crm_raw.deal_types cdt  ON s.deal_type_id = cdt.id
LEFT JOIN crm_raw.deal_pipelines cdpl  ON s.deal_pipeline_id = cdpl.id
LEFT JOIN crm_raw.deal_reasons cdre  ON s.deal_reason_id = cdre.id

"""

df = spark.sql(sql_query)

df.write \
    .mode("overwrite") \
    .format("parquet") \
    .option(
        "path",
        "s3a://vcs-silver/crm-silver/deals"
    ) \
    .saveAsTable("crm_silver.deals")
