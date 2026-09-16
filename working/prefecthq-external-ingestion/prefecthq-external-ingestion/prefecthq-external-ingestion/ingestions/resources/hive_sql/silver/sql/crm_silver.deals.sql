WITH last_deals AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY id
                   ORDER BY updated_at_ts DESC, id DESC
               ) rn
        FROM hive.crm_raw.deals ss
        where is_deleted = false 
        	AND NOT EXISTS (
		    SELECT 1
		    FROM hive.crm_raw.deleted_deals d
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
        FROM hive.crm_raw.deal_stages
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
        FROM hive.crm_raw.cm_contracts
    ) t
    WHERE rn = 1
),
last_currencies as (
	SELECT id,currency_code, exchange_rate FROM hive.crm_raw.currencies where is_active  = true
)
select
    CAST(s.id AS VARCHAR) AS deal_id,
    s.name AS deal_name,
    s.amount,
    s.base_currency_amount,
	ROUND(CAST(s.base_currency_amount AS DOUBLE) / NULLIF(CAST(s.amount AS DOUBLE), 0), 2) AS change_rate,
    s.expected_close as expected_close_date,
    s.closed_date,
    s.probability,
    s.expected_deal_value,
    s.forecast_category,
    s.deal_prediction,
    s.last_deal_prediction,
    s.custom_field.cf_interested_products as deal_interested_products,
    s.custom_field.cf_alias as deal_alias,
    s.custom_field.cf_budget as deal_budget,
    s.custom_field.cf_segment as segment_l1,
    s.custom_field.cf_segment2 as segment_l2,
    s.custom_field.cf_segment3 as segment_l3,
    s.custom_field.cf_channel as channel,
    s.custom_field.cf_presales as presale_names,
    s.custom_field.cf_project_manager as project_manager,
    s.custom_field.cf_partner as partner,
    s.custom_field.cf_contract as contract_id2,
    s.custom_field.cf__territory as territory_name,
	s.custom_field.cf__duration as deal_duration,
	s.custom_field.cf__company as company_name,
	s.custom_field.cf__address as address,
	s.custom_field.cf__province as province,
	s.custom_field.cf__country as country,
	s.custom_field.cf__email as email,
	s.custom_field.cf__am AS acount_manager,
	s.custom_field.cf__fac_date AS fact_date,
	s.sales_account_id AS sale_account_id,
--	s.custom_field.cf__currency as currency_code,
	
    c.custom_field.cf_contract_id AS contract_id,
    s.deal_stage_id,
	trim(split(b.name, '/')[0])             AS deal_stage_name,
	trim(split(b.name, '/')[1])             AS deal_stage_name_vi,
    b.forecast_type AS deal_stage_forecast_type,
    lc.currency_code,
	CAST(lc.exchange_rate AS DOUBLE) as exchange_rate,
    s.deal_type_id,
    cdt.name  as deal_type_name,
    s.deal_pipeline_id,
    cdpl.name as deal_pipeline_name,
    s.deal_reason_id,
    cdre.name as deal_reason_name 
FROM last_deals s
JOIN last_deal_stages b    ON s.deal_stage_id = b.id
LEFT JOIN last_cm_contracts c  ON s.id = c.custom_field.cf_opportunity
LEFT JOIN last_currencies lc  ON s.currency_id = lc.id
LEFT JOIN hive.crm_raw.deal_types cdt  ON s.deal_type_id = cdt.id
LEFT JOIN hive.crm_raw.deal_pipelines cdpl  ON s.deal_pipeline_id = cdpl.id
LEFT JOIN hive.crm_raw.deal_reasons cdre  ON s.deal_reason_id = cdre.id
where s.id=50001823763
limit 10;
