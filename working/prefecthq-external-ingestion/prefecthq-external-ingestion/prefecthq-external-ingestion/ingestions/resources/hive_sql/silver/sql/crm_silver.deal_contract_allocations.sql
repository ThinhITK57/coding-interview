SELECT dar.vcs_value , dd.deal_stage_name , dd.currency_code ,
coalesce (dar.forecast_start_date , cts.fac_date) as first_payment_date,
dar.allocation_count , dar.vcs_value / dd.exchange_rate as base_revenue_amount
FROM hive.crm_silver.deal_allocated_records  dar 
left join hive.crm_silver.deals dd on dd.deal_id = dar.deal_id 
left join hive.crm_silver.contracts cts on cts.deal_id = dar.deal_id 
where vcs_value is not null and dd.deal_stage_name ='Signed';
