
spark.sql("""

CREATE OR REPLACE VIEW hive.bi_silver.crm_deal_allocated_products AS
select 
deal_id,
expected_close_date,
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
item_index,
product_id,
product_name,
product_category,
product_service_group_vcs,
product_service_group,
product_category_code,
product_version,
allocation_value,
allocation_duration,
forecast_date,
actual_date,
coefficient_percent,
allocation_type,
product_type,
spdv_type,
region,
currency,
is_soc_qualified

from hive.crm_silver.deal_allocated_products

""")