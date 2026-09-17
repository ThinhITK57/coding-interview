
spark.sql("""


CREATE OR REPLACE VIEW hive.bi_silver.crm_deal_products AS
select 
deal_id,
expected_close_date,
probability,
channel,
presales_name,
project_manager,
partner,
territory_name,
deal_stage_name,
item_index,
product_id,
product_name,
category,
version,
spdv_type,
license,
quantitative,
min_value,
max_value,
product_unit,
product_package,
product_price_type,
duration,
allocation_duration,
allocation_value,
is_quantity_based,
max_discount,
vat,
discount,
discount_type,
base_price,
final_total,
currency

 from hive.crm_silver.deal_products

""")