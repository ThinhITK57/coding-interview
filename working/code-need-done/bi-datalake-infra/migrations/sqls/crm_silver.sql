CREATE SCHEMA hive.crm_silver
with (location='s3a://vcs-silver/crm-silver/');


-- hive.crm_silver.catalogs definition

CREATE TABLE hive.crm_silver.catalogs (
   id bigint,
   name varchar,
   category varchar,
   category_version varchar,
   max_discount bigint,
   item_type varchar,
   is_active boolean
)
WITH (
   format = 'PARQUET'
);


-- hive.crm_silver.contract_deployed_products definition

CREATE TABLE hive.crm_silver.contract_deployed_products (
   contract_id bigint,
   deployed_product_name varchar
)
WITH (
   format = 'PARQUET'
);


-- hive.crm_silver.contract_products definition

CREATE TABLE hive.crm_silver.contract_products (
   contract_id bigint,
   signed_product_name varchar
)
WITH (
   format = 'PARQUET'
);


-- hive.crm_silver.contract_signed_products definition

CREATE TABLE hive.crm_silver.contract_signed_products (
   contract_id bigint,
   signed_product_name varchar
)
WITH (
   format = 'PARQUET'
);


-- hive.crm_silver.contracts definition

CREATE TABLE hive.crm_silver.contracts (
   id bigint,
   name varchar,
   user_am_id bigint,
   contract_number varchar,
   contract_type varchar,
   signing_method varchar,
   sale_account_id bigint,
   company_alias varchar,
   tax_code varchar,
   customer_group varchar,
   segment_l1 varchar,
   segment_l2 varchar,
   segment_l3 varchar,
   status varchar,
   sign_date varchar,
   fac_date varchar,
   duration double,
   expire_date varchar,
   usd_exchange_rate double,
   currency_code varchar,
   revenue double,
   vat bigint,
   vcs_revenue bigint,
   viettel_revenue bigint,
   partner_id varchar,
   deal_id varchar
)
WITH (
   format = 'PARQUET'
);


-- hive.crm_silver.deal_allocated_products definition

CREATE TABLE hive.crm_silver.deal_allocated_products (
   deal_id varchar,
   item_index bigint,
   product_id bigint,
   product_name varchar,
   product_category varchar,
   product_version varchar,
   allocation_value double,
   allocation_duration integer,
   forecast_date varchar,
   actual_date varchar,
   coefficient varchar,
   allocation_type varchar,
   product_type varchar,
   spdv_type varchar,
   region varchar,
   currency varchar
)
WITH (
   format = 'PARQUET'
);


-- hive.crm_silver.deal_allocated_records definition

CREATE TABLE hive.crm_silver.deal_allocated_records (
   deal_id varchar,
   record_index bigint,
   product_id bigint,
   product_name varchar,
   product_category varchar,
   product_version varchar,
   territory varchar,
   is_recurring boolean,
   total_vcs_value double,
   total_value double,
   allocation_count integer,
   currency varchar,
   period varchar,
   forecast_start_date varchar,
   actual_start_date varchar,
   vcs_value double,
   forecast_value double,
   actual_value double,
   allocation_overrides array(varchar)
)
WITH (
   format = 'PARQUET'
);


-- hive.crm_silver.deal_contract_allocations definition

CREATE TABLE hive.crm_silver.deal_contract_allocations (
   vcs_value double,
   deal_stage_name varchar,
   currency_code varchar,
   first_payment_date varchar,
   allocation_count integer,
   base_revenue_amount double
)
WITH (
   format = 'PARQUET'
);


-- hive.crm_silver.deal_global_discounts definition

CREATE TABLE hive.crm_silver.deal_global_discounts (
   deal_id varchar,
   global_discount_value double,
   global_discount_type varchar
)
WITH (
   format = 'PARQUET'
);


-- hive.crm_silver.deal_interested_products definition

CREATE TABLE hive.crm_silver.deal_interested_products (
   deal_id varchar,
   interested_product varchar
)
WITH (
   format = 'PARQUET'
);


-- hive.crm_silver.deal_products definition

CREATE TABLE hive.crm_silver.deal_products (
   deal_id varchar,
   item_index bigint,
   product_id bigint,
   product_name varchar,
   category varchar,
   version varchar,
   spdv_type varchar,
   license varchar,
   quantitative integer,
   min_value integer,
   max_value integer,
   product_unit varchar,
   product_package varchar,
   product_price_type varchar,
   duration integer,
   allocation_duration integer,
   allocation_value double,
   is_quantity_based boolean,
   max_discount double,
   vat double,
   discount double,
   discount_type varchar,
   base_price double,
   final_total double,
   currency varchar
)
WITH (
   format = 'PARQUET'
);


-- hive.crm_silver.deal_quotation_products definition

CREATE TABLE hive.crm_silver.deal_quotation_products (
   deal_id bigint,
   quotation_id varchar,
   product_name varchar,
   base_price double,
   quantitative integer,
   unit varchar,
   duration integer,
   package varchar,
   vat integer,
   discount double,
   discount_type varchar,
   final_total_amount double,
   base_total_amount double
)
WITH (
   format = 'PARQUET'
);


-- hive.crm_silver.deal_quotations definition

CREATE TABLE hive.crm_silver.deal_quotations (
   deal_id bigint,
   quotation_id varchar,
   created_at varchar,
   status varchar,
   tlang varchar,
   currency varchar,
   global_discount double,
   global_discount_type varchar
)
WITH (
   format = 'PARQUET'
);


-- hive.crm_silver.deals definition

CREATE TABLE hive.crm_silver.deals (
   deal_id varchar,
   deal_name varchar,
   currency_amount varchar,
   vnd_anmount varchar,
   expected_close_date varchar,
   closed_date varchar,
   probability bigint,
   forecast_category bigint,
   deal_prediction bigint,
   last_deal_prediction bigint,
   deal_interested_products varchar,
   company_alias varchar,
   budget_amount bigint,
   customer_group varchar,
   is_vip_customer integer,
   is_enterprise_customer integer,
   segment_l1 varchar,
   segment_l2 varchar,
   segment_l3 varchar,
   channel varchar,
   presales_name varchar,
   project_manager varchar,
   partner bigint,
   contract_id2 bigint,
   territory_name varchar,
   contract_duration_month bigint,
   company_name varchar,
   address varchar,
   province varchar,
   country varchar,
   email varchar,
   acount_manager varchar,
   fact_date varchar,
   sale_account_id bigint,
   expected_deal_value bigint,
   next_scheduled_activity_time timestamp(3),
   last_assigned_at timestamp(3),
   last_contacted_activity_status varchar,
   last_contacted_activity_time timestamp(3),
   contract_id varchar,
   deal_stage_id bigint,
   deal_stage_name varchar,
   deal_stage_name_vi varchar,
   deal_stage_forecast_type varchar,
   currency_code varchar,
   vnd_to_usd double,
   deal_type_id bigint,
   deal_type_name varchar,
   deal_pipeline_id bigint,
   deal_pipeline_name varchar,
   deal_reason_id bigint,
   deal_reason_name varchar,
   is_state_owned integer,
   is_signed integer,
   probability_status varchar,
   is_private_enterprise integer,
   is_banking_group integer,
   is_international_revenue integer,
   is_internal_revenue integer,
   is_partner integer
)
WITH (
   format = 'PARQUET'
);


-- hive.crm_silver.dim_territory_name definition

CREATE TABLE hive.crm_silver.dim_territory_name (
   territory_name varchar
)
WITH (
   format = 'PARQUET'
);


-- hive.crm_silver.kpis definition

CREATE TABLE hive.crm_silver.kpis (
   id bigint,
   name varchar,
   owner_id bigint,
   kpi_scope varchar,
   currency_code varchar,
   relate_kpi_id bigint,
   sale_target varchar,
   revenue_target varchar,
   sale_target_q1 bigint,
   sale_target_m1 bigint,
   sale_target_m2 bigint,
   sale_target_m3 bigint,
   revenue_target_q1 bigint,
   revenue_target_m1 bigint,
   revenue_target_m2 bigint,
   revenue_target_m3 bigint,
   sale_target_q2 bigint,
   sale_target_m4 bigint,
   sale_target_m5 bigint,
   sale_target_m6 bigint,
   revenue_target_q2 bigint,
   revenue_target_m4 bigint,
   revenue_target_m5 bigint,
   revenue_target_m6 bigint,
   sale_target_q3 bigint,
   sale_target_m7 bigint,
   sale_target_m8 bigint,
   sale_target_m9 bigint,
   revenue_target_q3 bigint,
   revenue_target_m7 bigint,
   revenue_target_m8 bigint,
   revenue_target_m9 bigint,
   sale_target_q4 bigint,
   sale_target_m10 bigint,
   sale_target_m11 bigint,
   sale_target_m12 bigint,
   revenue_target_q4 bigint,
   revenue_target_m10 bigint,
   revenue_target_m11 bigint,
   revenue_target_m12 bigint,
   creator_id bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.crm_silver.partners definition

CREATE TABLE hive.crm_silver.partners (
   id bigint,
   name varchar,
   document_number varchar,
   company_alias varchar,
   address varchar,
   country varchar,
   contact_name varchar,
   contact_position varchar,
   contact_mobile varchar,
   contact_email varchar,
   effective_date varchar,
   expiration_date varchar,
   parter_type varchar,
   document_format varchar,
   status varchar,
   parter_type_vi varchar,
   document_format_vi varchar,
   status_vi varchar
)
WITH (
   format = 'PARQUET'
);


-- hive.crm_silver.price_books definition

CREATE TABLE hive.crm_silver.price_books (
   id bigint,
   name varchar,
   sku varchar,
   product_type varchar,
   sub_type varchar,
   license varchar,
   price_type varchar,
   package_type varchar,
   price double,
   currency_code varchar,
   price_min bigint,
   price_max bigint,
   product_unit varchar,
   csmp_discount_rate bigint,
   csmp_discount_silver_rate bigint,
   csmp_discount_gold_rate bigint,
   csmp_discount_diamond_rate bigint,
   catalog_id bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.crm_silver.sales_account_interested_products definition

CREATE TABLE hive.crm_silver.sales_account_interested_products (
   sale_id varchar,
   interested_product varchar
)
WITH (
   format = 'PARQUET'
);


-- hive.crm_silver.sales_account_product_history definition

CREATE TABLE hive.crm_silver.sales_account_product_history (
   sale_id varchar,
   product_name varchar
)
WITH (
   format = 'PARQUET'
);


-- hive.crm_silver.sales_account_team_users definition

CREATE TABLE hive.crm_silver.sales_account_team_users (
   sale_id varchar,
   user_id varchar
)
WITH (
   format = 'PARQUET'
);


-- hive.crm_silver.sales_accounts definition

CREATE TABLE hive.crm_silver.sales_accounts (
   sale_id varchar,
   is_partner boolean,
   name varchar,
   address varchar,
   city varchar,
   state varchar,
   zipcode varchar,
   country varchar,
   number_of_employees bigint,
   annual_revenue varchar,
   website varchar,
   user_am_id bigint,
   phone varchar,
   open_deals_amount varchar,
   open_deals_count bigint,
   won_deals_amount varchar,
   won_deals_count bigint,
   last_contacted varchar,
   last_contacted_mode varchar,
   facebook varchar,
   twitter varchar,
   company_alias varchar,
   tax_code varchar,
   is_using_soc varchar,
   current_soc_provider varchar,
   current_soc_brand varchar,
   first_date_using_soc varchar,
   country_custom varchar,
   province varchar,
   customer_group varchar,
   segment_l1 varchar,
   segment_l2 varchar,
   segment_l3 varchar,
   lead_initial_source varchar,
   lead_warm_up_source varchar,
   user_am_fullname varchar,
   company_domain varchar,
   parent_id bigint,
   last_assigned_at varchar,
   renewal_date varchar,
   business_type_id bigint,
   business_type_name varchar,
   industry_type_id bigint,
   industry_type_name varchar
)
WITH (
   format = 'PARQUET'
);


-- hive.crm_silver.users definition

CREATE TABLE hive.crm_silver.users (
   am_user_id varchar,
   display_name varchar,
   email varchar,
   is_active boolean,
   work_number varchar,
   mobile_number varchar,
   confirmed boolean,
   job_title varchar,
   language varchar,
   last_login_at varchar,
   time_zone varchar,
   access_scope varchar,
   line_manager_id varchar,
   role_id varchar,
   role_name varchar,
   user_access_type varchar
)
WITH (
   format = 'PARQUET'
);