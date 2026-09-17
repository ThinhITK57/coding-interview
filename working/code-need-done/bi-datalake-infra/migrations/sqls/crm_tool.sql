SHOW SCHEMAS FROM hive;
CREATE SCHEMA hive.crm_tool 
with (location='s3a://crm-data-tool/');


CREATE TABLE hive.crm_tool.customer_group_revenue_plan (
    plan_date VARCHAR,
    customer_segment_l2 VARCHAR,
    customer_segment_l1 VARCHAR,
    must_value DOUBLE,
    nice_value DOUBLE,
    group_value DOUBLE,
	adjusted_must_value DOUBLE,
    adjusted_nice_value DOUBLE,
    adjusted_group_value DOUBLE,
    file_name VARCHAR,
    created_by VARCHAR,
    created_at VARCHAR
)
WITH (
	external_location = 's3a://crm-data-tool/genbi/customer_group_revenue_plan/',
    format = 'PARQUET'
);


-- hive.crm_tool.customer_group_revenue_plan definition
CREATE SCHEMA IF NOT EXISTS crm_tool;

spark.sql("""
CREATE TABLE IF NOT EXISTS crm_tool.customer_group_revenue_plan (
    plan_date STRING,
    customer_segment_l2 STRING,
    customer_segment_l1 STRING,
    must_value DOUBLE,
    nice_value DOUBLE,
    group_value DOUBLE,
    adjusted_must_value DOUBLE,
    adjusted_nice_value DOUBLE,
    adjusted_group_value DOUBLE,
    file_name STRING,
    created_by STRING,
    created_at STRING
)
USING PARQUET
LOCATION '/opt/datasets/crawlers/vcs/crm-tool/data/customer_group_revenue_plan'
""")

-- hive.crm_tool.crm_allocated_revenue definition

CREATE TABLE hive.crm_tool.crm_allocated_revenue (
   deal_id varchar,
   version integer,
   seq integer,
   status varchar,
   deal_name varchar,
   allocated_revenue_amount double,
   revenue_type varchar,
   hold boolean,
   origin varchar,
   deal_stage_name varchar,
   deal_type varchar,
   contract_id varchar,
   contract_name varchar,
   contract_number varchar,
   sign_date varchar,
   fac_date varchar,
   customer_segment_l1 varchar,
   customer_segment_l2 varchar,
   customer_segment_l3 varchar,
   customer_group varchar,
   channel_name varchar,
   territory_name varchar,
   customer_id varchar,
   customer_name varchar,
   customer_alias varchar,
   customer_tax_code varchar,
   partner_id varchar,
   partner_name varchar,
   am_user_id varchar,
   am_email varchar,
   am_username varchar,
   team_name varchar,
   allocated_record_id varchar,
   pid varchar,
   parent_product_category varchar,
   product_category varchar,
   product_description varchar,
   product_version varchar,
   product_service_group varchar,
   deployment_type varchar,
   allocation_method varchar,
   revenue_frequency_type varchar,
   payment_index integer,
   product_total_value double,
   accumulated_recognized_revenue double,
   remaining_amount double,
   recognition_date varchar,
   source_revenue_type varchar,
   allocated_revenue_id varchar,
   product_category_code varchar,
   actual_start varchar,
   forecast_start varchar
)
WITH (
   external_location = 's3a://crm-data-tool/genbi/allocated-revenue',
   format = 'PARQUET'
);

alter table hive.crm_tool.crm_allocated_revenue add column actual_start varchar;
alter table hive.crm_tool.crm_allocated_revenue add column forecast_start varchar;

alter table hive.crm_tool.crm_allocated_revenue add column actual_start varchar;
alter table hive.crm_tool.crm_allocated_revenue add column forecast_start varchar;

ALTER TABLE hive.crm_tool.crm_allocated_revenue
ADD COLUMN currency varchar;

ALTER TABLE hive.crm_tool.crm_allocated_revenue
ADD COLUMN currency_amount double;

ALTER TABLE hive.crm_tool.crm_allocated_revenue
ADD COLUMN due_date varchar;

ALTER TABLE hive.crm_tool.crm_allocated_revenue
ADD COLUMN expected_close varchar;

ALTER TABLE hive.crm_tool.crm_allocated_revenue
ADD COLUMN closed_date varchar;

ALTER TABLE hive.crm_tool.crm_allocated_revenue
ADD COLUMN contract_expire_date varchar;

ALTER TABLE hive.crm_tool.crm_allocated_revenue
ADD COLUMN recurring boolean;

ALTER TABLE hive.crm_tool.crm_allocated_revenue
ADD COLUMN total_vcs_value double;

ALTER TABLE hive.crm_tool.crm_allocated_revenue
ADD COLUMN coefficient double;

ALTER TABLE hive.crm_tool.crm_allocated_revenue
ADD COLUMN product_type varchar;

ALTER TABLE hive.crm_tool.crm_allocated_revenue
ADD COLUMN spdv_type varchar;

alter table hive.crm_tool.crm_allocated_revenue add column forecast_category  varchar