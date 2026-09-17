CREATE TABLE IF NOT EXISTS crm_raw.cm_kpi (
          id BIGINT,
  name STRING,
  owner_id BIGINT,
  custom_field STRUCT<cf_kpi_scope:STRING, cf_currency:STRING, cf_relate_kpi:BIGINT, cf_sale_target:STRING, cf_revenue_target:STRING, cf_sale_target_quarter_1:BIGINT, cf_sale_target_month_1:BIGINT, cf_sale_target_month_2:BIGINT, cf_sale_target_month_3:BIGINT, cf_revenue_target_quarter_1:BIGINT, cf_revenue_target_month_1:BIGINT, cf_revenue_target_month_2:BIGINT, cf_revenue_target_month_3:BIGINT, cf_sale_target_quarter_2:BIGINT, cf_sale_target_month_4:BIGINT, cf_sale_target_month_5:BIGINT, cf_sale_target_month_6:BIGINT, cf_revenue_target_quarter_2:BIGINT, cf_revenue_target_month_4:BIGINT, cf_revenue_target_month_5:BIGINT, cf_revenue_target_month_6:BIGINT, cf_sale_target_quarter_3:BIGINT, cf_sale_target_month_7:BIGINT, cf_sale_target_month_8:BIGINT, cf_sale_target_month_9:BIGINT, cf_revenue_target_quarter_3:BIGINT, cf_revenue_target_month_7:BIGINT, cf_revenue_target_month_8:BIGINT, cf_revenue_target_month_9:BIGINT, cf_sale_target_quarter_4:BIGINT, cf_sale_target_month_10:BIGINT, cf_sale_target_month_11:BIGINT, cf_sale_target_month_12:BIGINT, cf_revenue_target_quarter_4:BIGINT, cf_revenue_target_month_10:BIGINT, cf_revenue_target_month_11:BIGINT, cf_revenue_target_month_12:BIGINT>,
  created_at STRING,
  creator_id BIGINT,
  updated_at STRING,
  updater_id BIGINT,
  avatar STRING,
  recent_note STRING,
  links STRUCT<document_associations:STRING, notes:STRING>,
  record_type_id STRING,
  created_at_ts BIGINT,
  updated_at_ts BIGINT,
  crawled_at_ts BIGINT
        )
        USING PARQUET
        LOCATION 's3a://vcs-raw/crm-raw/cm_kpi'