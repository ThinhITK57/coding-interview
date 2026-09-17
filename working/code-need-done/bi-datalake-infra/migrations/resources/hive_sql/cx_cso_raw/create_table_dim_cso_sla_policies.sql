CREATE TABLE IF NOT EXISTS cx_cso_raw.raw_dim_cso_sla_policies (
          id BIGINT,
  name STRING,
  description STRING,
  active BOOLEAN,
  sla_target STRUCT<priority_4:STRUCT<respond_within:BIGINT, resolve_within:BIGINT, next_respond_within:BIGINT, business_hours:BOOLEAN, escalation_enabled:BOOLEAN>, priority_3:STRUCT<respond_within:BIGINT, resolve_within:BIGINT, next_respond_within:BIGINT, business_hours:BOOLEAN, escalation_enabled:BOOLEAN>, priority_2:STRUCT<respond_within:BIGINT, resolve_within:BIGINT, next_respond_within:BIGINT, business_hours:BOOLEAN, escalation_enabled:BOOLEAN>, priority_1:STRUCT<respond_within:BIGINT, resolve_within:BIGINT, next_respond_within:BIGINT, business_hours:BOOLEAN, escalation_enabled:BOOLEAN>>,
  applicable_to STRUCT<ticket_types:ARRAY<STRING>, _dummy_:BIGINT>,
  is_default BOOLEAN,
  position BIGINT,
  created_at STRING,
  updated_at STRING,
  escalation STRUCT<reminder_response:STRUCT<_dummy_:BIGINT>, reminder_resolution:STRUCT<_dummy_:BIGINT>, response:STRUCT<_dummy_:BIGINT>, resolution:STRUCT<_dummy_:BIGINT>, reminder_next_response:STRUCT<_dummy_:BIGINT>, next_response:STRUCT<_dummy_:BIGINT>>,
  created_at_ts BIGINT,
  updated_at_ts BIGINT,
  crawled_at_ts BIGINT
        )
        USING PARQUET
        LOCATION 's3a://vcs-raw/cx-cso-raw/dim_cso_sla_policies'