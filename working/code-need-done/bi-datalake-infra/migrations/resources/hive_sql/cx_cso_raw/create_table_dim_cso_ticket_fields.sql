CREATE TABLE IF NOT EXISTS cx_cso_raw.raw_dim_cso_ticket_fields (
          id BIGINT,
  name STRING,
  label STRING,
  description STRING,
  position BIGINT,
  required_for_closure BOOLEAN,
  required_for_agents BOOLEAN,
  type STRING,
  default BOOLEAN,
  customers_can_edit BOOLEAN,
  customers_can_filter BOOLEAN,
  label_for_customers STRING,
  required_for_customers BOOLEAN,
  displayed_to_customers BOOLEAN,
  created_at STRING,
  updated_at STRING,
  portal_cc BOOLEAN,
  portal_cc_to STRING,
  created_at_ts BIGINT,
  updated_at_ts BIGINT,
  choices STRING,
  nested_ticket_fields ARRAY<STRUCT<id:BIGINT, name:STRING, description:STRING, label:STRING, label_in_portal:STRING, level:BIGINT, ticket_field_id:BIGINT, created_at:STRING, updated_at:STRING>>,
  crawled_at_ts BIGINT
        )
        USING PARQUET
        LOCATION 's3a://vcs-raw/cx-cso-raw/dim_cso_ticket_fields'