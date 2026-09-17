CREATE TABLE IF NOT EXISTS cx_cso_raw.fact_cso_tickets_sla (
          ticket_id BIGINT,
  agent_name STRING,
  status STRING,
  priority STRING,
  resolution_time_in_business_hours DOUBLE,
  agent_reply_count BIGINT,
  first_response_date STRING,
  ticket_type STRING,
  tickets_first_responded_within_sla STRING,
  tickets_resolved_within_sla STRING,
  ttr_time STRING,
  crawled_at_ts BIGINT
        )
        USING PARQUET
        LOCATION 's3a://vcs-raw/cx-cso-raw/fact_cso_tickets_sla'