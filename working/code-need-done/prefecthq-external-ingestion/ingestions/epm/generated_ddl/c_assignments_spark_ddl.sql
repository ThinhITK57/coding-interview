-- Spark SQL DDL cho bang 'c_assignments' (39 cot tu data_type/)
-- Chay tren Zeppelin (%spark.sql).
-- Bang KHONG partition: dat thang cac file .parquet vao LOCATION.
-- 'ingest_date' la cot DATE binh thuong nam trong file.

CREATE DATABASE IF NOT EXISTS personal_raw;

DROP TABLE IF EXISTS personal_raw.c_assignments;

CREATE TABLE personal_raw.c_assignments (
    c_countof_sub_assignments DOUBLE,
    c_achievement_rate DOUBLE,
    c_assignee STRING,
    c_assignment_number STRING,
    c_assignor STRING,
    c_department STRING,
    c_end_date DATE,
    c_essential_weight_total_percent DOUBLE,
    c_parent_assignment STRING,
    c_reporter STRING,
    c_start_date DATE,
    c_sum_target_weight_percent DOUBLE,
    c_sum_target_result_weight_all DOUBLE,
    c_sum_target_w_result_compliance DOUBLE,
    c_total_assignment_weight_result DOUBLE,
    c_total_weight DOUBLE,
    c_update_description STRING,
    c_weight_bonus DOUBLE,
    c_weight_bonus_description STRING,
    c_weight_collaboration DOUBLE,
    c_weight_collaboration_description STRING,
    c_weight_compliance DOUBLE,
    c_weight_policy DOUBLE,
    c_weight_policy_description STRING,
    emails_count DOUBLE,
    created_by STRING,
    created_on DATE,
    currency_exchange_date DATE,
    default_integration_path STRING,
    description STRING,
    external_id STRING,
    sysid STRING,
    image_url STRING,
    entity_type STRING,
    last_updated_by STRING,
    last_updated_on DATE,
    name STRING,
    entity_owner STRING,
    last_updated_by_system_on DATE,
    _raw_payload STRING,
    _batch_id STRING,
    _ingest_timestamp TIMESTAMP,
    ingest_date DATE
)
USING PARQUET
LOCATION '/opt/datasets/crawlers/vcs/clarizen/data/personal_raw/c_assignments';

CREATE DATABASE IF NOT EXISTS global_clean;

DROP TABLE IF EXISTS global_clean.c_assignments;

CREATE TABLE global_clean.c_assignments (
    c_countof_sub_assignments DOUBLE,
    c_achievement_rate DOUBLE,
    c_assignee STRING,
    c_assignment_number STRING,
    c_assignor STRING,
    c_department STRING,
    c_end_date DATE,
    c_essential_weight_total_percent DOUBLE,
    c_parent_assignment STRING,
    c_reporter STRING,
    c_start_date DATE,
    c_sum_target_weight_percent DOUBLE,
    c_sum_target_result_weight_all DOUBLE,
    c_sum_target_w_result_compliance DOUBLE,
    c_total_assignment_weight_result DOUBLE,
    c_total_weight DOUBLE,
    c_update_description STRING,
    c_weight_bonus DOUBLE,
    c_weight_bonus_description STRING,
    c_weight_collaboration DOUBLE,
    c_weight_collaboration_description STRING,
    c_weight_compliance DOUBLE,
    c_weight_policy DOUBLE,
    c_weight_policy_description STRING,
    emails_count DOUBLE,
    created_by STRING,
    created_on DATE,
    currency_exchange_date DATE,
    default_integration_path STRING,
    description STRING,
    external_id STRING,
    sysid STRING,
    image_url STRING,
    entity_type STRING,
    last_updated_by STRING,
    last_updated_on DATE,
    name STRING,
    entity_owner STRING,
    last_updated_by_system_on DATE,
    _batch_id STRING,
    _ingest_timestamp TIMESTAMP,
    ingest_date DATE
)
USING PARQUET
LOCATION '/opt/datasets/crawlers/vcs/clarizen/data/global_clean/c_assignments';
