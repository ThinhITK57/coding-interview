CREATE VIEW hive.jira_silver.jira_kpi_fct_vw AS
SELECT
    'KQI'                      AS kpi_type,
    issue_key,
    summary,
    CAST(NULL AS STRING)       AS issue_type,
    CAST(NULL AS STRING)       AS status,
    service_product            AS product_name,
    reporting_date_time       AS reporting_date,
    'execution_result'         AS kpi_name,
    execution_result           AS kpi_value,
    CAST(target AS DOUBLE)     AS target_value,
    unit_of_measure            AS unit_of_measure,
    updated_time         AS updated_time
FROM jira_silver.kpi_kqi
UNION ALL
SELECT
    'STAFF_KPI',
    issue_key,
    summary,
    issue_type,
    status,
    product_name,
    reporting_date_time as reporting_date,
    'agent_latest',
    agent_latest,
    CAST(NULL AS DOUBLE),
    CAST(NULL AS DOUBLE),
    updated_time
FROM jira_silver.kpi_binh_staff

UNION ALL
SELECT
    'PROCESS_COMPLIANCE',
    issue_key,
    summary,
    issue_type,
    CAST(NULL AS STRING),
    ttqt_product_name,
    due_date_time,
    'process_compliance_rate',
    process_compliance_rate,
    CAST(NULL AS DOUBLE),
    CAST(NULL AS DOUBLE),
    updated_time
FROM jira_silver.process_compliance