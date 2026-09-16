WITH last_items_cte AS (
    SELECT
        *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY key
                ORDER BY updated_ts desc, key desc 
            ) AS rn
        FROM hive.jira_raw.kpi_binh_staff
    ) t
    WHERE rn = 1
)

SELECT
    -- ========= IDENTIFIER =========
    key          AS issue_key,

    -- ========= ISSUE INFO =========
    summary      AS summary,
    issuetype    AS issue_type,
    status       AS status,
    description AS description,

    -- ========= RAW STRING TIME =========
--    created              AS created_str,
--    updated              AS updated_str,
--    reporting_date       AS reporting_date_str,
--    update_start_date    AS update_start_date_str,
--    update_date          AS update_date_str,

    -- ========= TRY_CAST STRING → TIMESTAMP =========
    TRY_CAST(created           AS TIMESTAMP) AS created_time,
    TRY_CAST(updated           AS TIMESTAMP) AS updated_time,
    TRY_CAST(reporting_date    AS TIMESTAMP) AS reporting_date_time,
    TRY_CAST(update_start_date AS TIMESTAMP) AS update_start_date_time,
    TRY_CAST(update_date       AS TIMESTAMP) AS update_date_time,

    -- ========= TRY_CAST EPOCH (ms) → TIMESTAMP =========
--    TRY_CAST(from_unixtime(created_ts            / 1000) AS TIMESTAMP) AS created_time_ts,
--    TRY_CAST(from_unixtime(updated_ts            / 1000) AS TIMESTAMP) AS updated_time_ts,
--    TRY_CAST(from_unixtime(reporting_date_ts     / 1000) AS TIMESTAMP) AS reporting_date_time_ts,
--    TRY_CAST(from_unixtime(update_start_date_ts  / 1000) AS TIMESTAMP) AS update_start_date_time_ts,
--    TRY_CAST(from_unixtime(update_date_ts        / 1000) AS TIMESTAMP) AS update_date_time_ts,

    -- ========= KPI / BUSINESS =========
    agent_online      AS agent_online,
    agent_latest      AS agent_latest,
    latency_delay     AS latency_delay,

--    has_soc247        AS has_soc247,
    CASE WHEN lower(has_soc247) IN ('y','yes','true','1') THEN true ELSE false END AS has_soc247,
--    is_exception      AS is_exception,
    CASE WHEN lower(is_exception) IN ('y','yes','true','1') THEN true ELSE false END AS is_exception,
    exception_reason AS exception_reason,

    latest_version   AS latest_version,
    version          AS version,

    contract_type    AS contract_type,
    product_type     AS product_type,
    product_name     AS product_name,
    update_frequency AS update_frequency,

    -- ========= RAW TS =========
    created_ts,
    updated_ts
--    reporting_date_ts,
--    update_start_date_ts,
--    update_date_ts,

    -- ========= META =========
--    source_file
FROM last_items_cte;
