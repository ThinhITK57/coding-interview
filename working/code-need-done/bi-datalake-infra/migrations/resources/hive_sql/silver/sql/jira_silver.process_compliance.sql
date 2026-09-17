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
        FROM hive.jira_raw.process_compliance
    ) t
    WHERE rn = 1
)

SELECT
    -- ========= IDENTIFIER =========
    key AS issue_key,

    -- ========= ISSUE INFO =========
    summary     AS summary,
    issuetype   AS issue_type,
    ttqt_product_name AS ttqt_product_name,

    -- ========= KPI METRIC =========
    process_compliance_rate AS process_compliance_rate,

    -- ========= RAW STRING TIME =========
--    duedate     AS due_date_str,
--    updated     AS updated_str,

    -- ========= TRY_CAST STRING → TIMESTAMP =========
    TRY_CAST(duedate AS TIMESTAMP) AS due_date_time,
    TRY_CAST(updated AS TIMESTAMP) AS updated_time

    -- ========= TRY_CAST EPOCH (ms) → TIMESTAMP =========
--    TRY_CAST(from_unixtime(duedate_ts / 1000) AS TIMESTAMP) AS due_date_time_ts,
--    TRY_CAST(from_unixtime(updated_ts / 1000) AS TIMESTAMP) AS updated_time_ts,

    -- ========= RAW TS =========
--    duedate_ts,
--    updated_ts,

    -- ========= META =========
--    source_file
FROM last_items_cte;
