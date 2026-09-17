WITH last_items_cte AS (
    SELECT
        *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY key
                ORDER BY updated_ts DESC
            ) AS rn
        FROM hive.jira_raw.defect_task
    ) t
    WHERE rn = 1
)

SELECT
    -- ========= IDENTIFIER =========
    key         AS issue_key,

    -- ========= PROJECT =========
    projectkey  AS project_key,
    projectname AS project_name,

    -- ========= ISSUE INFO =========
    summary     AS summary,
    status      AS status,
    priority    AS priority,

    -- ========= RAW STRING TIME =========
--    created   AS created_str,
--    updated   AS updated_str,
--    resolved  AS resolved_str,

    -- ========= TRY_CAST STRING → TIMESTAMP =========
    TRY_CAST(created  AS TIMESTAMP) AS created_time,
    TRY_CAST(updated  AS TIMESTAMP) AS updated_time,
    TRY_CAST(resolved AS TIMESTAMP) AS resolved_time

    -- ========= TRY_CAST EPOCH (ms) → TIMESTAMP =========
--    TRY_CAST(from_unixtime(created_ts  / 1000) AS TIMESTAMP) AS created_time_ts,
--    TRY_CAST(from_unixtime(updated_ts  / 1000) AS TIMESTAMP) AS updated_time_ts,
--    TRY_CAST(from_unixtime(resolved_ts / 1000) AS TIMESTAMP) AS resolved_time_ts,

    -- ========= RAW TS =========
--    created_ts,
--    updated_ts,
--    resolved_ts,

    -- ========= META =========
--    source_file
FROM last_items_cte;
