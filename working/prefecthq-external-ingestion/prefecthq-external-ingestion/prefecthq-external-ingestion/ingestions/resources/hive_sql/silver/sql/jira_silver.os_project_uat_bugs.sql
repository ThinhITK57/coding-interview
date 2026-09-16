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
        FROM hive.jira_raw.os_project_uat_bugs
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
    issuetype   AS issue_type,
    status      AS status,

    -- ========= RAW STRING TIME =========
--    created     AS created_str,
--    updated     AS updated_str,

    -- ========= TRY_CAST STRING → TIMESTAMP =========
    TRY_CAST(created AS TIMESTAMP) AS created_time,
    TRY_CAST(updated AS TIMESTAMP) AS updated_time

    -- ========= TRY_CAST EPOCH (ms) → TIMESTAMP =========
--    TRY_CAST(from_unixtime(created_ts / 1000) AS TIMESTAMP) AS created_time_ts,
--    TRY_CAST(from_unixtime(updated_ts / 1000) AS TIMESTAMP) AS updated_time_ts,

    -- ========= RAW TS =========
--    created_ts,
--    updated_ts,

    -- ========= META =========
--    source_file
FROM last_items_cte;
