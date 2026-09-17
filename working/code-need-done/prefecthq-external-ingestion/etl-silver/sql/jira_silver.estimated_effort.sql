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
        FROM hive.jira_raw.ulnl
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
    assignee    AS assignee,
    ttsx_product AS ttsx_product,

    -- ========= EFFORT =========
    total_net_effort AS total_net_effort,

    -- ========= RAW STRING TIME =========
--    resolved    AS resolved_str,
--    updated     AS updated_str,

    -- ========= TRY_CAST STRING → TIMESTAMP =========
    TRY_CAST(resolved AS TIMESTAMP) AS resolved_time,
    TRY_CAST(updated  AS TIMESTAMP) AS updated_time

    -- ========= TRY_CAST EPOCH (ms) → TIMESTAMP =========
--    TRY_CAST(from_unixtime(resolved_ts / 1000) AS TIMESTAMP) AS resolved_time_ts,
--    TRY_CAST(from_unixtime(updated_ts  / 1000) AS TIMESTAMP) AS updated_time_ts,

    -- ========= RAW TS =========
--    resolved_ts,
--    updated_ts,

    -- ========= META =========
--    source_file
FROM last_items_cte;
