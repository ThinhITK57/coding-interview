WITH last_items_cte AS (
    SELECT
        *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY key
                ORDER BY updated_ts DESC, key DESC
            ) AS rn
        FROM hive.jira_raw.bug_prod
    ) t
    WHERE rn = 1
)

SELECT
    key         AS issue_key,
    projectkey  AS project_key,
    projectname AS project_name,
    summary     AS summary,
    issuetype   AS issue_type,
    status      AS status,
    assignee    AS assignee,
    priority    AS priority,

    -- ========= RAW STRING =========
--    created   AS created_str,
--    updated   AS updated_str,
--    resolved  AS resolved_str,
--    duedate   AS due_date_str,

    -- ========= TRY_CAST FROM STRING =========
    TRY_CAST(created  AS TIMESTAMP) AS created_time,
    TRY_CAST(updated  AS TIMESTAMP) AS updated_time,
    TRY_CAST(resolved AS TIMESTAMP) AS resolved_time,
    TRY_CAST(duedate  AS TIMESTAMP) AS due_date_time,
	CAST(created  AS TIMESTAMP) AS created_time,
CAST(updated  AS TIMESTAMP) AS updated_time,
CAST(resolved AS TIMESTAMP) AS resolved_time,
CAST(duedate  AS TIMESTAMP) AS due_date_time

    -- ========= TRY_CAST FROM EPOCH (ms) =========
--    TRY_CAST(from_unixtime(created_ts  / 1000) AS TIMESTAMP) AS created_time_ts,
--    TRY_CAST(from_unixtime(updated_ts  / 1000) AS TIMESTAMP) AS updated_time_ts,
--    TRY_CAST(from_unixtime(resolved_ts / 1000) AS TIMESTAMP) AS resolved_time_ts,
--    TRY_CAST(from_unixtime(duedate_ts  / 1000) AS TIMESTAMP) AS due_date_time_ts,

    -- ========= RAW TS =========
--    created_ts,
--    updated_ts,
--    resolved_ts,
--    duedate_ts,

--    source_file
FROM last_items_cte;
