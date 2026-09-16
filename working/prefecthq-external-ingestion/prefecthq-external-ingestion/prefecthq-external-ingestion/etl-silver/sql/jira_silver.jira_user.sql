SELECT
    -- ========= IDENTIFIER =========
    user_id       AS user_id,
    user_key      AS user_key,

    -- ========= USER INFO =========
    username      AS username,
    display_name  AS display_name,
    email         AS email,

    -- ========= STATUS =========
    active        AS is_active,
    directory_id  AS directory_id

    -- ========= META =========
--    source_file   AS source_file
FROM hive.jira_raw.jira_user;
