WITH last_sales_accounts AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY id
                   ORDER BY updated_at_ts DESC, id DESC
               ) rn
        FROM hive.crm_raw.sales_accounts ss
        WHERE NOT EXISTS (
            SELECT 1
            FROM hive.crm_raw.deleted_sales_accounts d
            WHERE CAST(d.id AS BIGINT) = ss.id
        )
    ) t
    WHERE rn = 1
)

SELECT
    CAST(s.id AS VARCHAR)   AS sale_id,
    u.user_id               AS user_id
FROM last_sales_accounts s
CROSS JOIN UNNEST(
    COALESCE(s.team_user_ids, ARRAY[])
) AS u(user_id)
WHERE is_deleted = false;
