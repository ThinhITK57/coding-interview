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
    CAST(s.id AS VARCHAR)      AS sale_id,
    trim(p)                   AS interested_product
FROM last_sales_accounts s
CROSS JOIN UNNEST(
    split(
        COALESCE(s.custom_field.cf_interested_products, ''),
        ';'
    )
) AS u(p)
WHERE is_deleted = false
  AND trim(p) <> '';
