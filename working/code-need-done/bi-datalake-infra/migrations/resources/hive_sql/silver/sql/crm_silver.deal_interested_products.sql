WITH last_deals AS (
    SELECT
        id,
        custom_field.cf_interested_products AS interested_products
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY id
                ORDER BY updated_at_ts DESC, id DESC
            ) AS rn
        FROM hive.crm_raw.deals ss
        WHERE is_deleted = false
          AND NOT EXISTS (
              SELECT 1
              FROM hive.crm_raw.deleted_deals d
              WHERE CAST(d.id AS bigint) = ss.id
          )
    ) t
    WHERE rn = 1
)

SELECT
    t.id        AS deal_id,
    trim(p)     AS interested_product
FROM last_deals t
CROSS JOIN UNNEST(
    split(t.interested_products, ';')
) AS u(p);
