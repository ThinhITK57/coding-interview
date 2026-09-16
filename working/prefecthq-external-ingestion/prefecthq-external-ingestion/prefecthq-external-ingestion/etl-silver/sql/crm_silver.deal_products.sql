WITH last_deals AS (
    SELECT
        id,
        custom_field.cf__products AS json_products
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY id
                ORDER BY updated_at_ts DESC, id DESC
            ) rn
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
    t.id                      AS deal_id,
    u.id                      AS product_id,
    u.pid                     AS pid,
    u.name                    AS product_name,
    u.category                AS category,
    u.allocationValue         AS allocation_value,
    u.basePrice               AS base_price,
    u.finalTotal              AS final_total,
    u.currency                AS currency
FROM last_deals t
CROSS JOIN UNNEST (
    CAST(
        COALESCE(
            json_parse(t.json_products),
            json_parse('[]')
        )
        AS ARRAY(
            ROW(
                id BIGINT,
                pid BIGINT,
                name VARCHAR,
                category VARCHAR,
                allocationValue DOUBLE,
                basePrice DOUBLE,
                finalTotal DOUBLE,
                currency VARCHAR
            )
        )
    )
) AS u (
    id,
    pid,
    name,
    category,
    allocationValue,
    basePrice,
    finalTotal,
    currency
);
