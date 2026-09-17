WITH last_deals AS (
    SELECT
        id,
        custom_field.cf__global_discount AS global_discount
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
    id AS deal_id,

    CAST(
        json_extract_scalar(global_discount, '$.value')
        AS DOUBLE
    ) AS global_discount_value,

    json_extract_scalar(
        global_discount,
        '$.type'
    ) AS global_discount_type

FROM last_deals;
