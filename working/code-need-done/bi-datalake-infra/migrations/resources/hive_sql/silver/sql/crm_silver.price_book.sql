WITH last_cm_pricebook AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY id
                   ORDER BY updated_at_ts DESC, id DESC
               ) rn
        FROM hive.crm_raw.cm_pricebook ss
        WHERE NOT EXISTS (
            SELECT 1
            FROM hive.crm_raw.deleted_cm_pricebook d
            WHERE CAST(d.id AS BIGINT) = ss.id
        )
    ) t
    WHERE rn = 1
)
SELECT
    id,
    name,
--    owner_id ,

    -- custom_field
    custom_field.cf_sku                         AS sku,
    custom_field.cf_type                        AS product_type,
    custom_field.cf_sub_type                    AS sub_type,
    custom_field.cf_license                     AS license,
    custom_field.cf_price_type                  AS price_type,
    custom_field.cf_package                     AS package_type,
    custom_field.cf_price                       AS price,
    custom_field.cf_currency                    AS currency_code,
    custom_field.cf_min                         AS price_min,
    custom_field.cf_max                         AS price_max,
    custom_field.cf_unit                        AS product_unit,
--    custom_field.cf_is_quantity_based           AS cf_is_quantity_based,
--    custom_field.cf_is_related_csmp             AS cf_is_related_csmp,
    custom_field.cf_csmp_discount               AS csmp_discount_rate,
    custom_field.cf_csmp_discount_silver        AS csmp_discount_silver_rate,
    custom_field.cf_csmp_discount_gold          AS csmp_discount_gold_rate,
    custom_field.cf_csmp_discount_diamond       AS csmp_discount_diamond_rate,
    custom_field.cf_catalog                     AS catalog_id
--    custom_field.cf_related_pricebook           AS cf_related_pricebook,

--    created_at,
--    creator_id,
--    updated_at,
--    updater_id,
--    avatar,
--    recent_note,

    -- links
--    links.document_associations                 AS document_associations,
--    links.notes                                 AS notes,

--    record_type_id,
--    created_at_ts,
--    updated_at_ts
FROM last_cm_pricebook;
