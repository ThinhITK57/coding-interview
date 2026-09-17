# %livy.pyspark

sql_query = """
WITH last_cm_catalog AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY id
                   ORDER BY updated_at_ts DESC, id DESC
               ) rn
        FROM crm_raw.cm_catalog ss
    ) t
    WHERE rn = 1
)
SELECT
    id,
    name,
--    owner_id,

    -- custom_field
    custom_field.cf_category        AS category,
    custom_field.cf_version         AS category_version,
    custom_field.cf_max_discount    AS max_discount,
    custom_field.cf_item_type       AS item_type,
    custom_field.cf_active          AS is_active
--    custom_field.cf_market          AS cf_market

--    created_at,
--    creator_id,
--    updated_at,
--    updater_id,
--    avatar,
--    recent_note,

    -- links
--    links.document_associations     AS document_associations,
--    links.notes                     AS notes,

--    record_type_id,
--    created_at_ts,
--    updated_at_ts
FROM crm_raw.cm_catalog

"""

df = spark.sql(sql_query)

df.write \
    .mode("overwrite") \
    .format("parquet") \
    .option(
        "path",
        "s3a://vcs-silver/crm-silver/catalogs"
    ) \
    .saveAsTable("crm_silver.catalogs")
