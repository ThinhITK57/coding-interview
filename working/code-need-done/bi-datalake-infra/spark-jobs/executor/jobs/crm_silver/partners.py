# %livy.pyspark

sql_query = """
WITH last_cm_partners AS (
    SELECT *
    FROM (
        SELECT
            ss.*,
            ROW_NUMBER() OVER (
                PARTITION BY id
                ORDER BY updated_at_ts DESC, id DESC
            ) AS rn
        FROM crm_raw.cm_partners ss
    ) t
    WHERE rn = 1
)

SELECT
    p.id,
    p.name,
--    p.owner_id,

    -- custom_field
    p.custom_field.cf_document_number     AS document_number,
    p.custom_field.cf_alias               AS company_alias,
    p.custom_field.cf_address             AS address,
    p.custom_field.cf_country             AS country,
    p.custom_field.cf_contact_name        AS contact_name,
    p.custom_field.cf_contact_position    AS contact_position,
    p.custom_field.cf_contact_mobile      AS contact_mobile,
    p.custom_field.cf_contact_email       AS contact_email,
    p.custom_field.cf_effective_date      AS effective_date,
    p.custom_field.cf_expiration_date     AS expiration_date,
    trim(split(p.custom_field.cf_type, '/')[1])             AS parter_type,
    trim(split(p.custom_field.cf_document_format, '/')[1])  AS document_format,
    trim(split(p.custom_field.cf_status, '/')[1])           AS status,
    trim(split(p.custom_field.cf_type, '/')[2])             AS parter_type_vi,
    trim(split(p.custom_field.cf_document_format, '/')[2])  AS document_format_vi,
    trim(split(p.custom_field.cf_status, '/')[2])           AS status_vi


--    p.created_at,
--    p.creator_id,
--    p.updated_at,
--    p.updater_id,
--    p.avatar,
--    p.recent_note,

    -- links
--    p.links.document_associations         AS document_associations,
--    p.links.notes                         AS notes,
--
--    p.record_type_id,
--    p.created_at_ts,
--    p.updated_at_ts

FROM last_cm_partners p


"""

df = spark.sql(sql_query)

df.write \
    .mode("overwrite") \
    .format("parquet") \
    .option(
        "path",
        "s3a://vcs-silver/crm-silver/partners"
    ) \
    .saveAsTable("crm_silver.partners")
