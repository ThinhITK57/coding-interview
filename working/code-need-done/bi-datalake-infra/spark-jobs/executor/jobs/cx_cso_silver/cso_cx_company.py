# %livy.pyspark
spark.catalog.clearCache()
spark.sql("REFRESH TABLE cx_cso_raw.dim_cso_companies")
spark.sql("REFRESH TABLE cx_cso_raw.cx_company")
spark.sql("REFRESH TABLE cx_cso_raw.dim_cso_contacts")

tgt_table = "cx_cso_silver.cso_cx_company"
tgt_path  = "s3a://vcs-silver/cx-cso-silver/cso_cx_company"

sql_query = """
WITH cso_company AS (
    SELECT *
    FROM (
        SELECT id, name, custom_fields, org_company_id, domains, created_at, updated_at,
               ROW_NUMBER() OVER (
                   PARTITION BY id
                   ORDER BY updated_at_ts DESC, id DESC
               ) rn
        FROM cx_cso_raw.dim_cso_companies
    ) t
    WHERE rn = 1
),
cx_company AS (
    SELECT
        CAST(id AS STRING) AS id,
        CAST(tax_code AS STRING) AS tax_code,
        CAST(customer_segment_l1 AS STRING) AS segment_level1,
        CAST(customer_segment_l2 AS STRING) AS segment_level2,
        CAST(customer_segment_l3 AS STRING) AS segment_level3
    FROM cx_cso_raw.cx_company
)
,cso_contact AS (
    SELECT
        company_id,
        contact_id,
        email as contact_email,
        name as contact_name,
        contact_level
    FROM (
        SELECT
            company_id,
            id as contact_id,
            email,
            name,
            custom_fields.level as contact_level,
            ROW_NUMBER() OVER (
                PARTITION BY company_id, email
                ORDER BY crawled_at_ts DESC
            ) AS rn
        FROM cx_cso_raw.dim_cso_contacts
        WHERE company_id IS NOT NULL
        AND email IS NOT NULL
    ) t
    WHERE rn = 1
 )

SELECT
    CAST(s.id AS STRING) AS cso_company_id,
    CAST(ct.contact_id AS STRING) AS contact_id,

    REGEXP_REPLACE(c.tax_code, '[^0-9]', '') AS company_tax_code,
    get_json_object(to_json(s.custom_fields), '$.alias') AS company_alias,
    CAST(s.name AS STRING) AS company_name,


    get_json_object(to_json(s.custom_fields), '$.company_segment') AS company_group,
    get_json_object(to_json(s.custom_fields), '$.company_type') AS company_type,
    get_json_object(to_json(s.custom_fields), '$.account_tier') AS company_account_tier,
   

    c.segment_level1 AS company_segement_level1,
    c.segment_level2 AS company_segement_level2,
    c.segment_level3 AS company_segement_level3,
    

    ct.contact_email AS contact_email,
    ct.contact_name AS contact_name,
    ct.contact_level AS contact_level,


    CAST(s.org_company_id AS STRING) AS org_company_id,
    concat_ws(',', s.domains) AS domains,

    CAST(s.created_at AS TIMESTAMP) AS created_at,
    CAST(s.updated_at AS TIMESTAMP) AS updated_at,
    CURRENT_TIMESTAMP() AS inserted_ts
FROM cso_company s
LEFT JOIN cx_company c
    ON CAST(s.id AS STRING) = c.id
LEFT JOIN cso_contact ct
    ON CAST(s.id AS STRING) = ct.company_id
"""

df = spark.sql(sql_query)

(df.repartition(1).write
  .mode("overwrite")
  .format("parquet")
  .save(tgt_path)
)

#   .option("path", tgt_path)
#   .saveAsTable(tgt_table)
#   .save(tgt_path)

spark.catalog.refreshTable(tgt_table)

print(tgt_table)
spark.sql("SELECT count(*) cnt FROM " + tgt_table).show()