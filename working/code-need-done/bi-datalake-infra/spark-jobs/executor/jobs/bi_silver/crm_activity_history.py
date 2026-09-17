# %livy.pyspark

tgt_table = "bi_silver.crm_activity_history"
tgt_path  = "s3a://bi-silver/crm_activity_history"

# Làm mới danh mục file nguồn
spark.sql("REFRESH TABLE crm_raw.cm_activity_history")
spark.catalog.clearCache()

sql_query = """
WITH last_cm_activity_history AS (
    SELECT *
    FROM (
        SELECT
            ss.*,
            ROW_NUMBER() OVER (
                PARTITION BY id
                ORDER BY
                    updated_at_ts DESC,
                    created_at_ts DESC,
                    crawled_at_ts DESC
            ) AS rn
        FROM crm_raw.cm_activity_history ss
    ) t
    WHERE rn = 1
),

base AS (
    SELECT
        c.*,

        TRIM(c.name) AS activity_subject,

        CAST(c.owner_id AS STRING) AS owner_user_id,

        CAST(c.custom_field.cf_related_contact AS STRING) AS contact_id,
        CAST(c.custom_field.cf_related_company AS STRING) AS company_id,
        CAST(c.custom_field.cf_related_opportunity AS STRING) AS deal_id

    FROM last_cm_activity_history c
),

normalized AS (
    SELECT
        b.*,

        /*
         * Normalize action
         */
        CASE
            WHEN regexp_like(
                LOWER(TRIM(b.name)),
                '(^|[ |_-])(add|create|created|new)([ |_-]|$)'
            )
                THEN 'CREATE'

            WHEN regexp_like(
                LOWER(TRIM(b.name)),
                '(^|[ |_-])(update|updated|edit|edited)([ |_-]|$)'
            )
                THEN 'UPDATE'

            WHEN regexp_like(
                LOWER(TRIM(b.name)),
                '(^|[ |_-])(delete|deleted)([ |_-]|$)'
            )
                THEN 'DELETE'

            ELSE 'ACTION'
        END AS activity_action,

        /*
         * Normalize entity
         */
        CASE
            WHEN LOWER(TRIM(b.name)) = 'action on company'
                THEN 'COMPANY'

            WHEN LOWER(TRIM(b.name)) = 'action on opportunity'
                THEN 'OPPORTUNITY'

            WHEN LOWER(TRIM(b.name)) LIKE 'product | %'
                THEN 'OPPORTUNITY'

            WHEN b.deal_id IS NOT NULL
                THEN 'OPPORTUNITY'

            WHEN b.company_id IS NOT NULL
                THEN 'COMPANY'

            WHEN b.contact_id IS NOT NULL
                THEN 'CONTACT'

            ELSE 'OTHER'
        END AS entity_type

    FROM base b
)

SELECT
    /*
     * =========================================================
     * Activity identity
     * =========================================================
     */
    CAST(n.id AS STRING) AS id,

    TRIM(n.name) AS activity_name,
    n.activity_subject,

    CAST(n.created_at AS TIMESTAMP) AS created_at,
    CAST(n.updated_at AS TIMESTAMP) AS updated_at,

    CAST(n.created_at AS DATE) AS activity_date,

    /*
     * =========================================================
     * Users
     * =========================================================
     */

    -- Owner of CRM object
    n.owner_user_id,

    u_owner.display_name AS owner_user_name,

    -- Actor / creator
    CAST(n.creator_id AS STRING) AS actor_user_id,

    u_actor.display_name AS actor_user_name,

    -- Keep original fields for traceability
    CAST(n.creator_id AS STRING) AS creator_id,
    CAST(n.updater_id AS STRING) AS updater_id,

    /*
     * =========================================================
     * AM
     * =========================================================
     */

    n.owner_user_id AS am_user_id,
    u_owner.display_name AS am_user_name,

    CASE
        WHEN LOWER(TRIM(u_owner.team_name)) = 'unnamed'
             AND LOWER(TRIM(u_owner.job_title)) = 'am unnamed account'
            THEN 'UNNAMED'

        WHEN LOWER(TRIM(u_owner.team_name)) = 'trọng điểm'
             OR LOWER(TRIM(u_owner.job_title)) = 'am vvip'
            THEN 'VVIP'

        WHEN LOWER(TRIM(u_owner.job_title))
                 IN ('am kênh', 'am kinh doanh kênh')
             OR LOWER(TRIM(u_owner.team_name)) = 'kênh'
            THEN 'CHANNEL'

        WHEN LOWER(TRIM(u_owner.job_title)) = 'am nội bộ'
             OR LOWER(TRIM(u_owner.team_name)) = 'nội bộ'
            THEN 'INTERNAL'

        WHEN LOWER(TRIM(u_owner.job_title))
                 IN ('am', 'bdm', 'global business development')
            THEN 'INTERNATIONAL'

        ELSE 'OTHER'
    END AS am_group,

    CASE
        WHEN u_owner.is_active = TRUE
            THEN 'ACTIVE'

        WHEN u_owner.is_active = FALSE
            THEN 'INACTIVE'

        ELSE 'UNKNOWN'
    END AS am_status,

    /*
     * =========================================================
     * Related CRM objects
     * =========================================================
     */

    n.contact_id,
    n.company_id,
    n.deal_id,

    CAST(NULL AS STRING) AS quotation_id,
    CAST(NULL AS STRING) AS contract_id,

    /*
     * =========================================================
     * Entity
     * =========================================================
     */

    n.entity_type,

    CASE
        WHEN n.entity_type = 'COMPANY'
            THEN n.company_id

        WHEN n.entity_type = 'CONTACT'
            THEN n.contact_id

        WHEN n.entity_type = 'OPPORTUNITY'
            THEN n.deal_id

        ELSE NULL
    END AS entity_id,

    /*
     * =========================================================
     * Action
     * =========================================================
     */

    n.activity_action,

    /*
     * =========================================================
     * Activity type
     * =========================================================
     */

    CASE
        WHEN n.entity_type = 'COMPANY'
             AND n.activity_action = 'CREATE'
            THEN 'CREATE_COMPANY'

        WHEN n.entity_type = 'COMPANY'
             AND n.activity_action = 'UPDATE'
            THEN 'UPDATE_COMPANY'

        WHEN n.entity_type = 'COMPANY'
             AND n.activity_action = 'DELETE'
            THEN 'DELETE_COMPANY'


        WHEN n.entity_type = 'CONTACT'
             AND n.activity_action = 'CREATE'
            THEN 'CREATE_CONTACT'

        WHEN n.entity_type = 'CONTACT'
             AND n.activity_action = 'UPDATE'
            THEN 'UPDATE_CONTACT'

        WHEN n.entity_type = 'CONTACT'
             AND n.activity_action = 'DELETE'
            THEN 'DELETE_CONTACT'


        WHEN n.entity_type = 'OPPORTUNITY'
             AND n.activity_action = 'CREATE'
            THEN 'CREATE_OPPORTUNITY'

        WHEN n.entity_type = 'OPPORTUNITY'
             AND n.activity_action = 'UPDATE'
            THEN 'UPDATE_OPPORTUNITY'

        WHEN n.entity_type = 'OPPORTUNITY'
             AND n.activity_action = 'DELETE'
            THEN 'DELETE_OPPORTUNITY'

        WHEN n.entity_type = 'OPPORTUNITY'
             AND LOWER(n.name) LIKE 'product | %'
            THEN 'UPDATE_PRODUCT'

        WHEN LOWER(n.name) LIKE '%quotation%'
            THEN 'QUOTATION_ACTION'

        WHEN LOWER(n.name) LIKE '%contract%'
            THEN 'CONTRACT_ACTION'

        ELSE 'OTHER'
    END AS activity_type,

    /*
     * =========================================================
     * Activity category
     * =========================================================
     */

    CASE
        WHEN LOWER(n.name) LIKE 'product | %'
            THEN 'SALES_PROGRESS'

        WHEN LOWER(n.name) LIKE '%quotation%'
            THEN 'SALES_PROGRESS'

        WHEN LOWER(n.name) LIKE '%contract%'
            THEN 'BUSINESS_OUTCOME'

        WHEN n.activity_action IN ('CREATE', 'UPDATE', 'DELETE')
            THEN 'CRM_COMPLIANCE'

        ELSE 'SALES_ACTIVITY'
    END AS activity_category,

    /*
     * =========================================================
     * KPI qualification
     * =========================================================
     */

    CASE
        WHEN n.entity_type IN (
            'COMPANY',
            'CONTACT',
            'OPPORTUNITY'
        )
        AND n.activity_action IN (
            'CREATE',
            'UPDATE',
            'DELETE'
        )
            THEN TRUE

        WHEN LOWER(n.name) LIKE 'product | %'
            THEN TRUE

        WHEN LOWER(n.name) LIKE '%quotation%'
            THEN TRUE

        WHEN LOWER(n.name) LIKE '%contract%'
            THEN TRUE

        ELSE FALSE
    END AS is_countable,

    /*
     * =========================================================
     * CRM object attributes
     * =========================================================
     */

    d.deal_name,
    sa.name AS company_name,

    n.recent_note,
    CAST(n.record_type_id AS STRING) AS record_type_id,

    /*
     * Keep original source metadata
     */
    CAST(n.owner_id AS STRING) AS source_owner_id

FROM normalized n

LEFT JOIN bi_silver.crm_users u_owner
    ON CAST(n.owner_user_id AS STRING)
       = CAST(u_owner.user_id AS STRING)

LEFT JOIN bi_silver.crm_users u_actor
    ON CAST(n.creator_id AS STRING)
       = CAST(u_actor.user_id AS STRING)

LEFT JOIN bi_silver.crm_deals d
    ON CAST(n.deal_id AS STRING) = d.deal_id

LEFT JOIN bi_silver.crm_sales_accounts sa
    ON CAST(n.company_id AS STRING) = sa.id

Where u_owner.role_name = 'AM'
"""

df = spark.sql(sql_query)

# 4) Save
df.repartition(1).write  \
  .mode("overwrite") \
  .format("parquet") \
  .save(tgt_path)

#  .save(tgt_path)

  # .option("path", tgt_path) \
  # .saveAsTable(tgt_table)
  
spark.catalog.refreshTable(tgt_table)

print(tgt_table)
# 5) Quick check
spark.sql("SELECT * FROM " + tgt_table + " LIMIT 1").show(truncate=False)
