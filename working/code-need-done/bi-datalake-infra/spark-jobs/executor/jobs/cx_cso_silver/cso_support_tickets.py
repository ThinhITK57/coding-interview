# %livy.pyspark

from pyspark.sql import functions as F
from functools import reduce

tgt_table = "cx_cso_silver.cx_cso_support_tickets"
tgt_path  = "s3a://vcs-silver/cx-cso-silver/cx_cso_support_tickets"

# 0) Refresh metadata
spark.sql("REFRESH TABLE cx_cso_raw.fact_cso_tickets")
spark.sql("REFRESH TABLE cx_cso_raw.fact_cso_tickets_sla")
spark.sql("REFRESH TABLE cx_cso_silver.cso_cx_company")

# 2) Chuẩn hoá Temp Views & Query Logic
# Lấy danh sách ticket mới nhất, loại bỏ các ticket đã bị deleted
df_latest_tickets = spark.sql("""
    SELECT 
        id,
        company_id,
        company,
        custom_fields,
        created_at,
        updated_at,
        fr_due_by,
        due_by,
        stats,
        subject,
        type,
        status,
        priority,
        requester
    FROM (
        SELECT
            id,
            company_id,
            company,
            custom_fields,
            created_at,
            updated_at,
            fr_due_by,
            due_by,
            stats,
            subject,
            type,
            status,
            priority,
            requester,
            ROW_NUMBER() OVER (
                PARTITION BY id
                ORDER BY CAST(updated_at AS TIMESTAMP) DESC, id DESC
            ) AS rn
        FROM cx_cso_raw.fact_cso_tickets
        WHERE (deleted IS NULL OR deleted = false)
        AND source NOT IN (101) --> Remove tickets from old system
    ) t
    WHERE rn = 1
""")
df_latest_tickets.createOrReplaceTempView("v_dedup_tickets")


# Lấy bản ghi SLA mới nhất cho từng ticket (Sửa lỗi to_timestamp sai pattern)
df_latest_sla = spark.sql("""
    SELECT 
        ticket_id,
        priority,
        agent_name,
        resolution_time_in_business_hours,
        agent_reply_count,
        CAST(first_response_date AS TIMESTAMP) AS first_response_date, 
        ticket_type,
        tickets_first_responded_within_sla,
        tickets_resolved_within_sla,
        ttr_time
    FROM (
        SELECT
            ticket_id,
            priority,
            agent_name,
            resolution_time_in_business_hours,
            agent_reply_count,
            CAST(first_response_date AS STRING) AS first_response_date,
            ticket_type,
            tickets_first_responded_within_sla,
            tickets_resolved_within_sla,
            ttr_time,
            ROW_NUMBER() OVER (
                PARTITION BY ticket_id
                ORDER BY crawled_at_ts DESC
            ) AS rn
        FROM cx_cso_raw.fact_cso_tickets_sla
    ) t
    WHERE rn = 1
""")
df_latest_sla.createOrReplaceTempView("v_dedup_sla")


df_ticket_status = spark.sql("""
    SELECT 2 AS status_code, 'Open'                   AS status_name UNION ALL
    SELECT 3 AS status_code, 'Pending'                AS status_name UNION ALL
    SELECT 4 AS status_code, 'Resolved'               AS status_name UNION ALL
    SELECT 5 AS status_code, 'Closed'                 AS status_name UNION ALL
    SELECT 6 AS status_code, 'Waiting for Customer'   AS status_name UNION ALL
    SELECT 7 AS status_code, 'Processing'             AS status_name UNION ALL
    SELECT 8 AS status_code, 'Under Investigation'    AS status_name
""")
df_ticket_status.createOrReplaceTempView("v_dim_ticket_status")

df_company = spark.sql("""
    SELECT
        cso_company_id,
        company_name,
        company_alias,
        company_group,
        company_segement_level1,
        company_segement_level2,
        company_segement_level3
    FROM (
        SELECT
            cso_company_id,
            company_name,
            company_alias,
            company_group,
            company_segement_level1,
            company_segement_level2,
            company_segement_level3,
            ROW_NUMBER() OVER (
                PARTITION BY cso_company_id
                ORDER BY cso_company_id
            ) AS rn
        FROM cx_cso_silver.cso_cx_company
        WHERE cso_company_id IS NOT NULL
    ) t
    WHERE rn = 1
""")
df_company.createOrReplaceTempView("v_company")


# Thực hiện Join Ticket với SLA và Status. 
df_joined = spark.sql("""
    SELECT
        src.*,
        sla.priority AS sla_priority_name,
        st.status_name AS status_name,
        sla.agent_name AS sla_agent_name,
        sla.resolution_time_in_business_hours AS sla_resolution_time_in_business_hours,
        sla.agent_reply_count AS sla_agent_reply_count,
        COALESCE(
            CAST(sla.first_response_date AS TIMESTAMP),
            CAST(src.stats.first_responded_at AS TIMESTAMP)
        ) AS sla_first_response_date,
        sla.ticket_type AS sla_ticket_type,
        sla.tickets_first_responded_within_sla AS sla_tickets_first_responded_within_sla,
        sla.tickets_resolved_within_sla AS sla_tickets_resolved_within_sla,
        sla.ttr_time AS sla_ttr_time,
        c.cso_company_id,
        c.company_name,
        c.company_alias,
        c.company_group,
        c.company_segement_level1,
        c.company_segement_level2,
        c.company_segement_level3
    FROM v_dedup_tickets src
    LEFT JOIN v_dedup_sla sla
        ON CAST(src.id AS STRING) = CAST(sla.ticket_id AS STRING)
    LEFT JOIN v_dim_ticket_status st
        ON CAST(src.status AS INT) = st.status_code
    LEFT JOIN v_company c
        ON src.company_id = c.cso_company_id
""")
df_joined.createOrReplaceTempView("v_joined_data")

df_cso_tickets = spark.sql("""
  SELECT
    /* ==========================================
       1. KEY COLUMNS (PK, NK, FK)
       ========================================== */
    CAST(id AS STRING) AS ticket_id,
    CAST(custom_fields.cf_related_ticket AS STRING) AS related_ticket,
    COALESCE(CAST(company_id AS STRING), 'N/A') AS company_id,

    /* ==========================================
       2. TIMESTAMPS & DATES
       ========================================== */
    CAST(created_at AS TIMESTAMP) AS created_at,
    CAST(updated_at AS TIMESTAMP) AS updated_at,
    CAST(stats.first_responded_at AS TIMESTAMP) AS first_responded_at,
    CAST(stats.resolved_at AS TIMESTAMP) AS resolved_at,
    CAST(stats.closed_at AS TIMESTAMP) AS closed_at,
    CAST(fr_due_by AS TIMESTAMP) AS fr_due_by,
    CAST(due_by AS TIMESTAMP) AS due_by,
    CAST(custom_fields.cf_customer_respond_date AS TIMESTAMP) AS customer_respond_date,
    CAST(custom_fields.cf_imported_ticket_date AS TIMESTAMP) AS imported_ticket_date,
    CAST(custom_fields.cf_imported_ticket_due_date AS TIMESTAMP) AS imported_ticket_due_date,
    CAST(custom_fields.cf_old_due_date AS TIMESTAMP) AS old_due_date,
    created_at AS created_at_full,
    updated_at AS updated_at_full,
    stats.first_responded_at AS first_responded_at_full,
    stats.resolved_at AS resolved_at_full,

    /* ==========================================
       3. KPI, SLA & PERFORMANCE METRICS
       ========================================== */
    CAST(sla_tickets_first_responded_within_sla AS STRING) AS tickets_first_responded_within_sla,
    CAST(sla_tickets_resolved_within_sla AS STRING) AS tickets_resolved_within_sla,
    CAST(sla_first_response_date AS TIMESTAMP) AS first_response_date,
    CAST(sla_agent_reply_count AS INT) AS agent_reply_count,
    CAST(sla_resolution_time_in_business_hours AS INT) AS resolution_time_in_business_hours,
    CAST(sla_ttr_time AS STRING) AS ttr_time_minutes,

    CAST(
        ROUND(
            (unix_timestamp(CAST(stats.resolved_at AS TIMESTAMP)) - unix_timestamp(CAST(created_at AS TIMESTAMP))) / 60.0, 2
        ) AS DECIMAL(27,2)
    ) AS resolution_time_minutes,

    CAST(
        ROUND(
            (unix_timestamp(CAST(sla_first_response_date AS TIMESTAMP)) - unix_timestamp(CAST(created_at AS TIMESTAMP))) / 60.0, 2
        ) AS DECIMAL(27,2)
    ) AS first_response_time_minutes,

    CASE
        WHEN custom_fields.cf__l4_violated = 'Yes' THEN 4
        WHEN custom_fields.cf__l3_violated = 'Yes' THEN 3
        WHEN custom_fields.cf__l2_violated = 'Yes' THEN 2
        WHEN custom_fields.cf__l1_violated = 'Yes' THEN 1
        ELSE 0
    END AS violated_level,
    CASE WHEN custom_fields.cf__l1_violated = 'Yes' THEN true ELSE false END AS l1_violated,
    CASE WHEN custom_fields.cf__l2_violated = 'Yes' THEN true ELSE false END AS l2_violated,
    CASE WHEN custom_fields.cf__l3_violated = 'Yes' THEN true ELSE false END AS l3_violated,
    CASE WHEN custom_fields.cf__l4_violated = 'Yes' THEN true ELSE false END AS l4_violated,

    -- L1 Time Minutes
    CAST(
        CASE
            WHEN custom_fields.cf__l1_time_actual IS NULL
                OR trim(CAST(custom_fields.cf__l1_time_actual AS STRING)) = '' THEN 0.0
            WHEN (
                CASE
                    WHEN substr(trim(CAST(custom_fields.cf__l1_time_actual AS STRING)), 1, 1) = '-' THEN -1.0
                    ELSE 1.0
                END
                * (
                    coalesce(CAST(NULLIF(regexp_extract(CAST(custom_fields.cf__l1_time_actual AS STRING), '([0-9]+)M', 1), '') AS DOUBLE), 0.0) * 43200.0
                    + coalesce(CAST(NULLIF(regexp_extract(CAST(custom_fields.cf__l1_time_actual AS STRING), '([0-9]+)w', 1), '') AS DOUBLE), 0.0) * 10080.0
                    + coalesce(CAST(NULLIF(regexp_extract(CAST(custom_fields.cf__l1_time_actual AS STRING), '([0-9]+)d', 1), '') AS DOUBLE), 0.0) * 1440.0
                    + coalesce(CAST(NULLIF(regexp_extract(CAST(custom_fields.cf__l1_time_actual AS STRING), '([0-9]+)h', 1), '') AS DOUBLE), 0.0) * 60.0
                    + coalesce(CAST(NULLIF(regexp_extract(CAST(custom_fields.cf__l1_time_actual AS STRING), '([0-9]+)m', 1), '') AS DOUBLE), 0.0)
                    + coalesce(CAST(NULLIF(regexp_extract(CAST(custom_fields.cf__l1_time_actual AS STRING), '([0-9]+)s', 1), '') AS DOUBLE), 0.0) / 60.0
                )
            ) < 0 THEN NULL
            ELSE (
                CASE
                    WHEN substr(trim(CAST(custom_fields.cf__l1_time_actual AS STRING)), 1, 1) = '-' THEN -1.0
                    ELSE 1.0
                END
                * (
                    coalesce(CAST(NULLIF(regexp_extract(CAST(custom_fields.cf__l1_time_actual AS STRING), '([0-9]+)M', 1), '') AS DOUBLE), 0.0) * 43200.0
                    + coalesce(CAST(NULLIF(regexp_extract(CAST(custom_fields.cf__l1_time_actual AS STRING), '([0-9]+)w', 1), '') AS DOUBLE), 0.0) * 10080.0
                    + coalesce(CAST(NULLIF(regexp_extract(CAST(custom_fields.cf__l1_time_actual AS STRING), '([0-9]+)d', 1), '') AS DOUBLE), 0.0) * 1440.0
                    + coalesce(CAST(NULLIF(regexp_extract(CAST(custom_fields.cf__l1_time_actual AS STRING), '([0-9]+)h', 1), '') AS DOUBLE), 0.0) * 60.0
                    + coalesce(CAST(NULLIF(regexp_extract(CAST(custom_fields.cf__l1_time_actual AS STRING), '([0-9]+)m', 1), '') AS DOUBLE), 0.0)
                    + coalesce(CAST(NULLIF(regexp_extract(CAST(custom_fields.cf__l1_time_actual AS STRING), '([0-9]+)s', 1), '') AS DOUBLE), 0.0) / 60.0
                )
            )
        END AS DOUBLE
    ) AS l1_time_actual_minutes,
    CAST(
        CASE
            WHEN custom_fields.cf__l1_time_allowed IS NULL OR trim(CAST(custom_fields.cf__l1_time_allowed AS STRING)) = '' THEN 0.0
            ELSE CASE WHEN substr(trim(CAST(custom_fields.cf__l1_time_allowed AS STRING)), 1, 1) = '-' THEN -1.0 ELSE 1.0 END
                * (
                    coalesce(CAST(NULLIF(regexp_extract(CAST(custom_fields.cf__l1_time_allowed AS STRING), '([0-9]+)M', 1), '') AS DOUBLE), 0.0) * 43200.0
                  + coalesce(CAST(NULLIF(regexp_extract(CAST(custom_fields.cf__l1_time_allowed AS STRING), '([0-9]+)w', 1), '') AS DOUBLE), 0.0) * 10080.0
                  + coalesce(CAST(NULLIF(regexp_extract(CAST(custom_fields.cf__l1_time_allowed AS STRING), '([0-9]+)d', 1), '') AS DOUBLE), 0.0) * 1440.0
                  + coalesce(CAST(NULLIF(regexp_extract(CAST(custom_fields.cf__l1_time_allowed AS STRING), '([0-9]+)h', 1), '') AS DOUBLE), 0.0) * 60.0
                  + coalesce(CAST(NULLIF(regexp_extract(CAST(custom_fields.cf__l1_time_allowed AS STRING), '([0-9]+)m', 1), '') AS DOUBLE), 0.0)
                  + coalesce(CAST(NULLIF(regexp_extract(CAST(custom_fields.cf__l1_time_allowed AS STRING), '([0-9]+)s', 1), '') AS DOUBLE), 0.0) / 60.0
                )
        END AS DOUBLE
    ) AS l1_time_allowed_minutes,

    -- L2 Time Minutes
    CAST(
        CASE
            WHEN custom_fields.cf__l2_time_actual IS NULL
                OR trim(CAST(custom_fields.cf__l2_time_actual AS STRING)) = '' THEN 0.0
            WHEN (
                CASE
                    WHEN substr(trim(CAST(custom_fields.cf__l2_time_actual AS STRING)), 1, 1) = '-' THEN -1.0
                    ELSE 1.0
                END
                * (
                    coalesce(CAST(NULLIF(regexp_extract(CAST(custom_fields.cf__l2_time_actual AS STRING), '([0-9]+)M', 1), '') AS DOUBLE), 0.0) * 43200.0
                    + coalesce(CAST(NULLIF(regexp_extract(CAST(custom_fields.cf__l2_time_actual AS STRING), '([0-9]+)w', 1), '') AS DOUBLE), 0.0) * 10080.0
                    + coalesce(CAST(NULLIF(regexp_extract(CAST(custom_fields.cf__l2_time_actual AS STRING), '([0-9]+)d', 1), '') AS DOUBLE), 0.0) * 1440.0
                    + coalesce(CAST(NULLIF(regexp_extract(CAST(custom_fields.cf__l2_time_actual AS STRING), '([0-9]+)h', 1), '') AS DOUBLE), 0.0) * 60.0
                    + coalesce(CAST(NULLIF(regexp_extract(CAST(custom_fields.cf__l2_time_actual AS STRING), '([0-9]+)m', 1), '') AS DOUBLE), 0.0)
                    + coalesce(CAST(NULLIF(regexp_extract(CAST(custom_fields.cf__l2_time_actual AS STRING), '([0-9]+)s', 1), '') AS DOUBLE), 0.0) / 60.0
                )
            ) < 0 THEN NULL
            ELSE (
                CASE
                    WHEN substr(trim(CAST(custom_fields.cf__l2_time_actual AS STRING)), 1, 1) = '-' THEN -1.0
                    ELSE 1.0
                END
                * (
                    coalesce(CAST(NULLIF(regexp_extract(CAST(custom_fields.cf__l2_time_actual AS STRING), '([0-9]+)M', 1), '') AS DOUBLE), 0.0) * 43200.0
                    + coalesce(CAST(NULLIF(regexp_extract(CAST(custom_fields.cf__l2_time_actual AS STRING), '([0-9]+)w', 1), '') AS DOUBLE), 0.0) * 10080.0
                    + coalesce(CAST(NULLIF(regexp_extract(CAST(custom_fields.cf__l2_time_actual AS STRING), '([0-9]+)d', 1), '') AS DOUBLE), 0.0) * 1440.0
                    + coalesce(CAST(NULLIF(regexp_extract(CAST(custom_fields.cf__l2_time_actual AS STRING), '([0-9]+)h', 1), '') AS DOUBLE), 0.0) * 60.0
                    + coalesce(CAST(NULLIF(regexp_extract(CAST(custom_fields.cf__l2_time_actual AS STRING), '([0-9]+)m', 1), '') AS DOUBLE), 0.0)
                    + coalesce(CAST(NULLIF(regexp_extract(CAST(custom_fields.cf__l2_time_actual AS STRING), '([0-9]+)s', 1), '') AS DOUBLE), 0.0) / 60.0
                )
            )
        END AS DOUBLE
    ) AS l2_time_actual_minutes,
    CAST(
        CASE
            WHEN custom_fields.cf__l2_time_allowed IS NULL OR trim(CAST(custom_fields.cf__l2_time_allowed AS STRING)) = '' THEN 0.0
            ELSE CASE WHEN substr(trim(CAST(custom_fields.cf__l2_time_allowed AS STRING)), 1, 1) = '-' THEN -1.0 ELSE 1.0 END
                * (
                    coalesce(CAST(NULLIF(regexp_extract(CAST(custom_fields.cf__l2_time_allowed AS STRING), '([0-9]+)M', 1), '') AS DOUBLE), 0.0) * 43200.0
                  + coalesce(CAST(NULLIF(regexp_extract(CAST(custom_fields.cf__l2_time_allowed AS STRING), '([0-9]+)w', 1), '') AS DOUBLE), 0.0) * 10080.0
                  + coalesce(CAST(NULLIF(regexp_extract(CAST(custom_fields.cf__l2_time_allowed AS STRING), '([0-9]+)d', 1), '') AS DOUBLE), 0.0) * 1440.0
                  + coalesce(CAST(NULLIF(regexp_extract(CAST(custom_fields.cf__l2_time_allowed AS STRING), '([0-9]+)h', 1), '') AS DOUBLE), 0.0) * 60.0
                  + coalesce(CAST(NULLIF(regexp_extract(CAST(custom_fields.cf__l2_time_allowed AS STRING), '([0-9]+)m', 1), '') AS DOUBLE), 0.0)
                  + coalesce(CAST(NULLIF(regexp_extract(CAST(custom_fields.cf__l2_time_allowed AS STRING), '([0-9]+)s', 1), '') AS DOUBLE), 0.0) / 60.0
                )
        END AS DOUBLE
    ) AS l2_time_allowed_minutes,

    -- L3 Time Minutes
    CAST(
        CASE
            WHEN custom_fields.cf__l3_time_actual IS NULL
                OR trim(CAST(custom_fields.cf__l3_time_actual AS STRING)) = '' THEN 0.0
            WHEN (
                CASE WHEN substr(trim(CAST(custom_fields.cf__l3_time_actual AS STRING)), 1, 1) = '-' THEN -1.0 ELSE 1.0 END
                * (
                    coalesce(CAST(NULLIF(regexp_extract(CAST(custom_fields.cf__l3_time_actual AS STRING), '([0-9]+)M', 1), '') AS DOUBLE), 0.0) * 43200.0
                    + coalesce(CAST(NULLIF(regexp_extract(CAST(custom_fields.cf__l3_time_actual AS STRING), '([0-9]+)w', 1), '') AS DOUBLE), 0.0) * 10080.0
                    + coalesce(CAST(NULLIF(regexp_extract(CAST(custom_fields.cf__l3_time_actual AS STRING), '([0-9]+)d', 1), '') AS DOUBLE), 0.0) * 1440.0
                    + coalesce(CAST(NULLIF(regexp_extract(CAST(custom_fields.cf__l3_time_actual AS STRING), '([0-9]+)h', 1), '') AS DOUBLE), 0.0) * 60.0
                    + coalesce(CAST(NULLIF(regexp_extract(CAST(custom_fields.cf__l3_time_actual AS STRING), '([0-9]+)m', 1), '') AS DOUBLE), 0.0)
                    + coalesce(CAST(NULLIF(regexp_extract(CAST(custom_fields.cf__l3_time_actual AS STRING), '([0-9]+)s', 1), '') AS DOUBLE), 0.0) / 60.0
                )
            ) < 0 THEN NULL
            ELSE (
                CASE WHEN substr(trim(CAST(custom_fields.cf__l3_time_actual AS STRING)), 1, 1) = '-' THEN -1.0 ELSE 1.0 END
                * (
                    coalesce(CAST(NULLIF(regexp_extract(CAST(custom_fields.cf__l3_time_actual AS STRING), '([0-9]+)M', 1), '') AS DOUBLE), 0.0) * 43200.0
                    + coalesce(CAST(NULLIF(regexp_extract(CAST(custom_fields.cf__l3_time_actual AS STRING), '([0-9]+)w', 1), '') AS DOUBLE), 0.0) * 10080.0
                    + coalesce(CAST(NULLIF(regexp_extract(CAST(custom_fields.cf__l3_time_actual AS STRING), '([0-9]+)d', 1), '') AS DOUBLE), 0.0) * 1440.0
                    + coalesce(CAST(NULLIF(regexp_extract(CAST(custom_fields.cf__l3_time_actual AS STRING), '([0-9]+)h', 1), '') AS DOUBLE), 0.0) * 60.0
                    + coalesce(CAST(NULLIF(regexp_extract(CAST(custom_fields.cf__l3_time_actual AS STRING), '([0-9]+)m', 1), '') AS DOUBLE), 0.0)
                    + coalesce(CAST(NULLIF(regexp_extract(CAST(custom_fields.cf__l3_time_actual AS STRING), '([0-9]+)s', 1), '') AS DOUBLE), 0.0) / 60.0
                )
            )
        END AS DOUBLE
    ) AS l3_time_actual_minutes,
    CAST(
        CASE
            WHEN custom_fields.cf__l3_time_allowed IS NULL OR trim(CAST(custom_fields.cf__l3_time_allowed AS STRING)) = '' THEN 0.0
            ELSE CASE WHEN substr(trim(CAST(custom_fields.cf__l3_time_allowed AS STRING)), 1, 1) = '-' THEN -1.0 ELSE 1.0 END
                * (
                    coalesce(CAST(NULLIF(regexp_extract(CAST(custom_fields.cf__l3_time_allowed AS STRING), '([0-9]+)M', 1), '') AS DOUBLE), 0.0) * 43200.0
                  + coalesce(CAST(NULLIF(regexp_extract(CAST(custom_fields.cf__l3_time_allowed AS STRING), '([0-9]+)w', 1), '') AS DOUBLE), 0.0) * 10080.0
                  + coalesce(CAST(NULLIF(regexp_extract(CAST(custom_fields.cf__l3_time_allowed AS STRING), '([0-9]+)d', 1), '') AS DOUBLE), 0.0) * 1440.0
                  + coalesce(CAST(NULLIF(regexp_extract(CAST(custom_fields.cf__l3_time_allowed AS STRING), '([0-9]+)h', 1), '') AS DOUBLE), 0.0) * 60.0
                  + coalesce(CAST(NULLIF(regexp_extract(CAST(custom_fields.cf__l3_time_allowed AS STRING), '([0-9]+)m', 1), '') AS DOUBLE), 0.0)
                  + coalesce(CAST(NULLIF(regexp_extract(CAST(custom_fields.cf__l3_time_allowed AS STRING), '([0-9]+)s', 1), '') AS DOUBLE), 0.0) / 60.0
                )
        END AS DOUBLE
    ) AS l3_time_allowed_minutes,

    -- L4 Time Minutes
    CAST(
        CASE
            WHEN custom_fields.cf__l4_time_actual IS NULL OR trim(CAST(custom_fields.cf__l4_time_actual AS STRING)) = '' THEN 0.0
            ELSE CASE WHEN substr(trim(CAST(custom_fields.cf__l4_time_actual AS STRING)), 1, 1) = '-' THEN -1.0 ELSE 1.0 END
                * (
                    coalesce(CAST(NULLIF(regexp_extract(CAST(custom_fields.cf__l4_time_actual AS STRING), '([0-9]+)M', 1), '') AS DOUBLE), 0.0) * 43200.0
                  + coalesce(CAST(NULLIF(regexp_extract(CAST(custom_fields.cf__l4_time_actual AS STRING), '([0-9]+)w', 1), '') AS DOUBLE), 0.0) * 10080.0
                  + coalesce(CAST(NULLIF(regexp_extract(CAST(custom_fields.cf__l4_time_actual AS STRING), '([0-9]+)d', 1), '') AS DOUBLE), 0.0) * 1440.0
                  + coalesce(CAST(NULLIF(regexp_extract(CAST(custom_fields.cf__l4_time_actual AS STRING), '([0-9]+)h', 1), '') AS DOUBLE), 0.0) * 60.0
                  + coalesce(CAST(NULLIF(regexp_extract(CAST(custom_fields.cf__l4_time_actual AS STRING), '([0-9]+)m', 1), '') AS DOUBLE), 0.0)
                  + coalesce(CAST(NULLIF(regexp_extract(CAST(custom_fields.cf__l4_time_actual AS STRING), '([0-9]+)s', 1), '') AS DOUBLE), 0.0) / 60.0
                )
        END AS DOUBLE
    ) AS l4_time_actual_minutes,
    CAST(
        CASE
            WHEN custom_fields.cf__l4_time_allowed IS NULL OR trim(CAST(custom_fields.cf__l4_time_allowed AS STRING)) = '' THEN 0.0
            ELSE CASE WHEN substr(trim(CAST(custom_fields.cf__l4_time_allowed AS STRING)), 1, 1) = '-' THEN -1.0 ELSE 1.0 END
                * (
                    coalesce(CAST(NULLIF(regexp_extract(CAST(custom_fields.cf__l4_time_allowed AS STRING), '([0-9]+)M', 1), '') AS DOUBLE), 0.0) * 43200.0
                  + coalesce(CAST(NULLIF(regexp_extract(CAST(custom_fields.cf__l4_time_allowed AS STRING), '([0-9]+)w', 1), '') AS DOUBLE), 0.0) * 10080.0
                  + coalesce(CAST(NULLIF(regexp_extract(CAST(custom_fields.cf__l4_time_allowed AS STRING), '([0-9]+)d', 1), '') AS DOUBLE), 0.0) * 1440.0
                  + coalesce(CAST(NULLIF(regexp_extract(CAST(custom_fields.cf__l4_time_allowed AS STRING), '([0-9]+)h', 1), '') AS DOUBLE), 0.0) * 60.0
                  + coalesce(CAST(NULLIF(regexp_extract(CAST(custom_fields.cf__l4_time_allowed AS STRING), '([0-9]+)m', 1), '') AS DOUBLE), 0.0)
                  + coalesce(CAST(NULLIF(regexp_extract(CAST(custom_fields.cf__l4_time_allowed AS STRING), '([0-9]+)s', 1), '') AS DOUBLE), 0.0) / 60.0
                )
        END AS DOUBLE
    ) AS l4_time_allowed_minutes,

    -- Time to Response & Overdue
    CAST(
        CASE
            WHEN custom_fields.cf_ttr_time IS NULL OR trim(CAST(custom_fields.cf_ttr_time AS STRING)) = '' THEN 0.0
            ELSE CASE WHEN substr(trim(CAST(custom_fields.cf_ttr_time AS STRING)), 1, 1) = '-' THEN -1.0 ELSE 1.0 END
                * (
                    coalesce(CAST(NULLIF(regexp_extract(CAST(custom_fields.cf_ttr_time AS STRING), '([0-9]+)M', 1), '') AS DOUBLE), 0.0) * 43200.0
                  + coalesce(CAST(NULLIF(regexp_extract(CAST(custom_fields.cf_ttr_time AS STRING), '([0-9]+)w', 1), '') AS DOUBLE), 0.0) * 10080.0
                  + coalesce(CAST(NULLIF(regexp_extract(CAST(custom_fields.cf_ttr_time AS STRING), '([0-9]+)d', 1), '') AS DOUBLE), 0.0) * 1440.0
                  + coalesce(CAST(NULLIF(regexp_extract(CAST(custom_fields.cf_ttr_time AS STRING), '([0-9]+)h', 1), '') AS DOUBLE), 0.0) * 60.0
                  + coalesce(CAST(NULLIF(regexp_extract(CAST(custom_fields.cf_ttr_time AS STRING), '([0-9]+)m', 1), '') AS DOUBLE), 0.0)
                  + coalesce(CAST(NULLIF(regexp_extract(CAST(custom_fields.cf_ttr_time AS STRING), '([0-9]+)s', 1), '') AS DOUBLE), 0.0) / 60.0
                )
        END AS DOUBLE
    ) AS time_to_response_minutes,
    CAST(
        CASE
            WHEN custom_fields.cf_rt_time IS NULL OR trim(CAST(custom_fields.cf_rt_time AS STRING)) = '' THEN 0.0
            ELSE CASE WHEN substr(trim(CAST(custom_fields.cf_rt_time AS STRING)), 1, 1) = '-' THEN -1.0 ELSE 1.0 END
                * (
                    coalesce(CAST(NULLIF(regexp_extract(CAST(custom_fields.cf_rt_time AS STRING), '([0-9]+)M', 1), '') AS DOUBLE), 0.0) * 43200.0
                  + coalesce(CAST(NULLIF(regexp_extract(CAST(custom_fields.cf_rt_time AS STRING), '([0-9]+)w', 1), '') AS DOUBLE), 0.0) * 10080.0
                  + coalesce(CAST(NULLIF(regexp_extract(CAST(custom_fields.cf_rt_time AS STRING), '([0-9]+)d', 1), '') AS DOUBLE), 0.0) * 1440.0
                  + coalesce(CAST(NULLIF(regexp_extract(CAST(custom_fields.cf_rt_time AS STRING), '([0-9]+)h', 1), '') AS DOUBLE), 0.0) * 60.0
                  + coalesce(CAST(NULLIF(regexp_extract(CAST(custom_fields.cf_rt_time AS STRING), '([0-9]+)m', 1), '') AS DOUBLE), 0.0)
                  + coalesce(CAST(NULLIF(regexp_extract(CAST(custom_fields.cf_rt_time AS STRING), '([0-9]+)s', 1), '') AS DOUBLE), 0.0) / 60.0
                )
        END AS DOUBLE
    ) AS response_time_minutes,

    /* ==========================================
       4. CLASSIFICATIONS & METADATA
       ========================================== */
    CAST(subject AS STRING) AS subject,
    COALESCE(company_group, CAST(type AS STRING), 'N/A') AS customer_group,
    COALESCE(CAST(status_name AS STRING), 'Unknown') AS status_name,
    CAST(priority AS STRING) AS priority_level,
    COALESCE(
        NULLIF(trim(CAST(custom_fields.cf__components AS STRING)), ''), 
        NULLIF(trim(CAST(custom_fields.cf_component AS STRING)), ''), 
        '-'
    ) AS issue_category,
    COALESCE(CAST(custom_fields.cf_issues_type AS STRING), 'N/A') AS issues_type,
    COALESCE(company_name, company.name, 'N/A')             AS company_name,
    COALESCE(company_alias, custom_fields.cf_company, 'N/A') AS company_alias,                                                                           
    COALESCE(custom_fields.cf_incident_root_cause, 'N/A') AS incident_root_cause,
    COALESCE(CAST(custom_fields.cf_subtype AS STRING), 'N/A') AS support_category,
    COALESCE(CAST(custom_fields.cf_reason AS STRING), 'N/A') AS reason,
    COALESCE(CAST(custom_fields.cf_spam_type AS STRING), 'N/A') AS spam_type,
    COALESCE(CAST(custom_fields.cf_sentiment AS STRING), 'N/A') AS sentiment,
    COALESCE(CAST(custom_fields.cf_severity AS STRING), 'N/A') AS severity_level,
    COALESCE(CAST(custom_fields.cf_urgency AS STRING), 'N/A') AS urgency_level,
    COALESCE(CAST(custom_fields.cf_ttr_overdue AS STRING), 'N/A') AS ttr_overdue,
    COALESCE(CAST(custom_fields.cf_rt_overdue AS STRING), 'N/A') AS rt_overdue,
    COALESCE(CAST(custom_fields.cf_note AS STRING), '') AS note,

    -- Customer & Segments
    COALESCE(CAST(requester.name AS STRING), 'N/A') AS requester_name,
    COALESCE(company_segement_level1, CAST(custom_fields.cf__segment AS STRING), 'N/A') AS customer_segment_l1,
    COALESCE(company_segement_level2, CAST(custom_fields.cf__segment AS STRING), 'N/A') AS customer_segment_l2,
    COALESCE(company_segement_level3, CAST(custom_fields.cf__segment AS STRING), 'N/A') AS customer_segment_l3,

    COALESCE(CAST(custom_fields.cf_customer_entry_channel AS STRING), 'N/A') AS customer_entry_channel,
    COALESCE(CAST(custom_fields.cf_csat_rating AS STRING), '0') AS customer_satisfaction_rating,
    COALESCE(CAST(custom_fields.cf_hiu_qu_trao_i AS STRING), 'Chưa xác định') AS communication_effectiveness,

    -- Agents & Assignment Stages
    COALESCE(CAST(sla_agent_name AS STRING), 'unknown') AS agent_name,
    CASE 
        WHEN custom_fields.cf_agent_l1 IS NULL OR trim(CAST(custom_fields.cf_agent_l1 AS STRING)) = '' THEN 'Chưa xác định' 
        ELSE CAST(custom_fields.cf_agent_l1 AS STRING) 
    END AS assigned_to,
    CASE
        WHEN custom_fields.cf_l1 = true THEN 'L1_AGENT'
        WHEN custom_fields.cf_l2 = true THEN 'L2_AGENT'
        WHEN custom_fields.cf_l3 = true THEN 'L3_AGENT'
        WHEN custom_fields.cf_l4 = true THEN 'L4_AGENT'
        ELSE 'UNASSIGNED'
    END AS assigned_agent_stage,

    -- Flags & Indicators
    COALESCE(CAST(custom_fields.cf__isvcs AS BOOLEAN), false) AS is_vcs,
    COALESCE(CAST(custom_fields.cf_l1 AS BOOLEAN), false) AS l1,
    COALESCE(CAST(custom_fields.cf_l2 AS BOOLEAN), false) AS l2,
    COALESCE(CAST(custom_fields.cf_l3 AS BOOLEAN), false) AS l3,
    COALESCE(CAST(custom_fields.cf_l4 AS BOOLEAN), false) AS l4,
    CASE WHEN lower(CAST(custom_fields.cf__merge AS STRING)) = 'yes' THEN true ELSE false END AS is_duplicated_ticket,
    COALESCE(CAST(custom_fields.cf__ticket_cx_report AS BOOLEAN), true) AS is_reopened_by_cx,
    COALESCE(CAST(custom_fields.cf_onedaybeforedue AS BOOLEAN), false) AS is_one_day_before_due,
    COALESCE(CAST(custom_fields.cf_onehourbeforedue AS BOOLEAN), false) AS is_one_hour_before_due,
    CAST(custom_fields.cf_reminder_update AS BOOLEAN) AS is_reminder_update,
    CAST(custom_fields.cf_rt_notification AS BOOLEAN) AS is_notification,
    CASE 
        WHEN custom_fields.cf_rt_overdue IS NULL THEN NULL
        WHEN lower(trim(CAST(custom_fields.cf_rt_overdue AS STRING))) = 'yes' THEN true
        ELSE false
    END AS is_overdue,
    
    COALESCE(CAST(custom_fields.cf_thirdfourtime AS BOOLEAN), false) AS is_third_four_time,

    -- Misc / Action Program / Counter
    COALESCE(CAST(custom_fields.cf_chng_trnh_hng_ng AS STRING), 'N/A') AS action_program,
    COALESCE(CAST(custom_fields.cf__call_reminder AS STRING), 'N/A') AS call_reminder,
    COALESCE(CAST(custom_fields.cf_number_of_due_date_changes AS STRING), '0') AS number_of_due_date_changes,
    COALESCE(CAST(custom_fields.cf_number_of_due_date_changes AS STRING), '0') AS due_date_change_count

FROM v_joined_data
""")

# 3) Drop cột không cần (rn + _raw/_full)
cols_to_exclude = ["rn"] + [c for c in df_cso_tickets.columns if c.endswith("_raw") or c.endswith("_full")]
df_final = df_cso_tickets.drop(*cols_to_exclude)

# 4) Write (fresh path)
df_final.repartition(1).write \
    .mode("overwrite") \
    .format("parquet") \
    .save(tgt_path)

# .option("path", tgt_path) \
#     .saveAsTable(tgt_table)
#     .save(tgt_path)

spark.catalog.refreshTable(tgt_table)

# 5) Quick checks
print(tgt_table)
spark.sql("SELECT count(*) AS cnt FROM " + tgt_table).show()

