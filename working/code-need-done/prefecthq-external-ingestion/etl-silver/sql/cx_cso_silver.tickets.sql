WITH last_fact_cso_tickets AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY id
                   ORDER BY updated_at_ts DESC, id DESC
               ) rn
        FROM hive.cx_cso_raw.fact_cso_tickets ss
        where deleted is null
    ) t
    WHERE rn = 1
)

SELECT
    -- ===== identifiers =====
    t.id                                   AS ticket_id,
    t.subject,
    t.type as customer_type,
    t.status as status_name,
    t.priority as priority_name,
    t.source,
    t.spam,
--    t.deleted,

    -- ===== company =====
    COALESCE(t.company_id,  t.company.id)                          AS company_id,
    t.company.name                        AS company_name,

    -- ===== requester =====
    coalesce(t.requester_id, t.requester.id)                        AS requester_id,
    t.requester.name                      AS requester_name,
    t.requester.email                     AS requester_email,
    t.requester.phone                     AS requester_phone,
    t.requester.mobile                    AS requester_mobile,

    -- ===== group / agent =====
    t.group_id,
    t.responder_id,

    -- ===== timing =====
    from_unixtime(created_at_ts) AS created_at,
    from_unixtime(updated_at_ts) AS updated_at,

--    t.due_by,
    from_unixtime(t.due_by_ts) AS due_by,
--    t.fr_due_by,
    from_unixtime(t.fr_due_by_ts) AS fr_due_by,
--    t.nr_due_by,
    from_unixtime(t.nr_due_by_ts) AS nr_due_by,

    t.fr_escalated,
    t.nr_escalated,
    t.is_escalated,

    -- ===== stats =====
	from_iso8601_timestamp(t.stats.first_responded_at)      AS first_responded_at,
	from_iso8601_timestamp(t.stats.agent_responded_at)      AS agent_responded_at,
	from_iso8601_timestamp(t.stats.requester_responded_at)  AS requester_responded_at,
	from_iso8601_timestamp(t.stats.pending_since)           AS pending_since_at,
	from_iso8601_timestamp(t.stats.resolved_at)              AS resolved_at,
	from_iso8601_timestamp(t.stats.closed_at)                AS closed_at,
	from_iso8601_timestamp(t.stats.reopened_at)              AS reopened_at,
	from_iso8601_timestamp(t.stats.status_updated_at)        AS status_updated_at,


    -- ===== custom_fields (core) =====
    t.custom_fields.cf__segment                   AS segment,
    t.custom_fields.cf__isvcs                     AS is_vcs,
    t.custom_fields.cf_severity                   AS severity,
    t.custom_fields.cf_urgency                    AS urgency,
    t.custom_fields.cf_sentiment                  AS sentiment,
    t.custom_fields.cf_ai_sentiment               AS ai_sentiment,
    t.custom_fields.cf_product_category           AS product_category,
    t.custom_fields.cf_component                  AS component,
--    t.custom_fields.cf_componentS                 AS cf_components, -- nếu engine phân biệt hoa/thường bỏ dòng này
    t.custom_fields.cf_issues_type                AS issues_type,
    t.custom_fields.cf_incident_root_cause        AS incident_root_cause,
    t.custom_fields.cf_reason                     AS reason,
    t.custom_fields.cf_subtype                    AS subtype,

    -- ===== SLA / TTR =====
--    t.custom_fields.cf__l1_time_actual as l1_time_actual,
    -- L1 actual
CASE
  WHEN t.custom_fields.cf__l1_time_actual IS NULL
    OR trim(t.custom_fields.cf__l1_time_actual) = ''
  THEN NULL
  ELSE
    (CASE WHEN substr(trim(t.custom_fields.cf__l1_time_actual),1,1)='-' THEN -1 ELSE 1 END)
    *
    (
        coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l1_time_actual,'([0-9]+)M',1),'') AS INT),0) * 43200
      + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l1_time_actual,'([0-9]+)w',1),'') AS INT),0) * 10080
      + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l1_time_actual,'([0-9]+)d',1),'') AS INT),0) * 1440
      + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l1_time_actual,'([0-9]+)h',1),'') AS INT),0) * 60
      + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l1_time_actual,'([0-9]+)m',1),'') AS INT),0)
      + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l1_time_actual,'([0-9]+)s',1),'') AS INT),0) / 60
    )
END AS l1_time_actual_minutes,


--    t.custom_fields.cf__l1_time_allowed as l1_time_allowed,
-- L1 allowed
CASE
  WHEN t.custom_fields.cf__l1_time_allowed IS NULL
       OR trim(t.custom_fields.cf__l1_time_allowed) = ''
  THEN NULL
  ELSE
    (CASE
        WHEN substr(trim(t.custom_fields.cf__l1_time_allowed), 1, 1) = '-' THEN -1
        ELSE 1
     END)
    *
    (
        coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l1_time_allowed, '([0-9]+)M', 1), '') AS INT), 0) * 43200
      + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l1_time_allowed, '([0-9]+)w', 1), '') AS INT), 0) * 10080
      + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l1_time_allowed, '([0-9]+)d', 1), '') AS INT), 0) * 1440
      + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l1_time_allowed, '([0-9]+)h', 1), '') AS INT), 0) * 60
      + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l1_time_allowed, '([0-9]+)m', 1), '') AS INT), 0)
      + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l1_time_allowed, '([0-9]+)s', 1), '') AS DOUBLE), 0) / 60
    )
END AS l1_time_allowed_minutes,
    t.custom_fields.cf__l1_violated as l1_violated,
--    t.custom_fields.cf__l2_time_actual as l2_time_actual,
    -- L2 actual
CASE
  WHEN t.custom_fields.cf__l2_time_actual IS NULL
       OR trim(t.custom_fields.cf__l2_time_actual) = ''
  THEN NULL
  ELSE
    (CASE
        WHEN substr(trim(t.custom_fields.cf__l2_time_actual), 1, 1) = '-' THEN -1
        ELSE 1
     END)
    *
    (
        coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l2_time_actual, '([0-9]+)M', 1), '') AS INT), 0) * 43200
      + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l2_time_actual, '([0-9]+)w', 1), '') AS INT), 0) * 10080
      + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l2_time_actual, '([0-9]+)d', 1), '') AS INT), 0) * 1440
      + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l2_time_actual, '([0-9]+)h', 1), '') AS INT), 0) * 60
      + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l2_time_actual, '([0-9]+)m', 1), '') AS INT), 0)
      + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l2_time_actual, '([0-9]+)s', 1), '') AS DOUBLE), 0) / 60.0
    )
END AS l2_time_actual_minutes,
--    t.custom_fields.cf__l2_time_allowed as l2_time_allowed,
-- L2 allowed
CASE
  WHEN t.custom_fields.cf__l2_time_allowed IS NULL
       OR trim(t.custom_fields.cf__l2_time_allowed) = ''
  THEN NULL
  ELSE
    (CASE
        WHEN substr(trim(t.custom_fields.cf__l2_time_allowed), 1, 1) = '-' THEN -1
        ELSE 1
     END)
    *
    (
        coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l2_time_allowed, '([0-9]+)M', 1), '') AS INT), 0) * 43200
      + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l2_time_allowed, '([0-9]+)w', 1), '') AS INT), 0) * 10080
      + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l2_time_allowed, '([0-9]+)d', 1), '') AS INT), 0) * 1440
      + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l2_time_allowed, '([0-9]+)h', 1), '') AS INT), 0) * 60
      + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l2_time_allowed, '([0-9]+)m', 1), '') AS INT), 0)
      + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l2_time_allowed, '([0-9]+)s', 1), '') AS DOUBLE), 0) / 60.0
    )
END AS l2_time_allowed_minutes,
    t.custom_fields.cf__l2_violated as l2_violated,
--    t.custom_fields.cf__l3_time_actual as l3_time_actual,
   CASE
  WHEN t.custom_fields.cf__l3_time_actual IS NULL
       OR trim(t.custom_fields.cf__l3_time_actual) = ''
  THEN NULL
  ELSE
    (CASE
        WHEN substr(trim(t.custom_fields.cf__l3_time_actual), 1, 1) = '-' THEN -1
        ELSE 1
     END)
    *
    (
        coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l3_time_actual, '([0-9]+)M', 1), '') AS INT), 0) * 43200
      + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l3_time_actual, '([0-9]+)w', 1), '') AS INT), 0) * 10080
      + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l3_time_actual, '([0-9]+)d', 1), '') AS INT), 0) * 1440
      + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l3_time_actual, '([0-9]+)h', 1), '') AS INT), 0) * 60
      + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l3_time_actual, '([0-9]+)m', 1), '') AS INT), 0)
      + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l3_time_actual, '([0-9]+)s', 1), '') AS DOUBLE), 0) / 60.0
    )
END AS l3_time_actual_minutes,
--    t.custom_fields.cf__l3_time_allowed as l3_time_allowed,
CASE
  WHEN t.custom_fields.cf__l3_time_allowed IS NULL
       OR trim(t.custom_fields.cf__l3_time_allowed) = ''
  THEN NULL
  ELSE
    (CASE
        WHEN substr(trim(t.custom_fields.cf__l3_time_allowed), 1, 1) = '-' THEN -1
        ELSE 1
     END)
    *
    (
        coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l3_time_allowed, '([0-9]+)M', 1), '') AS INT), 0) * 43200
      + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l3_time_allowed, '([0-9]+)w', 1), '') AS INT), 0) * 10080
      + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l3_time_allowed, '([0-9]+)d', 1), '') AS INT), 0) * 1440
      + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l3_time_allowed, '([0-9]+)h', 1), '') AS INT), 0) * 60
      + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l3_time_allowed, '([0-9]+)m', 1), '') AS INT), 0)
      + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l3_time_allowed, '([0-9]+)s', 1), '') AS DOUBLE), 0) / 60.0
    )
END AS l3_time_allowed_minutes,
    t.custom_fields.cf__l3_violated as l3_violated,
--    t.custom_fields.cf__l4_time_actual as l4_time_actual,
    CASE
  WHEN t.custom_fields.cf__l4_time_actual IS NULL
       OR trim(t.custom_fields.cf__l4_time_actual) = ''
  THEN NULL
  ELSE
    (CASE
        WHEN substr(trim(t.custom_fields.cf__l4_time_actual), 1, 1) = '-' THEN -1
        ELSE 1
     END)
    *
    (
        coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l4_time_actual, '([0-9]+)M', 1), '') AS INT), 0) * 43200
      + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l4_time_actual, '([0-9]+)w', 1), '') AS INT), 0) * 10080
      + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l4_time_actual, '([0-9]+)d', 1), '') AS INT), 0) * 1440
      + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l4_time_actual, '([0-9]+)h', 1), '') AS INT), 0) * 60
      + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l4_time_actual, '([0-9]+)m', 1), '') AS INT), 0)
      + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l4_time_actual, '([0-9]+)s', 1), '') AS DOUBLE), 0) / 60.0
    )
END AS l4_time_actual_minutes,

--    t.custom_fields.cf__l4_time_allowed as l4_time_allowed,
CASE
  WHEN t.custom_fields.cf__l4_time_allowed IS NULL
       OR trim(t.custom_fields.cf__l4_time_allowed) = ''
  THEN NULL
  ELSE
    (CASE
        WHEN substr(trim(t.custom_fields.cf__l4_time_allowed), 1, 1) = '-' THEN -1
        ELSE 1
     END)
    *
    (
        coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l4_time_allowed, '([0-9]+)M', 1), '') AS INT), 0) * 43200
      + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l4_time_allowed, '([0-9]+)w', 1), '') AS INT), 0) * 10080
      + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l4_time_allowed, '([0-9]+)d', 1), '') AS INT), 0) * 1440
      + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l4_time_allowed, '([0-9]+)h', 1), '') AS INT), 0) * 60
      + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l4_time_allowed, '([0-9]+)m', 1), '') AS INT), 0)
      + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l4_time_allowed, '([0-9]+)s', 1), '') AS DOUBLE), 0) / 60.0
    )
END AS l4_time_allowed_minutes,
    t.custom_fields.cf__l4_violated as l4_violated,
    CASE
  WHEN t.custom_fields.cf_ttr_time IS NULL
       OR trim(t.custom_fields.cf_ttr_time) = ''
  THEN NULL
  ELSE
    (CASE
        WHEN substr(trim(t.custom_fields.cf_ttr_time), 1, 1) = '-' THEN -1
        ELSE 1
     END)
    *
    (
        -- 1M = 30 days
        coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf_ttr_time, '([0-9]+)M', 1), '') AS INT), 0) * 43200
      + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf_ttr_time, '([0-9]+)w', 1), '') AS INT), 0) * 10080
      + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf_ttr_time, '([0-9]+)d', 1), '') AS INT), 0) * 1440
      + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf_ttr_time, '([0-9]+)h', 1), '') AS INT), 0) * 60
      + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf_ttr_time, '([0-9]+)m', 1), '') AS INT), 0)
      + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf_ttr_time, '([0-9]+)s', 1), '') AS DOUBLE), 0) / 60.0
    )
END AS ttr_minutes,

--    t.custom_fields.cf_ttr_time as ttr_time,
    t.custom_fields.cf_ttr_overdue as ttr_overdue,

    -- ===== CSAT / CX =====
    t.custom_fields.cf_csat_rating               AS csat_rating,
    t.custom_fields.cf__ticket_cx_report          AS ticket_cx_report,

    -- ===== relationships =====
--    t.custom_fields.cf_related_ticket            AS related_ticket,
    t.custom_fields.cf_link_kb                   AS link_kb,

    -- ===== arrays (giữ nguyên) =====
--    t.tags,
--    t.attachments,
--    t.cc_emails,
--    t.fwd_emails,
--    t.reply_cc_emails,
--    t.ticket_cc_emails,
--    t.ticket_bcc_emails,
--    t.to_emails,
--    t.associated_tickets_list,
--    t.associated_tickets_count,
    t.association_type

FROM last_fact_cso_tickets t;
