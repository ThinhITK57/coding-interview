# # %livy.pyspark

# from pyspark.sql import functions as F

# tgt_table = "cx_cso_silver.cx_cso_support_tickets"
# tgt_path  = "s3a://vcs-silver/cx-cso-silver/cx_cso_support_tickets"

# # 0) Refresh metadata
# spark.sql("REFRESH TABLE cx_cso_raw.fact_cso_tickets")

# spark.catalog.clearCache()
# # spark.sql("DROP TABLE IF EXISTS " + tgt_table)
# # hard_delete_path(tgt_path)

# # 2) Query (giữ cấu trúc, chuẩn hoá alias custom_fields = cf)
# df_cso_tickets = spark.sql("""
#     WITH latest_ticket AS (
#     SELECT
#         CAST(id as STRING)               AS ticket_id,
#         subject,
#         type             AS customer_group,
#         status,
#         priority,
#         created_at       AS created_at_full,
#         updated_at       AS updated_at_full,
#         stats,
#         custom_fields,
#         requester,
#         company,
#         company_id,
#         tags,
#         fr_due_by,
#         due_by,
#         ROW_NUMBER() OVER (
#             PARTITION BY id
#             ORDER BY updated_at DESC
#         ) AS rn
#     FROM cx_cso_raw.fact_cso_tickets
# )
# , base AS (
#     SELECT *
#     FROM latest_ticket
#     WHERE rn = 1
# )
# SELECT
#     ticket_id,
#     subject,
#     COALESCE(customer_group, 'N/A') as customer_group,

#     /* ===== Status ===== */
#     CASE status
#         WHEN 2 THEN 'Open'
#         WHEN 3 THEN 'Pending'
#         WHEN 4 THEN 'Resolved'
#         WHEN 5 THEN 'Closed'
#         WHEN 6 THEN 'Waiting for Customer'
#         WHEN 7 THEN 'Processing'
#         WHEN 8 THEN 'Under Investigation'
#         ELSE 'Unknown'
#     END AS status_name,

#     /* ===== Priority ===== */
#     CASE priority
#         WHEN 1 THEN 'Low'
#         WHEN 2 THEN 'Medium'
#         WHEN 3 THEN 'High'
#         WHEN 4 THEN 'Urgent'
#         ELSE 'Unknown Priority'
#     END AS priority_level,

#     created_at_full,
#     updated_at_full,
#     stats.first_responded_at   AS first_responded_at_full,
#     stats.resolved_at          AS resolved_at_full,

#     CAST(created_at_full as TIMESTAMP)               AS created_at,
#     CAST(updated_at_full  as TIMESTAMP)               AS updated_at,
#     CAST(fr_due_by  as TIMESTAMP)               AS fr_due_by,
#     CAST(due_by  as TIMESTAMP)               AS due_by,
#     CAST(stats.first_responded_at as TIMESTAMP)      AS first_responded_at,
#     CAST(stats.resolved_at  as TIMESTAMP)             AS resolved_at,
#     CAST(stats.closed_at  as TIMESTAMP)               AS closed_at,
#       -- Custom Fields
#                 COALESCE(custom_fields.cf__call_reminder, 'N/A') AS call_reminder,
#                 COALESCE(
#                     NULLIF(trim(custom_fields.cf__components), ''), 
#                     NULLIF(trim(custom_fields.cf_component), ''), 
#                     '-'
#                 ) AS issue_category,       
#                 COALESCE(custom_fields.cf__isvcs, false) AS is_vcs,
#                 COALESCE(custom_fields.cf_l1, false) as l1,
#                 COALESCE(custom_fields.cf_l2, false) AS l2,
#                 COALESCE(custom_fields.cf_l3, false) AS l3,
#                 COALESCE(custom_fields.cf_l4, false) AS l4,
#                 CASE WHEN lower(custom_fields.cf__merge) = 'yes' then true else false end AS is_duplicated_ticket,
#                 -- Xóa segment và market_segment
#                 -- COALESCE(custom_fields.cf__segment, 'N/A') AS segment,
#                 -- COALESCE(custom_fields.cf__segment, 'N/A') AS market_segment,
#                 COALESCE(CAST(custom_fields.cf__ticket_cx_report as BOOLEAN), true) as is_reopened_by_cx,
#                 CASE WHEN custom_fields.cf_agent_l1 is null or custom_fields.cf_agent_l1 = '' THEN 'Chưa xác định' ELSE custom_fields.cf_agent_l1 END AS assigned_to,
#                 COALESCE(custom_fields.cf_chng_trnh_hng_ng, 'N/A') AS action_program,
#                 COALESCE(custom_fields.cf_company, 'N/A') AS company_alias,
#                 COALESCE(custom_fields.cf_csat_rating, 0) as customer_satisfaction_rating,
#                 COALESCE(custom_fields.cf_customer_entry_channel, 'N/A') AS customer_entry_channel,
#                 CAST(custom_fields.cf_customer_respond_date as TIMESTAMP) AS customer_respond_date,
#                 COALESCE(custom_fields.cf_hiu_qu_trao_i, 'Chưa xác định') AS communication_effectiveness,
#                 CAST(custom_fields.cf_imported_ticket_date as TIMESTAMP) AS imported_ticket_date,
#                 CAST(COALESCE(custom_fields.cf_imported_ticket_due_date, due_by) as TIMESTAMP) AS imported_ticket_due_date,
#                 COALESCE(custom_fields.cf_incident_root_cause, 'N/A') AS incident_root_cause,
#                 COALESCE(custom_fields.cf_issues_type, 'N/A') AS issues_type,
#                 COALESCE(custom_fields.cf_note, '') AS note,
#                 COALESCE(custom_fields.cf_number_of_due_date_changes, 0) AS number_of_due_date_changes,
#                 CAST(custom_fields.cf_old_due_date as TIMESTAMP) AS old_due_date,
#                 COALESCE(custom_fields.cf_onedaybeforedue, false) AS is_one_day_before_due,
#                 COALESCE(custom_fields.cf_onehourbeforedue, false) AS is_one_hour_before_due,
#                 COALESCE(custom_fields.cf_reason, 'N/A') AS reason,
#                 custom_fields.cf_related_ticket AS related_ticket,
#                 custom_fields.cf_reminder_update AS is_reminder_update,
#                 custom_fields.cf_rt_notification AS is_notification,
#                 CASE WHEN lower(custom_fields.cf_rt_overdue) = 'yes' THEN true ELSE false END  AS is_overdue,
#                 -- custom_fields.cf_rt_time as rt_time,
                
#                 CASE
#   WHEN custom_fields.cf_rt_time IS NULL
#     OR trim(custom_fields.cf_rt_time) = ''
#   THEN 0
#   ELSE
#     (CASE WHEN substr(trim(custom_fields.cf_rt_time),1,1)='-' THEN -1 ELSE 1 END)
#     *
#     (
#         coalesce(CAST(NULLIF(regexp_extract(custom_fields.cf_rt_time,'([0-9]+)M',1),'') AS INT),0) * 43200
#       + coalesce(CAST(NULLIF(regexp_extract(custom_fields.cf_rt_time,'([0-9]+)w',1),'') AS INT),0) * 10080
#       + coalesce(CAST(NULLIF(regexp_extract(custom_fields.cf_rt_time,'([0-9]+)d',1),'') AS INT),0) * 1440
#       + coalesce(CAST(NULLIF(regexp_extract(custom_fields.cf_rt_time,'([0-9]+)h',1),'') AS INT),0) * 60
#       + coalesce(CAST(NULLIF(regexp_extract(custom_fields.cf_rt_time,'([0-9]+)m',1),'') AS INT),0)
#       + coalesce(CAST(NULLIF(regexp_extract(custom_fields.cf_rt_time,'([0-9]+)s',1),'') AS INT),0) / 60
#     )
    
# END AS response_time_minutes,

#                 COALESCE(custom_fields.cf_sentiment, 'N/A') AS sentiment,
#                 COALESCE(custom_fields.cf_severity, 'N/A') AS severity_level,
#                 COALESCE(custom_fields.cf_spam_type, 'N/A') AS spam_type,
#                 COALESCE(custom_fields.cf_subtype, 'N/A') AS support_category,
#                 COALESCE(custom_fields.cf_thirdfourtime, false) as is_third_four_time,
#                 COALESCE(custom_fields.cf_ttr_overdue, 'N/A') AS ttr_overdue,
#                 COALESCE(custom_fields.cf_urgency, 'N/A') AS urgency_level,
     
#     /* ===== SLA ===== */
#     COALESCE(ROUND(
#         (UNIX_TIMESTAMP(CAST(stats.resolved_at  as TIMESTAMP)) - UNIX_TIMESTAMP(CAST(created_at_full as TIMESTAMP))) / 60,
#         2
#     ) , 0) AS resolution_time_minutes,

#     COALESCE(ROUND(
#         (UNIX_TIMESTAMP(CAST(stats.first_responded_at as TIMESTAMP)) - UNIX_TIMESTAMP(CAST(created_at_full as TIMESTAMP))) / 60,
#         2
#     ) , 0)AS first_response_time_minutes,

#    --    t.custom_fields.cf__l1_time_actual as l1_time_actual,
#     -- L1 actual
# CASE
#   WHEN t.custom_fields.cf__l1_time_actual IS NULL
#     OR trim(t.custom_fields.cf__l1_time_actual) = ''
#   THEN 0
#   ELSE
#     (CASE WHEN substr(trim(t.custom_fields.cf__l1_time_actual),1,1)='-' THEN -1 ELSE 1 END)
#     *
#     (
#         coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l1_time_actual,'([0-9]+)M',1),'') AS INT),0) * 43200
#       + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l1_time_actual,'([0-9]+)w',1),'') AS INT),0) * 10080
#       + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l1_time_actual,'([0-9]+)d',1),'') AS INT),0) * 1440
#       + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l1_time_actual,'([0-9]+)h',1),'') AS INT),0) * 60
#       + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l1_time_actual,'([0-9]+)m',1),'') AS INT),0)
#       + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l1_time_actual,'([0-9]+)s',1),'') AS INT),0) / 60
#     )
    
# END AS l1_time_actual_minutes,


# --    t.custom_fields.cf__l1_time_allowed as l1_time_allowed,
# -- L1 allowed
# CASE
#   WHEN t.custom_fields.cf__l1_time_allowed IS NULL
#        OR trim(t.custom_fields.cf__l1_time_allowed) = ''
#   THEN 0
#   ELSE
#     (CASE
#         WHEN substr(trim(t.custom_fields.cf__l1_time_allowed), 1, 1) = '-' THEN -1
#         ELSE 1
#      END)
#     *
#     (
#         coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l1_time_allowed, '([0-9]+)M', 1), '') AS INT), 0) * 43200
#       + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l1_time_allowed, '([0-9]+)w', 1), '') AS INT), 0) * 10080
#       + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l1_time_allowed, '([0-9]+)d', 1), '') AS INT), 0) * 1440
#       + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l1_time_allowed, '([0-9]+)h', 1), '') AS INT), 0) * 60
#       + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l1_time_allowed, '([0-9]+)m', 1), '') AS INT), 0)
#       + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l1_time_allowed, '([0-9]+)s', 1), '') AS DOUBLE), 0) / 60
#     )
# END AS l1_time_allowed_minutes,
#     CASE WHEN t.custom_fields.cf__l1_violated = 'Yes' THEN true ELSE false END as l1_violated,
# --    t.custom_fields.cf__l2_time_actual as l2_time_actual,
#     -- L2 actual
# CASE
#   WHEN t.custom_fields.cf__l2_time_actual IS NULL
#        OR trim(t.custom_fields.cf__l2_time_actual) = ''
#   THEN 0
#   ELSE
#     (CASE
#         WHEN substr(trim(t.custom_fields.cf__l2_time_actual), 1, 1) = '-' THEN -1
#         ELSE 1
#      END)
#     *
#     (
#         coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l2_time_actual, '([0-9]+)M', 1), '') AS INT), 0) * 43200
#       + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l2_time_actual, '([0-9]+)w', 1), '') AS INT), 0) * 10080
#       + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l2_time_actual, '([0-9]+)d', 1), '') AS INT), 0) * 1440
#       + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l2_time_actual, '([0-9]+)h', 1), '') AS INT), 0) * 60
#       + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l2_time_actual, '([0-9]+)m', 1), '') AS INT), 0)
#       + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l2_time_actual, '([0-9]+)s', 1), '') AS DOUBLE), 0) / 60.0
#     )
# END AS l2_time_actual_minutes,
# --    t.custom_fields.cf__l2_time_allowed as l2_time_allowed,
# -- L2 allowed
# CASE
#   WHEN t.custom_fields.cf__l2_time_allowed IS NULL
#        OR trim(t.custom_fields.cf__l2_time_allowed) = ''
#   THEN 0
#   ELSE
#     (CASE
#         WHEN substr(trim(t.custom_fields.cf__l2_time_allowed), 1, 1) = '-' THEN -1
#         ELSE 1
#      END)
#     *
#     (
#         coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l2_time_allowed, '([0-9]+)M', 1), '') AS INT), 0) * 43200
#       + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l2_time_allowed, '([0-9]+)w', 1), '') AS INT), 0) * 10080
#       + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l2_time_allowed, '([0-9]+)d', 1), '') AS INT), 0) * 1440
#       + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l2_time_allowed, '([0-9]+)h', 1), '') AS INT), 0) * 60
#       + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l2_time_allowed, '([0-9]+)m', 1), '') AS INT), 0)
#       + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l2_time_allowed, '([0-9]+)s', 1), '') AS DOUBLE), 0) / 60.0
#     )
# END AS l2_time_allowed_minutes,
#     CASE WHEN t.custom_fields.cf__l2_violated = 'Yes' THEN true ELSE false END as l2_violated,
# --    t.custom_fields.cf__l3_time_actual as l3_time_actual,
#    CASE
#   WHEN t.custom_fields.cf__l3_time_actual IS NULL
#        OR trim(t.custom_fields.cf__l3_time_actual) = ''
#   THEN 0
#   ELSE
#     (CASE
#         WHEN substr(trim(t.custom_fields.cf__l3_time_actual), 1, 1) = '-' THEN -1
#         ELSE 1
#      END)
#     *
#     (
#         coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l3_time_actual, '([0-9]+)M', 1), '') AS INT), 0) * 43200
#       + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l3_time_actual, '([0-9]+)w', 1), '') AS INT), 0) * 10080
#       + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l3_time_actual, '([0-9]+)d', 1), '') AS INT), 0) * 1440
#       + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l3_time_actual, '([0-9]+)h', 1), '') AS INT), 0) * 60
#       + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l3_time_actual, '([0-9]+)m', 1), '') AS INT), 0)
#       + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l3_time_actual, '([0-9]+)s', 1), '') AS DOUBLE), 0) / 60.0
#     )
# END AS l3_time_actual_minutes,
# --    t.custom_fields.cf__l3_time_allowed as l3_time_allowed,
# CASE
#   WHEN t.custom_fields.cf__l3_time_allowed IS NULL
#        OR trim(t.custom_fields.cf__l3_time_allowed) = ''
#   THEN 0
#   ELSE
#     (CASE
#         WHEN substr(trim(t.custom_fields.cf__l3_time_allowed), 1, 1) = '-' THEN -1
#         ELSE 1
#      END)
#     *
#     (
#         coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l3_time_allowed, '([0-9]+)M', 1), '') AS INT), 0) * 43200
#       + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l3_time_allowed, '([0-9]+)w', 1), '') AS INT), 0) * 10080
#       + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l3_time_allowed, '([0-9]+)d', 1), '') AS INT), 0) * 1440
#       + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l3_time_allowed, '([0-9]+)h', 1), '') AS INT), 0) * 60
#       + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l3_time_allowed, '([0-9]+)m', 1), '') AS INT), 0)
#       + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l3_time_allowed, '([0-9]+)s', 1), '') AS DOUBLE), 0) / 60.0
#     )
# END AS l3_time_allowed_minutes,
#     CASE WHEN t.custom_fields.cf__l3_violated = 'Yes' THEN true ELSE false END as l3_violated,
# --    t.custom_fields.cf__l4_time_actual as l4_time_actual,
#     CASE
#   WHEN t.custom_fields.cf__l4_time_actual IS NULL
#        OR trim(t.custom_fields.cf__l4_time_actual) = ''
#   THEN 0
#   ELSE
#     (CASE
#         WHEN substr(trim(t.custom_fields.cf__l4_time_actual), 1, 1) = '-' THEN -1
#         ELSE 1
#      END)
#     *
#     (
#         coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l4_time_actual, '([0-9]+)M', 1), '') AS INT), 0) * 43200
#       + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l4_time_actual, '([0-9]+)w', 1), '') AS INT), 0) * 10080
#       + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l4_time_actual, '([0-9]+)d', 1), '') AS INT), 0) * 1440
#       + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l4_time_actual, '([0-9]+)h', 1), '') AS INT), 0) * 60
#       + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l4_time_actual, '([0-9]+)m', 1), '') AS INT), 0)
#       + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l4_time_actual, '([0-9]+)s', 1), '') AS DOUBLE), 0) / 60.0
#     )
# END AS l4_time_actual_minutes,

# --    t.custom_fields.cf__l4_time_allowed as l4_time_allowed,
# CASE
#   WHEN t.custom_fields.cf__l4_time_allowed IS NULL
#        OR trim(t.custom_fields.cf__l4_time_allowed) = ''
#   THEN 0
#   ELSE
#     (CASE
#         WHEN substr(trim(t.custom_fields.cf__l4_time_allowed), 1, 1) = '-' THEN -1
#         ELSE 1
#      END)
#     *
#     (
#         coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l4_time_allowed, '([0-9]+)M', 1), '') AS INT), 0) * 43200
#       + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l4_time_allowed, '([0-9]+)w', 1), '') AS INT), 0) * 10080
#       + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l4_time_allowed, '([0-9]+)d', 1), '') AS INT), 0) * 1440
#       + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l4_time_allowed, '([0-9]+)h', 1), '') AS INT), 0) * 60
#       + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l4_time_allowed, '([0-9]+)m', 1), '') AS INT), 0)
#       + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf__l4_time_allowed, '([0-9]+)s', 1), '') AS DOUBLE), 0) / 60.0
#     )
# END AS l4_time_allowed_minutes,
#     CASE WHEN t.custom_fields.cf__l4_violated = 'Yes' THEN true ELSE false END as l4_violated,
#     CASE
#   WHEN t.custom_fields.cf_ttr_time IS NULL
#        OR trim(t.custom_fields.cf_ttr_time) = ''
#   THEN 0
#   ELSE
#     (CASE
#         WHEN substr(trim(t.custom_fields.cf_ttr_time), 1, 1) = '-' THEN -1
#         ELSE 1
#      END)
#     *
#     (
#         -- 1M = 30 days
#         coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf_ttr_time, '([0-9]+)M', 1), '') AS INT), 0) * 43200
#       + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf_ttr_time, '([0-9]+)w', 1), '') AS INT), 0) * 10080
#       + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf_ttr_time, '([0-9]+)d', 1), '') AS INT), 0) * 1440
#       + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf_ttr_time, '([0-9]+)h', 1), '') AS INT), 0) * 60
#       + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf_ttr_time, '([0-9]+)m', 1), '') AS INT), 0)
#       + coalesce(CAST(NULLIF(regexp_extract(t.custom_fields.cf_ttr_time, '([0-9]+)s', 1), '') AS DOUBLE), 0) / 60.0
#     )
# END AS time_to_response_minutes,

#  case
#    	when custom_fields.cf_l1=true then 'L1_AGENT'
#    	when custom_fields.cf_l2=true then 'L2_AGENT'
#    	when custom_fields.cf_l3=true then 'L3_AGENT'
#    	when custom_fields.cf_l4=true then 'L4_AGENT'
#    else 'UNASSIGNED'
#    end as assigned_agent_stage,
                           
#   case
# 	when custom_fields.cf__l4_violated=true then 4
# 	when custom_fields.cf__l3_violated=true then 3
# 	when custom_fields.cf__l2_violated=true then 2
#    	when custom_fields.cf__l1_violated=true then 1
#   else 0
#   end as violated_level ,--- xác minh lại kịch bản tiếp nhận L1-2-3-4
  

#     /* ===== metadata ===== */
#     COALESCE(requester.name, 'N/A')                 AS requester_name,
#     COALESCE(company.name, 'N/A')                   AS company_name,
                           
#      /* ===== thêm segment từ bảng company ===== */
#     COALESCE(c.customer_segment_l1, 'Unkown') AS customer_segment_l1,
#     COALESCE(c.customer_segment_l2, '-') AS customer_segment_l2,
#     COALESCE(c.customer_segment_l3, '-') AS customer_segment_l3,
                           
#     -- concat_ws('; ', tags)          AS tags,
#     /* ===== bổ sung KPI / SLA ===== */
#     CASE
#         WHEN custom_fields.cf_agent_l1 IS NULL
#           OR trim(custom_fields.cf_agent_l1) = ''
#         THEN 'unknown'
#         ELSE custom_fields.cf_agent_l1
#     END AS agent_name,

#     CAST(
#         get_json_object(custom_fields.cf_timer_history, '$.ttr.tu') AS DOUBLE
#     ) / 3600000 AS resolution_time_in_business_hours,

#     CASE
#         WHEN stats.agent_responded_at IS NOT NULL THEN 1
#         ELSE 0
#     END AS agent_reply_count,

#     CAST(stats.first_responded_at AS TIMESTAMP) AS first_response_date,
#     COALESCE(custom_fields.cf_number_of_due_date_changes, 0) 
#     AS due_date_change_count,

#     CASE
#         WHEN stats.first_responded_at IS NOT NULL
#         AND CAST(stats.first_responded_at AS TIMESTAMP)
#             <= CAST(fr_due_by AS TIMESTAMP)
#         THEN 'Non Violated'
#         WHEN stats.first_responded_at IS NULL AND current_timestamp < CAST(fr_due_by AS TIMESTAMP) THEN 'Pending'
#         ELSE 'Violated'
#     END AS tickets_first_responded_within_sla,

#     CASE
#         WHEN stats.resolved_at IS NOT NULL
#         AND CAST(stats.resolved_at AS TIMESTAMP)
#             <= CAST(due_by AS TIMESTAMP)
#         THEN 'Non Violated'
#         WHEN stats.resolved_at IS NULL AND current_timestamp < CAST(due_by AS TIMESTAMP) THEN 'Pending'
#         ELSE 'Violated'
#     END AS tickets_resolved_within_sla,

#     CASE
#       WHEN custom_fields.cf_ttr_time IS NULL
#         OR trim(custom_fields.cf_ttr_time) = ''
#       THEN 0
#       ELSE
#         (CASE WHEN substr(trim(custom_fields.cf_ttr_time), 1, 1) = '-' THEN -1 ELSE 1 END)
#         *
#         (
#             coalesce(CAST(NULLIF(regexp_extract(custom_fields.cf_ttr_time, '([0-9]+)M', 1), '') AS INT), 0) * 43200
#           + coalesce(CAST(NULLIF(regexp_extract(custom_fields.cf_ttr_time, '([0-9]+)w', 1), '') AS INT), 0) * 10080
#           + coalesce(CAST(NULLIF(regexp_extract(custom_fields.cf_ttr_time, '([0-9]+)d', 1), '') AS INT), 0) * 1440
#           + coalesce(CAST(NULLIF(regexp_extract(custom_fields.cf_ttr_time, '([0-9]+)h', 1), '') AS INT), 0) * 60
#           + coalesce(CAST(NULLIF(regexp_extract(custom_fields.cf_ttr_time, '([0-9]+)m', 1), '') AS INT), 0)
#           + coalesce(CAST(NULLIF(regexp_extract(custom_fields.cf_ttr_time, '([0-9]+)s', 1), '') AS DOUBLE), 0) / 60.0
#         )
#     END AS ttr_time_minutes
# FROM base t
# LEFT JOIN cx_cso_raw.cx_company c
#         ON CAST(t.company_id AS STRING) = CAST(c.id AS STRING)

# """)

# # 3) Drop cột không cần (rn + _raw/_full)
# cols_to_exclude = ["rn"] + [c for c in df_cso_tickets.columns if c.endswith("_raw") or c.endswith("_full")]
# df_final = df_cso_tickets.drop(*cols_to_exclude)

# # 4) Write (fresh path)
# df_final.repartition(1).write \
#     .mode("overwrite") \
#     .format("parquet") \
#     .option("path", tgt_path) \
#     .saveAsTable(tgt_table)

# spark.catalog.refreshTable(tgt_table)

# # 5) Quick checks
# print(tgt_table)
# spark.sql("SELECT count(*) AS cnt FROM " + tgt_table).show()

