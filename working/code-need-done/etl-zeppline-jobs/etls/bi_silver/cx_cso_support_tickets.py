%livy.pyspark

from pyspark.sql import functions as F

tgt_table = "bi_silver.cx_cso_support_tickets"
tgt_path  = "/opt/datasets/crawlers/vcs_silver/bi_silver/data/cx_cso_support_tickets"

# 0) Refresh metadata
spark.sql("REFRESH TABLE cx_cso_silver.cx_cso_support_tickets")

spark.catalog.clearCache()
# spark.sql("DROP TABLE IF EXISTS " + tgt_table)
# hard_delete_path(tgt_path)

# 2) Query đơn giản hóa logic, lấy từ silver và ghép nối porduct_category


df_cso_tickets = spark.sql("""
  WITH product_mapping AS (
    SELECT * FROM (
        VALUES
            ('SIEM','VCS002',"Product",'VCS-CyM'),
            ('EDR','VCS004',"Product",'VCS-aJiant'),
            ('Cloudrity','VCS013',"Service",'Cloudrity'),
            ('Threat Intelligence','VCS017',"Service",'VCS-TI'),
            ('SOAR','VCS003',"Product",'VCS-CyCir'),
            ('NDR','VCS005',"Product",'VCS-NDR'),
            ('SOC247','VCS001',"Service",'MSS'),
            ('NAC','VCS011',"Product",'VCS-NAC'),
            ('LIG','VCS020',"Product",'LIG'),
            ('Msuite','VCS015',"Product",'M-Suite'),
            ('AMA','VCS008',"Product",'VCS-AMA'),
            ('SOC Platform','VCS012',"Product",'SOC Platform'),
            ('NSM/NetAD','VCS006',"Product",'VCS-NSM'),
            ('AntiDDos','VCS014',"Product",'AntiDDos'),
            ('WSG','VCS010',"Product",'VCS-WSG'),
            ('F2DR','VCS016',"Product",'VCS-F2DR'),
            ('SE',NULL,"Product",'SE'),
            ('Others','VCS038',"Service",'Khác'),
            ('Pentest','VCS024',"Product",'VAPT (Pentest)'),
            ('INT','VCS018',"Product",'VCS-InT'),
            ('V2S','VCS035',"Product",'VCS-V2S'),
            ('Content','VCS039',"Product",'Content Security'),
            ('TKCG',NULL,"Service",'TKCG'),
            ('SIRC',NULL,"Service",'SIRC'),
            ('BRG','VCS022',"Product",'BRG'),
            ('ESG','VCS009',"Product",'VCS-ESG'),
            ('My Safe',NULL,"Product",'My Safe'),
            ('Asset Management','VCS038',"Service",'SI'),
            ('KIAN','VCS007',"Product",'VCS-KIAN'),
            ('Data Security','VCS038',"Product",'SI'),
            ('TAD','VCS023',"Product",'TAD'),
            ('SDM',NULL,"Product",'SDM'),
            ('TI Content','VCS017',"Product",'VCS-TI')
    ) AS t(issue_category, product_category_code,product_item_type, product_category)
)

SELECT DISTINCT
    t.ticket_id,
    t.subject,
    t.customer_group,
    t.status_name,
    t.priority_level,
    TO_TIMESTAMP(TO_DATE(t.created_at)) as created_at,
    TO_TIMESTAMP(TO_DATE(t.updated_at)) as updated_at,
    TO_TIMESTAMP(TO_DATE(t.first_responded_at)) as first_responded_at,
    t.fr_due_by,
    t.due_by,
    TO_TIMESTAMP(t.resolved_at) as resolved_at,
    TO_TIMESTAMP(t.closed_at) as closed_at,
    t.call_reminder,
    t.issue_category,
    t.is_vcs,
    t.l1,
    t.l2,
    t.l3,
    t.l4,
    t.is_duplicated_ticket,
    -- t.segment,
    -- t.market_segment,
    t.is_reopened_by_cx,
    t.assigned_to,
    t.action_program,
    t.company_alias,
    t.customer_satisfaction_rating,
    t.customer_entry_channel,
    TO_TIMESTAMP(TO_DATE(t.customer_respond_date)) as customer_respond_date,
    t.communication_effectiveness,
    TO_TIMESTAMP(TO_DATE(t.imported_ticket_date)) as imported_ticket_date,
    TO_TIMESTAMP(TO_DATE(t.imported_ticket_due_date)) as imported_ticket_due_date,
    t.incident_root_cause,
    t.issues_type,
    t.note,
    t.number_of_due_date_changes,
    TO_TIMESTAMP(TO_DATE(t.old_due_date)) as old_due_date,
    t.is_one_day_before_due,
    t.is_one_hour_before_due,
    t.reason,
    t.related_ticket,
    t.is_reminder_update,
    t.is_notification,
    t.is_overdue,
    t.response_time_minutes,
    t.sentiment,
    t.severity_level,
    t.spam_type,
    t.support_category,
    t.is_third_four_time,
    t.ttr_overdue,
    t.rt_overdue,
    t.urgency_level,
    t.resolution_time_minutes,
    t.first_response_time_minutes,
    t.l1_time_actual_minutes,
    t.l1_time_allowed_minutes,
    t.l1_violated,
    t.l2_time_actual_minutes,
    t.l2_time_allowed_minutes,
    t.l2_violated,
    t.l3_time_actual_minutes,
    t.l3_time_allowed_minutes,
    t.l3_violated,
    t.l4_time_actual_minutes,
    t.l4_time_allowed_minutes,
    t.l4_violated,
    t.time_to_response_minutes,
    t.assigned_agent_stage,
    t.violated_level,
    t.requester_name,
    t.company_name,
    t.agent_name,
    t.resolution_time_in_business_hours,
    t.agent_reply_count,
    TO_TIMESTAMP(TO_DATE(t.first_response_date)) as first_response_date,
    t.due_date_change_count,
    t.tickets_first_responded_within_sla,
    t.tickets_resolved_within_sla,
    t.ttr_time_minutes,
    t.customer_segment_l1,
    t.customer_segment_l2,
    t.customer_segment_l3,    
                                              
     -- NEW COLUMNS
    dayofweek(t.created_at) AS day_of_week,
    CASE
        WHEN dayofweek(t.created_at) IN (1, 7) THEN 'WEEKEND'
        ELSE 'WEEKDAY'
    END AS day_type,             

    -- mapping
    COALESCE(pm.product_category_code, '-') AS product_category_code,
    COALESCE(pm.product_category, '-') AS product_category,
    COALESCE(pm.product_item_type, '-') AS product_item_type


  FROM cx_cso_silver.cx_cso_support_tickets t
  LEFT JOIN product_mapping pm
      ON trim(lower(t.issue_category)) = trim(lower(pm.issue_category))

""")

# 3) Drop cột không cần (rn + _raw/_full)
cols_to_exclude = ["rn"] + [c for c in df_cso_tickets.columns if c.endswith("_raw") or c.endswith("_full")]
df_final = df_cso_tickets.drop(*cols_to_exclude)

# 4) Write (fresh path)
df_final.repartition(1).write \
    .mode("overwrite") \
    .format("parquet") \
    .option("path", tgt_path) \
    .saveAsTable(tgt_table)

spark.catalog.refreshTable(tgt_table)

# 5) Quick checks
print(tgt_table)
spark.sql("SELECT count(*) AS cnt FROM " + tgt_table).show()

