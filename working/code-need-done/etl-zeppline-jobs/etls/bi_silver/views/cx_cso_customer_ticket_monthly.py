%livy.pyspark

spark.sql("""
CREATE OR REPLACE VIEW bi_silver.cx_cso_customer_ticket_monthly AS
SELECT
    UPPER(product_category) AS product_category,
    UPPER(company_name) AS company_name,
    DATE_TRUNC('month', created_at) AS report_month,

    COUNT(DISTINCT ticket_id) AS ticket_count,

    -- customer-level fields (ANY)
    MAX(UPPER(customer_group)) AS customer_group,
    MAX(UPPER(customer_segment_l1)) AS customer_segment_l1,

    -- workload
    SUM(agent_reply_count) AS total_agent_reply_count,

    -- SLA-lite
    SUM(CASE WHEN is_overdue THEN 1 ELSE 0 END) AS overdue_ticket_count,
    SUM(CASE WHEN violated_level > 0 THEN 1 ELSE 0 END) AS sla_violated_ticket_count,

    -- resolved only (minutes)
    -- AVG(
    --     CASE 
 --             WHEN resolved_at IS NOT NULL 
            -- THEN datediff('minute', created_at, resolved_at) / 60
        -- END
    -- ) AS avg_resolve_time_minutes
    0 as AS avg_resolve_time_minutes


FROM bi_silver.cx_cso_support_tickets
GROUP BY 1, 2, 3
""")