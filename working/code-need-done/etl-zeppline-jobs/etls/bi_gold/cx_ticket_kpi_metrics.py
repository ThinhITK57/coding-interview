%livy.pyspark

target_table = "bi_gold.cx_ticket_kpi_metrics"
target_path  = "/opt/datasets/crawlers/vcs_gold/bi_gold/data/cx_ticket_kpi_metrics"

# spark.sql("DROP TABLE IF EXISTS " + target_table)

sql_query = """
WITH base AS (
  SELECT
    CAST(closed_at AS timestamp) AS closed_ts,
    CAST(company_name AS STRING) AS company_name,
    CAST(category AS STRING)     AS product_category,
    CAST(ticket_csat AS DOUBLE)  AS ticket_csat,
    COALESCE(CAST(customer_type AS STRING), 'ENTERPRISE') AS customer_type
  FROM bi_silver.cx_cso_tickets
  WHERE closed_at IS NOT NULL
),

periodized AS (
  -- month
  SELECT 'month' AS period_type,
         trunc(closed_ts, 'MM') AS period_start,
         company_name, product_category, ticket_csat, customer_type
  FROM base
  UNION ALL
  -- quarter (yyyy-((q-1)*3+1)-01)
  SELECT 'quarter' AS period_type,
         to_date(concat(
           CAST(year(closed_ts) AS STRING), '-',
           lpad(CAST(((quarter(closed_ts)-1)*3 + 1) AS STRING), 2, '0'),
           '-01'
         )) AS period_start,
         company_name, product_category, ticket_csat, customer_type
  FROM base
  UNION ALL
  -- year
  SELECT 'year' AS period_type,
         trunc(closed_ts, 'YY') AS period_start,
         company_name, product_category, ticket_csat, customer_type
  FROM base
),

t_agg AS (
  SELECT
    period_type,
    period_start,
    customer_type,
    CAST(period_start AS DATE) AS period_start_date,
    company_name,
    product_category,
    SUM(CASE WHEN ticket_csat IS NOT NULL THEN ticket_csat ELSE 0 END) AS ticket_csat_sum_score,
    SUM(CASE WHEN ticket_csat IS NOT NULL THEN 1 ELSE 0 END)           AS ticket_csat_cnt,
    SUM(CASE WHEN ticket_csat >= 4 THEN 1 ELSE 0 END)                  AS ticket_csat_topbox_cnt,
    COUNT(*) AS tickets_closed
  FROM periodized
  GROUP BY
    period_type, period_start, CAST(period_start AS DATE),
    customer_type, company_name, product_category
),

active AS (
  SELECT DISTINCT period_type, period_start, company_name
  FROM periodized
),

active_cnt AS (
  SELECT period_type, period_start, COUNT(*) AS customers_active
  FROM active
  GROUP BY period_type, period_start
),

retained AS (
  SELECT
    curr.period_type,
    curr.period_start,
    COUNT(*) AS retained_customers
  FROM active curr
  JOIN active prev
    ON curr.period_type = prev.period_type
   AND curr.company_name = prev.company_name
   AND prev.period_start =
        CASE
          WHEN curr.period_type = 'month'   THEN add_months(curr.period_start, -1)
          WHEN curr.period_type = 'quarter' THEN add_months(curr.period_start, -3)
          WHEN curr.period_type = 'year'    THEN add_months(curr.period_start, -12)
        END
  GROUP BY curr.period_type, curr.period_start
),

crr AS (
  SELECT
    a.period_type,
    a.period_start,
    COALESCE(r.retained_customers, 0) AS retained_customers,
    LAG(a.customers_active) OVER (PARTITION BY a.period_type ORDER BY a.period_start) AS customers_prev_period,
    100.0 * COALESCE(r.retained_customers, 0)
      / NULLIF(LAG(a.customers_active) OVER (PARTITION BY a.period_type ORDER BY a.period_start), 0) AS crr_pct_overall
  FROM active_cnt a
  LEFT JOIN retained r
    ON a.period_type = r.period_type
   AND a.period_start = r.period_start
)

SELECT
  t.period_type,
  t.customer_type,
  CAST(t.period_start as TIMESTAMP),
  t.period_start_date,
  t.company_name,
  t.product_category,

  t.ticket_csat_sum_score,
  t.ticket_csat_cnt,
  t.ticket_csat_topbox_cnt,
  t.tickets_closed,

  c.crr_pct_overall,
  c.retained_customers,
  c.customers_prev_period,
  c.crr_pct_overall as crr_pct
FROM t_agg t
LEFT JOIN crr c
  ON t.period_type = c.period_type
 AND t.period_start = c.period_start
"""

df = spark.sql(sql_query)

# ✅ BỎ partitionBy
(df.repartition(1).write
  .mode("overwrite")
  .format("parquet")
  .option("path", target_path)
  .saveAsTable(target_table)
)

spark.sql("SELECT count(*) AS total_gold FROM " + target_table).show()
