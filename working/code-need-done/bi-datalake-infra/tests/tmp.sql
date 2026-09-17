DROP VIEW IF EXISTS hive.bi_silver.finance_daily_business_metrics;

-- hive.bi_silver.finance_daily_business_metrics source

CREATE VIEW hive.bi_silver.finance_daily_business_metrics SECURITY DEFINER AS
WITH
  actual_cost AS (
   SELECT
     CAST(report_date AS DATE) report_date
   , product_category
   , business_unit_level_1 business_unit_level_1
   , territory_name
   , SUM(base_currency_amount) actual_cost_amount
   FROM
     bi_silver.finance_actual_cost
   GROUP BY CAST(report_date AS DATE), product_category, business_unit_level_1, territory_name
) 
, cash_collection AS (
   SELECT
     CAST(report_date AS DATE) report_date
   , product_category
   , business_unit_level_1 business_unit_level_1
   , territory_name territory_name
   , SUM(revenue_amount) allocated_revenue_amount
   FROM
     bi_silver.finance_allocated_revenue
   GROUP BY CAST(report_date AS DATE), product_category, business_unit_level_1, territory_name
) 
, sales_revenue AS (
   SELECT
     CAST(report_date AS DATE) report_date
   , product_category
   , null business_unit_level_1
   , territory_name territory_name
   , SUM(revenue_amount) sales_revenue_amount
   FROM
     bi_silver.finance_allocated_revenue
   GROUP BY CAST(report_date AS DATE), product_category, territory_name
) 
, revenue_plan AS (
   SELECT
     CAST(plan_date AS DATE) report_date
   , product_category
   , null business_unit_level_1
   , null territory_name
   , SUM(plan_must_amount) planned_revenue_must_amount
   , SUM(plan_nice_amount) planned_revenue_nice_amount
   FROM
     bi_silver.finance_product_revenue_plan
   GROUP BY CAST(plan_date AS DATE), product_category
) 
, fact_union AS (
   SELECT
     report_date
   , product_category
   , business_unit_level_1
   , territory_name
   , actual_cost_amount
   , 0 allocated_revenue_amount
   , 0 sales_revenue_amount
   , 0 planned_revenue_must_amount
   , 0 planned_revenue_nice_amount
   FROM
     actual_cost
UNION ALL    SELECT
     report_date
   , product_category
   , business_unit_level_1
   , territory_name
   , 0
   , allocated_revenue_amount
   , 0
   , 0
   , 0
   FROM
     cash_collection
UNION ALL    SELECT
     report_date
   , product_category
   , business_unit_level_1
   , territory_name
   , 0
   , 0
   , sales_revenue_amount
   , 0
   , 0
   FROM
     sales_revenue
UNION ALL    SELECT
     report_date
   , product_category
   , business_unit_level_1
   , territory_name
   , 0
   , 0
   , 0
   , planned_revenue_must_amount
   , planned_revenue_nice_amount
   FROM
     revenue_plan
) 
SELECT
  CAST(report_date AS timestamp) report_date
, UPPER(COALESCE(product_category, 'Khác')) product_category
, UPPER(COALESCE(business_unit_level_1, 'Khác')) business_unit_level_1
, UPPER(COALESCE(territory_name, 'Khác')) territory_name
, CAST(SUM(actual_cost_amount) AS BIGINT) actual_cost_amount
, CAST(SUM(allocated_revenue_amount) AS BIGINT) allocated_revenue_amount
, CAST(SUM(allocated_revenue_amount) AS BIGINT) cash_collection_amount
, CAST(SUM(sales_revenue_amount) AS BIGINT) sales_revenue_amount
, CAST(SUM(planned_revenue_must_amount) AS BIGINT) planned_revenue_must_amount
, CAST(SUM(planned_revenue_nice_amount) AS BIGINT) planned_revenue_nice_amount
, CAST((SUM(sales_revenue_amount) - SUM(actual_cost_amount)) AS BIGINT) gross_profit_amount
, CAST((SUM(allocated_revenue_amount) - SUM(planned_revenue_must_amount)) AS BIGINT) revenue_vs_plan_gap_amount
FROM
  fact_union
GROUP BY 1, 2, 3, 4;