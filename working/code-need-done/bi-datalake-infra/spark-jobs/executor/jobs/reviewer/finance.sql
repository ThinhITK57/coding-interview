SELECT
    year(report_date) AS year,
    SUM(base_currency_amount) AS total_actual_cost
FROM hive.bi_silver.finance_actual_cost
GROUP BY year(report_date)
ORDER BY year DESC;


SELECT
    year(report_date) AS year,
    SUM(revenue_amount) AS total_cash_collection
FROM hive.bi_silver.crm_allocated_revenue
GROUP BY year(report_date)
ORDER BY year DESC;


SELECT
    year(plan_date) AS year,
    SUM(expected_cost) AS total_cost_plan
FROM hive.bi_silver.finance_cost_plan
GROUP BY year(plan_date)
ORDER BY year DESC;


SELECT
    year(plan_date) AS year,
    SUM(plan_must) AS total_plan_must,
    SUM(plan_nice) AS total_plan_nice
FROM hive.bi_silver.finance_product_revenue_plan
GROUP BY year(plan_date)
ORDER BY year DESC;


SELECT
    year(report_date) AS year,
    SUM(base_currency_amount) AS total_production_cost
FROM hive.bi_silver.finance_production_cost_allocations
GROUP BY year(report_date)
ORDER BY year DESC;


SELECT
    year(plan_date) AS year,
    SUM(plan_must) AS total_plan_must,
    SUM(plan_nice) AS total_plan_nice
FROM hive.bi_silver.finance_revenue_plan
GROUP BY year(plan_date)
ORDER BY year DESC;




SELECT
    a.year,
    a.sales_revenue,
    c.cash_collection
FROM

(
SELECT
    year(report_date) AS year,
    SUM(revenue_amount) AS sales_revenue
FROM hive.bi_silver.crm_allocated_revenue
GROUP BY year(report_date)
) a

LEFT JOIN

(
SELECT
    year(report_date) AS year,
    SUM(revenue_amount) AS cash_collection
FROM hive.bi_silver.crm_allocated_revenue
GROUP BY year(report_date)
) c

ON a.year = c.year

ORDER BY a.year DESC;





SELECT 'finance_actual_cost' table_name, year(report_date) year, SUM(base_currency_amount) value
FROM hive.bi_silver.finance_actual_cost
GROUP BY year(report_date)

UNION ALL

SELECT 'crm_allocated_revenue', year(report_date), SUM(revenue_amount)
FROM hive.bi_silver.crm_allocated_revenue
GROUP BY year(report_date)

UNION ALL

SELECT 'finance_production_cost_allocations', year(report_date), SUM(base_currency_amount)
FROM hive.bi_silver.finance_production_cost_allocations
GROUP BY year(report_date);






SELECT
    date_trunc('month', report_date) AS month,
    product_category,
    unit_level_1,
    territory_name,
    SUM(base_currency_amount) AS total_cost
FROM hive.bi_silver.finance_actual_cost
GROUP BY
    date_trunc('month', report_date),
    product_category,
    unit_level_1,
    territory_name
ORDER BY total_cost DESC, month DESC;



SELECT
    year(report_date) AS year,
    product_category,
    SUM(base_currency_amount) AS total_cost
FROM hive.bi_silver.finance_actual_cost
GROUP BY
    year(report_date),
    product_category
ORDER BY total_cost, year DESC;


SELECT
    date_trunc('month', report_date) AS month,
    product_category,
    unit_level_1,
    revenue_group,
    payment_stage,
    SUM(revenue_amount) AS total_revenue
FROM hive.bi_silver.crm_allocated_revenue
GROUP BY
    date_trunc('month', report_date),
    product_category,
    unit_level_1,
    revenue_group,
    payment_stage
ORDER BY total_revenue DESC, month DESC;



SELECT
    year(report_date) AS year,
    product_category,
    SUM(revenue_amount) AS total_revenue
FROM hive.bi_silver.crm_allocated_revenue
GROUP BY
    year(report_date),
    product_category
ORDER BY total_revenue DESC, year DESC;


SELECT
    date_trunc('month', plan_date) AS month,
    cost_group,
    SUM(expected_cost) AS planned_cost
FROM hive.bi_silver.finance_cost_plan
GROUP BY
    date_trunc('month', plan_date),
    cost_group
ORDER BY planned_cost DESC, month DESC;


SELECT
    year(plan_date) AS year,
    cost_group,
    SUM(expected_cost) AS planned_cost
FROM hive.bi_silver.finance_cost_plan
GROUP BY
    year(plan_date),
    cost_group
ORDER BY planned_cost desc, year DESC;


SELECT
    date_trunc('month', plan_date) AS month,
    product_group,
    product_name,
    SUM(plan_must) AS must_plan,
    SUM(plan_nice) AS nice_plan
FROM hive.bi_silver.finance_product_revenue_plan
GROUP BY
    date_trunc('month', plan_date),
    product_group,
    product_name
ORDER BY must_plan DESC, month DESC;


SELECT
    year(plan_date) AS year,
    product_group,
    SUM(plan_must) AS must_plan,
    SUM(plan_nice) AS nice_plan
FROM hive.bi_silver.finance_product_revenue_plan
GROUP BY
    year(plan_date),
    product_group
ORDER BY must_plan DESC, year DESC;


SELECT
    date_trunc('month', report_date) AS month,
    old_category,
    cost_group,
    territory_name,
    SUM(base_currency_amount) AS production_cost
FROM hive.bi_silver.finance_production_cost_allocations
GROUP BY
    date_trunc('month', report_date),
    old_category,
    cost_group,
    territory_name
ORDER BY production_cost desc, month DESC;


SELECT
    year(report_date) AS year,
    old_category,
    SUM(base_currency_amount) AS production_cost
FROM hive.bi_silver.finance_production_cost_allocations
GROUP BY
    year(report_date),
    old_category
ORDER BY production_cost DESC, year DESC;

SELECT
    date_trunc('month', plan_date) AS month,
    segment_l1,
    segment_l2,
    SUM(plan_must) AS must_plan,
    SUM(plan_nice) AS nice_plan
FROM hive.bi_silver.finance_revenue_plan
GROUP BY
    date_trunc('month', plan_date),
    segment_l1,
    segment_l2
ORDER BY must_plan DESC, month DESC;


SELECT
    year(plan_date) AS year,
    segment_l1,
    SUM(plan_must) AS must_plan,
    SUM(plan_nice) AS nice_plan
FROM hive.bi_silver.finance_revenue_plan
GROUP BY
    year(plan_date),
    segment_l1
ORDER BY must_plan DESC,year DESC;


SELECT
    report_date,
    product_category,
    unit_level_1,
    territory_name,
    COUNT(*) AS row_count
FROM hive.bi_silver.finance_actual_cost
GROUP BY
    report_date,
    product_category,
    unit_level_1,
    territory_name
HAVING COUNT(*) > 1
ORDER BY row_count DESC;


