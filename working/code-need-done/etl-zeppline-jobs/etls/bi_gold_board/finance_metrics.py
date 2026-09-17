%livy.pyspark

# spark.sql("DROP VIEW IF EXISTS bi_gold.finance_metrics")

spark.sql("""
CREATE OR REPLACE VIEW bi_gold.finance_metrics  AS

WITH rev_agg AS (
  SELECT
    CAST(report_date AS DATE) AS report_date,
    product_category_code,
    product_category,
    business_unit_level_1,
    CAST(SUM(revenue_amount) AS BIGINT) AS total_revenue, 
    0 as fixed_asset_depreciation_cost,
    0 as direct_labor_os_cost,
    0 as production_infra_cost,
    0 as business_os_cost,
    0 as total_cost
  FROM bi_silver.crm_allocated_revenue
  GROUP BY 1,2,3,4
),
cost_agg AS (
  SELECT
    CAST(report_date AS DATE) AS report_date,
    product_category_code,
    product_category,
    business_unit_level_1,
    0 as total_revenue,
    

    CAST(SUM(CASE 
        WHEN cost_group = 'Chi phí khấu hao tài sản cố định'
        THEN base_currency_amount ELSE 0 END) AS BIGINT)
        AS fixed_asset_depreciation_cost,

    CAST(SUM(CASE 
        WHEN cost_group IN ('Chi phí Nhân công & OS trực tiếp',
                            'Chi phí Nhân công, outsource trực tiếp')
        THEN base_currency_amount ELSE 0 END) AS BIGINT)
        AS direct_labor_os_cost,

    CAST(SUM(CASE 
        WHEN cost_group IN ('Chi phí tài nguyên, CCDC phục vụ sản xuất',
                            'Chi phí hạ tầng, máy chủ, CCDC sản xuất')
        THEN base_currency_amount ELSE 0 END) AS BIGINT)
        AS production_infra_cost,

    CAST(SUM(CASE 
        WHEN cost_group = 'Chi phí OS kinh doanh'
        THEN base_currency_amount ELSE 0 END) AS BIGINT)
        AS business_os_cost,
    CAST(SUM(base_currency_amount) AS BIGINT) AS total_cost

  FROM bi_silver.finance_actual_cost
  GROUP BY 1,2,3,4
),
union_all_join AS (
  SELECT * from rev_agg
  UNION ALL 
  SELECT * from cost_agg
),
final_join as (
	select 
	report_date, product_category_code, product_category, business_unit_level_1,
	sum(total_revenue) as total_revenue,
	sum(total_cost) as total_cost,
	sum(direct_labor_os_cost) as direct_labor_os_cost,
	sum(production_infra_cost) as production_infra_cost,
	sum(fixed_asset_depreciation_cost) as fixed_asset_depreciation_cost,
	sum(business_os_cost) as business_os_cost
	from union_all_join
	group by 1,2,3,4
)
SELECT
  CAST(report_date as TIMESTAMP) as report_date,
  UPPER(product_category_code) as product_category_code,
  UPPER(product_category) as product_category,
  UPPER(business_unit_level_1) as business_unit_level_1, 

  total_revenue,
  total_cost,

  total_revenue - (direct_labor_os_cost + production_infra_cost) AS gross_profit,
  total_revenue - total_cost + fixed_asset_depreciation_cost     AS ebitda,

  fixed_asset_depreciation_cost,
  direct_labor_os_cost,
  production_infra_cost,
  business_os_cost
FROM final_join

""")
