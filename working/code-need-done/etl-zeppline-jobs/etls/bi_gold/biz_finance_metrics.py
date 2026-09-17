%livy.pyspark

target_table = "bi_gold.biz_finance_metrics"
target_path  = "/opt/datasets/crawlers/vcs_gold/bi_gold/data/biz_finance_metrics"

# hard delete path
# hconf = spark._jsc.hadoopConfiguration()
# fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hconf)
# path = spark._jvm.org.apache.hadoop.fs.Path(target_path)
# if fs.exists(path):
#     fs.delete(path, True)

# spark.sql("DROP TABLE IF EXISTS {}".format(target_table))

sql_query = """
WITH rev_agg AS (
  SELECT
    CAST(report_date AS TIMESTAMP) AS report_date,
    category_code,
    product_category,
    SUM(revenue_amount) AS total_revenue
  FROM bi_silver.biz_business_results
  GROUP BY 1, 2, 3
),
cost_agg AS (
  SELECT
    CAST(report_date AS TIMESTAMP) AS report_date,
    category_code,
    product_category,
    SUM(CASE WHEN cost_group = 'Chi phí khấu hao tài sản cố định' THEN base_currency_amount ELSE 0 END) AS fixed_asset_depreciation_cost,
    SUM(CASE WHEN cost_group IN ('Chi phí Nhân công & OS trực tiếp', 'Chi phí Nhân công, outsource trực tiếp') THEN base_currency_amount ELSE 0 END) AS direct_labor_os_cost,
    SUM(CASE WHEN cost_group IN ('Chi phí tài nguyên, CCDC phục vụ sản xuất', 'Chi phí hạ tầng, máy chủ, CCDC sản xuất') THEN base_currency_amount ELSE 0 END) AS production_infra_cost,
    SUM(CASE WHEN cost_group = 'Chi phí OS kinh doanh' THEN base_currency_amount ELSE 0 END) AS business_os_cost,
    SUM(base_currency_amount) AS total_cost
  FROM bi_silver.biz_actual_cost
  GROUP BY 1, 2, 3
),
final_join AS (
  SELECT
    COALESCE(r.report_date, c.report_date) AS report_date,
    COALESCE(r.category_code, c.category_code) AS category_code,
    COALESCE(r.product_category, c.product_category) AS product_category,
    COALESCE(r.total_revenue, 0) AS total_revenue,
    COALESCE(c.fixed_asset_depreciation_cost, 0) AS fixed_asset_depreciation_cost,
    COALESCE(c.direct_labor_os_cost, 0) AS direct_labor_os_cost,
    COALESCE(c.production_infra_cost, 0) AS production_infra_cost,
    COALESCE(c.business_os_cost, 0) AS business_os_cost,
    COALESCE(c.total_cost, 0) AS total_cost
  FROM rev_agg r
  FULL OUTER JOIN cost_agg c
    ON  r.report_date = c.report_date
    AND r.category_code = c.category_code
    AND r.product_category = c.product_category
)
SELECT
  md5(concat_ws('|',
    coalesce(CAST(report_date AS STRING),'__NULL__'),
    coalesce(CAST(category_code AS STRING),'__NULL__'),
    coalesce(CAST(product_category AS STRING),'__NULL__')
  )) AS id,

  report_date,
  category_code,
  product_category,

  ROUND(total_revenue, 2) AS total_revenue,
  ROUND(total_cost, 2) AS total_cost,

  ROUND(total_revenue - (direct_labor_os_cost + production_infra_cost), 2) AS gross_profit,
  ROUND(total_revenue - total_cost + fixed_asset_depreciation_cost, 2)      AS ebitda,

  ROUND(fixed_asset_depreciation_cost, 2) AS fixed_asset_depreciation_cost,
  ROUND(direct_labor_os_cost, 2)          AS direct_labor_os_cost,
  ROUND(production_infra_cost, 2)         AS production_infra_cost,
  ROUND(business_os_cost, 2)              AS business_os_cost
FROM final_join
"""

df = spark.sql(sql_query)

(df.repartition(1).write
  .mode("overwrite")
  .format("parquet")
  .option("path", target_path)
  .saveAsTable(target_table)
)

print("DONE")
