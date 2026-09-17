%livy.pyspark


# OLD: biz_cost_plan
# NEW: finance_cost_plan

# spark.sql("DROP TABLE IF EXISTS bi_silver.biz_cost_plan")

target_table = "bi_gold.finance_cost_plan"
target_path  = "/opt/datasets/crawlers/vcs_gold/bi_gold/data/finance_cost_plan"

# Hard delete path
# hconf = spark._jsc.hadoopConfiguration()
# fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hconf)
# path = spark._jvm.org.apache.hadoop.fs.Path(target_path)
# if fs.exists(path):
#     fs.delete(path, True)

# Drop table metadata
# spark.sql("DROP TABLE IF EXISTS {}".format(target_table))

sql_query = """
SELECT 
  CAST(plan_date AS TIMESTAMP) AS plan_date,
  plan_year,
  plan_month,
  cost_code,
  cost_group,
  CAST(expected_cost AS DECIMAL(18,2)) AS expected_cost,
  currency_code,
  cost_group_code
FROM bi_silver.finance_cost_plan
"""

df = spark.sql(sql_query)

(df.repartition(1).write
  .mode("overwrite")
  .format("parquet")
  .option("path", target_path)
  .saveAsTable(target_table)
)

print("DONE")
