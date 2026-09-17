%livy.pyspark

# OLD: biz_finance_plan
# NEW: finance_revenue_plan

# spark.sql("DROP TABLE IF EXISTS bi_silver.biz_finance_plan")

target_table = "bi_gold.finance_revenue_plan"
target_path  = "/opt/datasets/crawlers/vcs_gold/bi_gold/data/finance_revenue_plan"

# Hard delete path
# hconf = spark._jsc.hadoopConfiguration()
# fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hconf)
# path = spark._jvm.org.apache.hadoop.fs.Path(target_path)
# if fs.exists(path):
#     fs.delete(path, True)

# Drop table
# spark.sql("DROP TABLE IF EXISTS {}".format(target_table))

sql_query = """
SELECT 
   CAST(plan_date AS TIMESTAMP) AS plan_date,
   plan_year,
   plan_month,
   plan_viettel_group,
   CAST(plan_must AS DECIMAL(18,2)) AS plan_must,
   CAST(plan_nice AS DECIMAL(18,2)) AS plan_nice,
   segment_l1_alias,
   currency_code,
   segment_l1,
   segment_l2,
   CAST(is_international_client AS BOOLEAN) AS is_international_client,
   CAST(is_internal_client AS BOOLEAN)      AS is_internal_client
FROM bi_silver.finance_revenue_plan
"""

df = spark.sql(sql_query)

(df.repartition(1).write
  .mode("overwrite")
  .format("parquet")
  .option("path", target_path)
  .saveAsTable(target_table)
)

print("DONE")