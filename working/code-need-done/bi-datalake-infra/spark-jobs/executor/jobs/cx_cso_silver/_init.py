
# %livy.pyspark
# spark.sql("drop  table IF EXISTS bitu_silver_clone.trino_sql")
# spark.sql("drop  database IF EXISTS bitu_silver_clone1212")
spark.sql("create  database IF NOT EXISTS cx_cso_silver")
spark.sql("use cx_cso_silver")
