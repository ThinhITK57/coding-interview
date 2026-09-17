# %livy.pyspark
# spark.sql("drop  table IF EXISTS bitu_silver_clone.trino_sql")
# spark.sql("drop  database IF EXISTS bitu_silver_clone1212")
spark.sql("create  database IF NOT EXISTS cx_survicate_silver")
spark.sql("use cx_survicate_silver")
