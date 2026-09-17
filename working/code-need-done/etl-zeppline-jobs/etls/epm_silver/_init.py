%livy.pyspark

spark.sql("CREATE DATABASE IF NOT EXISTS bi_silver")
spark.sql("CREATE DATABASE IF NOT EXISTS epm_silver")
print("Initialized bi_silver and epm_silver databases successfully.")