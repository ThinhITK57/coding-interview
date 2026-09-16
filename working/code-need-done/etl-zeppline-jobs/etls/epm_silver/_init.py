%livy.pyspark

spark.sql("CREATE DATABASE IF NOT EXISTS bi_silver")
spark.sql("CREATE DATABASE IF NOT EXISTS epm_silver")
spark.sql("CREATE DATABASE IF NOT EXISTS bi_gold")
print("Initialized bi_silver, epm_silver, and bi_gold databases successfully.")