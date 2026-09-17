# %livy.pyspark

# Khởi tạo các cơ sở dữ liệu tầng Silver cho Data Lake (Phạm vi Spark Engine)
spark.sql("CREATE DATABASE IF NOT EXISTS bi_silver")
spark.sql("CREATE DATABASE IF NOT EXISTS epm_silver")

print("Initialized bi_silver and epm_silver databases successfully.")