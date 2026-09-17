# %livy.pyspark

# Khởi tạo các cơ sở dữ liệu cho hệ sinh thái EPM
spark.sql("CREATE DATABASE IF NOT EXISTS bi_silver")
spark.sql("CREATE DATABASE IF NOT EXISTS epm_silver")
spark.sql("CREATE DATABASE IF NOT EXISTS bi_gold")

print("Initialized bi_silver, epm_silver, bi_gold databases successfully.")