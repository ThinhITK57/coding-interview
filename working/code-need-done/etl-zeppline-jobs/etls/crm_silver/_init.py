%livy.pyspark

spark.sql("create database if not exists crm_silver")

spark.sql("use crm_silver")


%livy.pyspark

import json

from datetime import datetime, timedelta
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

def create_hive_tables(spark, table_configs):
    for config in table_configs:
        table_name = str(config['table_name'])
        
        # LOGIC RIÊNG CHO DIM_DATE NẾU SPARK CŨ KHÔNG CÓ SEQUENCE
        if "bitu_gold_data.dim_date" in table_name:
            print("--- Generating DIM_DATE with proper DateType ---")
            start_date = datetime(2024, 1, 1)
            end_date = datetime(2027, 12, 31)
            delta = end_date - start_date
            
            data = []
            for i in range(delta.days + 1):
                d = start_date + timedelta(days=i)
                data.append((
                    d.date(), # Trả về đối tượng date thay vì string
                    int(d.year),
                    int(d.month),
                    d.strftime('%Y-%m'),
                    (d.month - 1) // 3 + 1,
                    int(d.day),
                    d.strftime('%A') # Dùng %A để ra tên ngày đầy đủ
                ))
            
            # Định nghĩa Schema tường minh cho Spark
            schema = StructType([
                StructField("date_key", DateType(), False),
                StructField("year", IntegerType(), False),
                StructField("month", IntegerType(), False),
                StructField("year_month", StringType(), False),
                StructField("quarter", IntegerType(), False),
                StructField("day", IntegerType(), False),
                StructField("day_name", StringType(), False)
            ])
            
            df = spark.createDataFrame(data, schema)
            df.write.mode("overwrite").format("parquet").option("path", str(config['location'])).saveAsTable(table_name)
            continue

        # CÁC BẢNG KHÁC VẪN CHẠY SQL
        try:
            print("--- Processing table: " + table_name + " ---")
            sql_query = str(config['sql'])
            df = spark.sql(sql_query)
            df.repartition(5).write.mode("overwrite").format("parquet").option("path", str(config['location'])).saveAsTable(table_name)
        except Exception as e:
            print("Error: " + str(e))

# Khởi tạo spark hỗ trợ Hive (Thường Livy đã cấu hình sẵn 'spark')
# spark = SparkSession.builder.enableHiveSupport().getOrCreate()

