# %livy.pyspark
tgt_table = "bi_silver.crm_revenue_plan_month"
tgt_path  = "s3a://bi-silver/crm_revenue_plan_month"

import unicodedata
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

def normalize_vietnamese(value):
    if value is None:
        return None
    return unicodedata.normalize("NFC", value)

normalize_udf = F.udf(normalize_vietnamese, StringType())


spark.catalog.clearCache()

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

query = """
SELECT
    -- Month
    CAST(
        TO_TIMESTAMP(plan_date, 'yyyy-MM-dd')
        AS DATE
    ) AS plan_date,

    -- Customer segments
    CASE 
        WHEN customer_segment_l1 = 'International' AND customer_segment_l2 = 'Direct / Local Channel' THEN 'Quốc tế ngoài'
        WHEN customer_segment_l1 = 'International' AND customer_segment_l2 = 'Viettel Global Partner' THEN 'Thị Trường Viettel'
        WHEN customer_segment_l1 = 'Khách hàng ngoài' THEN 'Ngoài'
        WHEN customer_segment_l1 = 'Thị trường Viettel' THEN 'Thị Trường Viettel'
        ELSE customer_segment_l1
    END AS revenue_segment_l1,
     
    customer_segment_l1,
    customer_segment_l2,
    CASE
            WHEN customer_segment_l1 = 'Nội bộ'
              OR (
                    customer_segment_l1 = 'International'
                AND customer_segment_l2 in ('Viettel Global Partner' , 'Thị trường') 
              )
               THEN 'DT nội bộ + Thị trường (cả SI nội bộ)'
            WHEN customer_segment_l1 = 'International'  AND customer_segment_l2 in ( 'Quốc tế ngoài' , 'Direct / Local Channel' )  THEN 'DT Quốc tế (KH ngoài QT)'
            WHEN customer_segment_l1 = 'Bộ Quốc phòng'
                THEN 'DT BQP (gồm cả SI BQP)'
            WHEN customer_segment_l1 = 'Khách hàng ngoài'
                THEN 'DT ngoài trong nước (gồm cả SI ngoài)'
            ELSE 'Khác'
        END AS report_customer_segment,

    -- Revenue plan
    CAST(must_value AS DECIMAL(18, 2)) AS must_value,
    CAST(nice_value AS DECIMAL(18, 2)) AS nice_value,
    CAST(group_value AS DECIMAL(18, 2)) AS group_value,

    -- Adjusted revenue plan
    CAST(adjusted_must_value AS DECIMAL(18, 2)) AS adjusted_must_value,
    CAST(adjusted_nice_value AS DECIMAL(18, 2)) AS adjusted_nice_value,
    CAST(adjusted_group_value AS DECIMAL(18, 2)) AS adjusted_group_value,

    -- Metadata
    file_name,
    created_by,
    CAST(
        TO_TIMESTAMP(created_at)
        AS TIMESTAMP
    ) AS created_at

FROM crm_tool.customer_group_revenue_plan
"""

df = spark.sql(query)

df = df.withColumn(
    "customer_segment_l2",
    normalize_udf(F.col("customer_segment_l2"))
)

df = df.withColumn("revenue_segment_l1", F.initcap(F.col("revenue_segment_l1")))
df = df.withColumn("customer_segment_l1", F.initcap(F.col("customer_segment_l1")))
df = df.withColumn("customer_segment_l2", F.initcap(F.col("customer_segment_l2")))

# 2) Write
df.repartition(1).write \
  .mode("overwrite") \
  .format("parquet") \
  .save(tgt_path)

#   .option("path", tgt_path) \
#   .saveAsTable(tgt_table)
#   .save(tgt_path)

spark.catalog.refreshTable(tgt_table)

print(tgt_table)
print("Rows:", df.count())

