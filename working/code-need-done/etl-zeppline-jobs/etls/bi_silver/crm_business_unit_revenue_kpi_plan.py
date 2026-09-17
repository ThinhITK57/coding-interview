# %livy.pyspark
tgt_table = "bi_silver.crm_business_unit_revenue_kpi_plan"
tgt_path  = "/opt/datasets/crawlers/vcs_silver/bi_silver/data/crm_business_unit_revenue_kpi_plan"



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
spark.sql("REFRESH TABLE crm_tool.spdv_revenue_plan")

query = """
SELECT
    product_category,
    product_category_code,
    product_category_group,

    business_unit_n1,

    CASE business_unit_n1
        WHEN 'TTGS'
            THEN 'Trung tâm Giám sát & Phản ứng trên Không gian mạng'
        WHEN 'TT SP Enterprise'
            THEN 'Trung tâm Sản phẩm Enterprise'
        WHEN 'TT SP Telco'
            THEN 'Trung tâm Sản phẩm Telco'
        WHEN 'TT PT&CSNC'
            THEN 'Trung tâm Phân tích chia sẻ nguy cơ An ninh mạng'
        WHEN 'TT DV ATTT'
            THEN 'Trung tâm Dịch vụ An Toàn Thông Tin'
        WHEN 'SI'
            THEN 'Phòng Hợp tác SI'
        WHEN 'TT SX&CNTT'
            THEN 'Trung tâm Sản xuất & Công nghệ thông tin'
        WHEN 'TT KDMN'
            THEN 'Trung tâm Sản xuất & Công nghệ thông tin'
        ELSE business_unit_n1
    END AS business_unit_n1_full_name,

    report_year,

    CAST(must_internal_revenue       AS DECIMAL(18,2)) AS must_internal_revenue,
    CAST(must_domestic_revenue       AS DECIMAL(18,2)) AS must_domestic_revenue,
    CAST(must_international_revenue  AS DECIMAL(18,2)) AS must_international_revenue,
    CAST(must_bqp_revenue            AS DECIMAL(18,2)) AS must_bqp_revenue,
    CAST(must_total_revenue          AS DECIMAL(18,2)) AS must_total_revenue,

    CAST(should_internal_revenue      AS DECIMAL(18,2)) AS should_internal_revenue,
    CAST(should_domestic_revenue      AS DECIMAL(18,2)) AS should_domestic_revenue,
    CAST(should_international_revenue AS DECIMAL(18,2)) AS should_international_revenue,
    CAST(should_bqp_revenue           AS DECIMAL(18,2)) AS should_bqp_revenue,
    CAST(should_total_revenue         AS DECIMAL(18,2)) AS should_total_revenue,

    CAST(nice_internal_revenue      AS DECIMAL(18,2)) AS nice_internal_revenue,
    CAST(nice_domestic_revenue      AS DECIMAL(18,2)) AS nice_domestic_revenue,
    CAST(nice_international_revenue AS DECIMAL(18,2)) AS nice_international_revenue,
    CAST(nice_bqp_revenue           AS DECIMAL(18,2)) AS nice_bqp_revenue,
    CAST(nice_total_revenue         AS DECIMAL(18,2)) AS nice_total_revenue,

    CAST(must_q1        AS DECIMAL(18,2)) AS must_q1,
    CAST(must_q2        AS DECIMAL(18,2)) AS must_q2,
    CAST(must_q3        AS DECIMAL(18,2)) AS must_q3,
    CAST(must_q4        AS DECIMAL(18,2)) AS must_q4,
    CAST(must_year_total AS DECIMAL(18,2)) AS must_year_total,

    CAST(should_q1        AS DECIMAL(18,2)) AS should_q1,
    CAST(should_q2        AS DECIMAL(18,2)) AS should_q2,
    CAST(should_q3        AS DECIMAL(18,2)) AS should_q3,
    CAST(should_q4        AS DECIMAL(18,2)) AS should_q4,
    CAST(should_year_total AS DECIMAL(18,2)) AS should_year_total,

    CAST(nice_q1        AS DECIMAL(18,2)) AS nice_q1,
    CAST(nice_q2        AS DECIMAL(18,2)) AS nice_q2,
    CAST(nice_q3        AS DECIMAL(18,2)) AS nice_q3,
    CAST(nice_q4        AS DECIMAL(18,2)) AS nice_q4,
    CAST(nice_year_total AS DECIMAL(18,2)) AS nice_year_total

FROM crm_tool.spdv_revenue_plan
"""

df = spark.sql(query)


df = df.withColumn(
    "product_category",
    normalize_udf(F.col("product_category"))
)

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

