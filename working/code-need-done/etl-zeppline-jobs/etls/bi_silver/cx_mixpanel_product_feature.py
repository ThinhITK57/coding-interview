%livy.pyspark
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from functools import reduce
from pyspark.sql.functions import col

target_table = "bi_silver.cx_mixpanel_product_feature"
target_path  = "/opt/datasets/crawlers/vcs/bi_silver/data/cx_mixpanel_product_feature"
# ==============================================================================
# 1. LOAD ALL HISTORICAL FEATURE DATA
# ==============================================================================

TARGET_TABLE_PATH = "/opt/datasets/crawlers/vcs/cx_mixpanel_silver/data/event_product_feature_monthly"


def get_parquet_paths(spark, base_path):
    jvm = spark._jvm
    conf = spark._jsc.hadoopConfiguration()
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(conf)
    Path = jvm.org.apache.hadoop.fs.Path
    result = []
    def walk(path):
        statuses = fs.listStatus(Path(path))
        for status in statuses:
            p = status.getPath().toString()
            if status.isDirectory():
                walk(p)
            elif p.endswith(".parquet"):
                result.append(p)
    walk(base_path)
    return result

    return paths

paths = get_parquet_paths(spark, TARGET_TABLE_PATH)

if not paths:
    raise ValueError("No parquet files found in {}".format(TARGET_TABLE_PATH))

ti_product_feature_base_src = spark.read.parquet(*paths)
ti_product_feature_base_src.createOrReplaceTempView("ti_product_feature_base_src")


# ==============================================================================
# 2. CALCULATE FEATURE USAGE, ACTIVE USER, BLACKLIST USER
# ==============================================================================

mixpanel_product_feature_src = spark.sql("""
WITH counting_product_feature_use AS (
    SELECT
        report_date,
        event_name,
        user_id,
        product_feature,
        product_category,
        event_ts,
        CASE
            WHEN product_feature IS NOT NULL THEN ROW_NUMBER() OVER (
                PARTITION BY report_date, product_feature, user_id, product_category
                ORDER BY event_ts
            )
        END AS click_index
    FROM ti_product_feature_base_src
    WHERE user_id IS NOT NULL
),

active_user AS (
    SELECT
        report_date,
        user_id,
        product_category,
        MAX(CASE WHEN event_name IS NOT NULL THEN 1 ELSE 0 END) AS is_active_user
    FROM ti_product_feature_base_src
    WHERE user_id IS NOT NULL
    GROUP BY report_date, user_id, product_category
),

blacklist_user AS (
    SELECT
        user_id,
        product_category,
        inserted_ts
    FROM (
        SELECT
            user_id,
            product_category,
            inserted_ts,
            ROW_NUMBER() OVER (
                PARTITION BY user_id, product_category
                ORDER BY inserted_ts DESC
            ) AS rnk
        FROM cx_mixpanel_raw.excel_blacklist_user
    ) t
    WHERE rnk = 1
),

product_feature_summary_denormalize AS (
    SELECT
        report_date,
        user_id,
        product_feature,
        product_category,
        IF(product_feature IS NOT NULL, SORT_ARRAY(COLLECT_LIST(STRUCT(event_ts AS event_ts, click_index AS process_index))), NULL) AS clicking_process
    FROM counting_product_feature_use
    GROUP BY report_date, user_id, product_feature, product_category
)

SELECT
    fs.report_date,
    fs.user_id,
    fs.product_feature,
    fs.product_category,
    COALESCE(au.is_active_user, 0) AS is_active_user,
    CASE
        WHEN bl.user_id IS NOT NULL THEN 1
        ELSE 0
    END AS is_blacklist_user,
    SIZE(fs.clicking_process) AS product_feature_usage_count
FROM product_feature_summary_denormalize fs
LEFT JOIN active_user au
    ON fs.report_date = au.report_date
    AND fs.user_id = au.user_id
LEFT JOIN blacklist_user bl
    ON fs.user_id = bl.user_id
    AND fs.product_category = bl.product_category
ORDER BY fs.report_date DESC
""")

mixpanel_product_feature_src.createOrReplaceTempView("mixpanel_product_feature_src")


sql_query = """
    SELECT 
        CAST(report_date AS DATE) AS report_date,
        CAST(user_id AS STRING) AS user_id,
        CAST(product_feature AS STRING) AS product_feature,
        CAST(product_category AS STRING) AS product_category,
        CAST(is_active_user AS INT) AS is_active_user,
        CAST(is_blacklist_user AS INT) AS is_blacklist_user,
        CAST(product_feature_usage_count AS INT) AS product_feature_usage_count,
        CURRENT_TIMESTAMP() AS inserted_ts
    FROM mixpanel_product_feature_src
"""

df = spark.sql(sql_query)

(df.repartition(1).write
  .mode("overwrite")
  .format("parquet")
  .option("path", target_path)
  .saveAsTable(target_table)
#   .save(target_path)
)
spark.catalog.refreshTable(target_table)
print(target_table)