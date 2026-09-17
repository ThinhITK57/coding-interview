%livy.pyspark

from pyspark.sql import functions as F

tgt_table = "bi_silver.jira_task_operation"
tgt_path  = "/opt/datasets/crawlers/vcs_silver/bi_silver/data/jira_task_operation"

# -------------------------------------------------------------------------
# 0) HARD DELETE PATH helper (chống double file)
# -------------------------------------------------------------------------


# -------------------------------------------------------------------------
# 1) DỌN DẸP BẢNG CŨ (metadata + physical)
# -------------------------------------------------------------------------
# spark.catalog.clearCache()
# spark.sql("DROP TABLE IF EXISTS " + tgt_table)
# hard_delete_path(tgt_path)

# -------------------------------------------------------------------------
# 2) TRUY VẤN & BIẾN ĐỔI DỮ LIỆU (giữ SQL của bạn)
# -------------------------------------------------------------------------
df_jira_qlnv = spark.sql("""
WITH last_items_cte AS (
    SELECT
        *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY key
                ORDER BY updated_ts DESC
            ) AS rn
        FROM jira_raw.task_operation
    ) t
    WHERE rn = 1
)
    SELECT DISTINCT
        CAST(key AS STRING) AS key,
        TO_TIMESTAMP(TO_DATE(start_date))   AS start_date,
        TO_TIMESTAMP(TO_DATE(duedate))   AS due_date,
        summary             AS summary,
        issuetype as issue_type,
        priority,
        status,
        assignor,
        CASE WHEN work_group = 'nan' or work_group is null THEN 'Others' ELSE work_group END as work_group,
        department_center,
        CASE 
            WHEN assignee LIKE '%@%' THEN split(assignee, '@')[0] 
            ELSE assignee 
        END AS assignee,
        TO_TIMESTAMP(TO_DATE(created))           AS created,
        TO_TIMESTAMP(TO_DATE(updated))           AS updated
    FROM last_items_cte
""")


# -------------------------------------------------------------------------
# 3) CHECK
# -------------------------------------------------------------------------
print("Preview dữ liệu trước khi lưu (Kiểm tra cột assignee):")
df_jira_qlnv.select("key", "assignee").show(10, truncate=False)

# -------------------------------------------------------------------------
# 4) LƯU VÀO SILVER (fresh write)
# -------------------------------------------------------------------------
df_jira_qlnv.repartition(1).write \
    .mode("overwrite") \
    .format("parquet") \
    .option("path", tgt_path) \
    .saveAsTable(tgt_table)
    
spark.catalog.refreshTable(tgt_table)

print("Đã lưu bảng " + tgt_table + " thành công.")

