%livy.pyspark

spark.sql("""
    INSERT OVERWRITE TABLE bi_silver.jira_msn
    SELECT DISTINCT
        CAST(`key` AS STRING)               AS `key`,
        TO_TIMESTAMP(start_date)            AS start_date,
        summary                             AS summary,
        issuetype                           AS issuetype,
        status                              AS status,
        assignee                            AS assignee,
        projectkey                          AS projectkey,
        projectname                         AS department_center,
        task_type                           AS task_type,
        TO_TIMESTAMP(duedate)               AS duedate,
        TO_TIMESTAMP(created)               AS created,
        TO_TIMESTAMP(updated)               AS updated
    FROM jira_raw.msn_task
""")

print("Đã lưu bảng bi_silver.jira_msn thành công.")

spark.sql("SELECT * FROM bi_silver.jira_msn LIMIT 10").show(truncate=False)

print("Đã lưu bảng bi_silver.jira_msn thành công.")

# -------------------------------------------------------------------------
# 5) KIỂM TRA BẢNG SAU KHI GHI
# -------------------------------------------------------------------------
print("Truy vấn kiểm tra từ bảng đã lưu:")
spark.sql("SELECT * FROM bi_silver.jira_msn LIMIT 10").show(truncate=False)