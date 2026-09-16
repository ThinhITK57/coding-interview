%livy.pyspark

spark.sql("REFRESH TABLE bi_silver.epm_targets")
spark.sql("REFRESH TABLE bi_silver.epm_assignments")
spark.sql("REFRESH TABLE bi_silver.epm_tasks")
spark.sql("REFRESH TABLE bi_silver.epm_projects")

base_tgt_path = "/opt/datasets/crawlers/vcs_silver/bi_silver/data"

# -------------------------------------------------------------
# 1. fact_epm_target_bsc_snapshot (Dùng cho bsc_yearly & board_objectives)
# -------------------------------------------------------------
fact_target = spark.sql("""
SELECT
    CAST(DATE_FORMAT(COALESCE(TO_DATE(t.last_updated_on), CURRENT_DATE()), 'yyyyMMdd') AS INT) AS date_key,
    COALESCE(TO_DATE(t.last_updated_on), CURRENT_DATE()) AS snapshot_date,
    ABS(HASH(t.sysid)) AS target_key,
    CAST(t.sysid AS STRING) AS target_id,

    -- Foreign Keys trỏ sang các Dimension
    ABS(HASH(t.associated_objective)) AS objective_key,
    ABS(HASH(t.associated_item))      AS project_key,
    ABS(HASH(t.c_department))         AS department_key,
    ABS(HASH(t.c_assignee))           AS assignee_key,
    ABS(HASH(COALESCE(a.c_assignor, t.c_assignor, t.created_by))) AS assignor_key,

    -- Các trường định danh và thuộc tính
    CAST(t.associated_objective AS STRING) AS associated_objective,
    CAST(t.associated_item AS STRING) AS associated_item,
    CAST(t.target_type AS STRING) AS target_type,
    CAST(t.parent_target AS STRING) AS parent_target,
    CAST(t.c_department AS STRING) AS c_department,
    CAST(t.name AS STRING) AS name,
    CAST(t.c_assignee AS STRING) AS c_assignee,
    CAST(t.unit AS STRING) AS unit,
    TO_DATE(COALESCE(t.c_target_date_m, t.target_date)) AS target_date_m,
    TO_DATE(t.c_target_date_n) AS target_date_n,
    CAST(COALESCE(t.c_target_value_m, t.target_value) AS DOUBLE) AS target_value_m,
    CAST(t.c_target_value_n AS DOUBLE) AS target_value_n,
    CAST(COALESCE(t.c_target_result_value_m, t.c_target_result_m) AS DOUBLE) AS target_result_m,
    CAST(COALESCE(t.c_target_result_value_n, t.c_target_result_n) AS DOUBLE) AS target_result_n,
    CAST(t.state AS STRING) AS state,
    CAST(t.status AS STRING) AS status,
    CAST(COALESCE(a.c_assignor, t.c_assignor, t.created_by) AS STRING) AS assignor
FROM bi_silver.epm_targets t
LEFT JOIN bi_silver.epm_assignments a ON t.c_associated_assignment = a.sysid
""")

fact_target.write.mode("overwrite").format("parquet") \
    .partitionBy("snapshot_date") \
    .option("path", f"{base_tgt_path}/fact_epm_target_bsc_snapshot") \
    .saveAsTable("bi_silver.fact_epm_target_bsc_snapshot")

# -------------------------------------------------------------
# 2. fact_epm_cvct_execution_snapshot (Dùng cho cvct_execution_report)
# -------------------------------------------------------------
fact_cvct = spark.sql("""
SELECT
    CAST(DATE_FORMAT(COALESCE(TO_DATE(t.last_updated_on), CURRENT_DATE()), 'yyyyMMdd') AS INT) AS date_key,
    COALESCE(TO_DATE(t.last_updated_on), CURRENT_DATE()) AS snapshot_date,
    ABS(HASH(t.sysid)) AS cvct_key,

    -- Foreign Key trỏ sang dim_epm_resource
    ABS(HASH(COALESCE(t.c_assignee, t.entity_owner))) AS assignee_key,

    CAST(COALESCE(t.c_assignee, t.entity_owner) AS STRING) AS assignee,
    CAST(t.resources_and_placeholders_count AS STRING) AS resources,
    CAST(t.name AS STRING) AS name,
    CAST(t.status AS STRING) AS status,
    TO_DATE(COALESCE(t.c_target_date_m, t.target_date)) AS due_date,
    CAST(t.percent_completed AS DOUBLE) AS percent_completed,
    CAST(COALESCE(t.c_target_value_m, t.target_value) AS DOUBLE) AS target_value_m,
    TO_DATE(COALESCE(t.c_target_date_m, t.target_date)) AS target_date_m
FROM bi_silver.epm_targets t
""")

fact_cvct.write.mode("overwrite").format("parquet") \
    .partitionBy("snapshot_date") \
    .option("path", f"{base_tgt_path}/fact_epm_cvct_execution_snapshot") \
    .saveAsTable("bi_silver.fact_epm_cvct_execution_snapshot")

# -------------------------------------------------------------
# 3. fact_epm_task_execution_snapshot (Dùng cho task_report)
# -------------------------------------------------------------
fact_task = spark.sql("""
SELECT
    CAST(DATE_FORMAT(COALESCE(TO_DATE(t.last_updated_on), CURRENT_DATE()), 'yyyyMMdd') AS INT) AS date_key,
    COALESCE(TO_DATE(t.last_updated_on), CURRENT_DATE()) AS snapshot_date,
    ABS(HASH(t.sysid)) AS task_key,

    -- Foreign Keys trỏ sang Dimensions
    ABS(HASH(COALESCE(t.manager, t.c_assignee))) AS assignee_key,
    ABS(HASH(t.c_department))                   AS department_key,
    ABS(HASH(t.parent_project))                  AS project_key,

    CAST(t.task_type AS STRING) AS task_type,
    CAST(COALESCE(t.manager, t.c_assignee) AS STRING) AS assignee,
    CAST(t.c_department AS STRING) AS department,
    CAST(COALESCE(t.all_user_resources_count, t.resources_and_placeholders_count) AS STRING) AS resources,
    CAST(t.parent_project AS STRING) AS parent_project,
    CAST(COALESCE(t.track_status, t.status) AS STRING) AS jira_status,
    CAST(t.name AS STRING) AS name,
    CAST(t.description AS STRING) AS description,
    CAST(t.work AS DOUBLE) AS work,
    CAST(t.duration AS DOUBLE) AS duration,
    CAST(COALESCE(t.c_epm_default, t.default_integration_path) AS STRING) AS epm_default,
    TO_DATE(t.start_date) AS start_date,
    TO_DATE(t.due_date) AS due_date,
    CAST(t.percent_completed AS DOUBLE) AS percent_completed,
    CAST(COALESCE(t.c_update_description, t.overview) AS STRING) AS update_description
FROM bi_silver.epm_tasks t
""")

fact_task.write.mode("overwrite").format("parquet") \
    .partitionBy("snapshot_date") \
    .option("path", f"{base_tgt_path}/fact_epm_task_execution_snapshot") \
    .saveAsTable("bi_silver.fact_epm_task_execution_snapshot")

# -------------------------------------------------------------
# 4. fact_epm_project_progress_snapshot (Dùng cho project_report)
# -------------------------------------------------------------
fact_proj = spark.sql("""
SELECT
    CAST(DATE_FORMAT(COALESCE(TO_DATE(p.last_updated_on), CURRENT_DATE()), 'yyyyMMdd') AS INT) AS date_key,
    COALESCE(TO_DATE(p.last_updated_on), CURRENT_DATE()) AS snapshot_date,
    ABS(HASH(p.sysid)) AS project_key,

    -- Foreign Keys trỏ sang Dimensions
    ABS(HASH(p.c_department))                     AS department_key,
    ABS(HASH(COALESCE(p.c_assignor, p.created_by))) AS assignor_key,
    ABS(HASH(p.c_assignee))                       AS assignee_key,
    ABS(HASH(COALESCE(p.project_manager, p.manager))) AS pm_key,

    CAST(p.name AS STRING) AS name,
    CAST(COALESCE(p.c_internal_type, p.project_type) AS STRING) AS project_type,
    TO_DATE(p.due_date) AS due_date,
    CAST(p.state AS STRING) AS state,
    CAST(COALESCE(p.track_status, p.status) AS STRING) AS status,
    CAST(p.percent_completed AS DOUBLE) AS percent_completed,
    CAST(p.c_department AS STRING) AS department,
    CAST(COALESCE(p.c_assignor, p.created_by) AS STRING) AS assignor,
    CAST(p.c_assignee AS STRING) AS assignee,
    CAST(COALESCE(p.project_manager, p.manager) AS STRING) AS project_manager,
    CAST(COALESCE(p.c_action_resources, CAST(p.resources_and_placeholders_count AS STRING)) AS STRING) AS resources
FROM bi_silver.epm_projects p
""")

fact_proj.write.mode("overwrite").format("parquet") \
    .partitionBy("snapshot_date") \
    .option("path", f"{base_tgt_path}/fact_epm_project_progress_snapshot") \
    .saveAsTable("bi_silver.fact_epm_project_progress_snapshot")

print("DONE: Successfully created 4 EPM Snapshot Facts with Foreign Keys in bi_silver.")