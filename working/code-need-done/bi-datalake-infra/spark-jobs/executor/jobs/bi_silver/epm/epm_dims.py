# %livy.pyspark

from pyspark.sql import functions as F
from functools import reduce

# 0) Refresh nguồn
spark.sql("REFRESH TABLE bi_silver.epm_projects")
spark.sql("REFRESH TABLE bi_silver.epm_tasks")
spark.sql("REFRESH TABLE bi_silver.epm_targets")
spark.sql("REFRESH TABLE bi_silver.epm_objectives")
spark.sql("REFRESH TABLE bi_silver.epm_assignments")

base_tgt_path = "/opt/datasets/crawlers/vcs_silver/bi_silver/data"

# -------------------------------------------------------------
# 1. dim_epm_department
# -------------------------------------------------------------
p_df = spark.table("bi_silver.epm_projects")
t_df = spark.table("bi_silver.epm_tasks")
tg_df = spark.table("bi_silver.epm_targets")
o_df = spark.table("bi_silver.epm_objectives")
a_df = spark.table("bi_silver.epm_assignments")

depts = [
    p_df.select(F.col("c_department").alias("department_name")).filter("c_department IS NOT NULL"),
    t_df.select(F.col("c_department").alias("department_name")).filter("c_department IS NOT NULL"),
    tg_df.select(F.col("c_department").alias("department_name")).filter("c_department IS NOT NULL"),
    o_df.select(F.col("c_department").alias("department_name")).filter("c_department IS NOT NULL"),
    a_df.select(F.col("c_department").alias("department_name")).filter("c_department IS NOT NULL")
]
dim_dept = reduce(lambda x, y: x.union(y), depts).distinct().filter("department_name != ''") \
    .withColumn("department_key", F.abs(F.hash(F.col("department_name")))) \
    .select("department_key", "department_name")

dim_dept.repartition(1).write.mode("overwrite").format("parquet") \
    .option("path", f"{base_tgt_path}/dim_epm_department") \
    .saveAsTable("bi_silver.dim_epm_department")

# -------------------------------------------------------------
# 2. dim_epm_resource
# -------------------------------------------------------------
res_cols = [
    (p_df, "project_manager"), (p_df, "c_assignee"),
    (t_df, "manager"), (t_df, "c_assignee"),
    (tg_df, "c_assignee"), (tg_df, "entity_owner"),
    (a_df, "c_assignee"), (a_df, "c_assignor")
]
resources = [df.select(F.col(c).alias("resource_name")).filter(f"{c} IS NOT NULL AND {c} != ''") for df, c in res_cols if c in df.columns]
dim_res = reduce(lambda x, y: x.union(y), resources).distinct() \
    .withColumn("resource_key", F.abs(F.hash(F.col("resource_name")))) \
    .select("resource_key", "resource_name")

dim_res.repartition(1).write.mode("overwrite").format("parquet") \
    .option("path", f"{base_tgt_path}/dim_epm_resource") \
    .saveAsTable("bi_silver.dim_epm_resource")

# -------------------------------------------------------------
# 3. dim_epm_project (SCD Type 2 ready)
# -------------------------------------------------------------
dim_proj = spark.sql("""
SELECT DISTINCT
    ABS(HASH(sysid)) AS project_key,
    CAST(sysid AS STRING) AS project_id,
    CAST(name AS STRING) AS project_name,
    CAST(COALESCE(c_internal_type, project_type) AS STRING) AS project_type,
    CAST(COALESCE(project_manager, manager) AS STRING) AS project_manager,
    CAST(c_department AS STRING) AS department,
    CAST(COALESCE(track_status, status) AS STRING) AS track_status,
    CAST(state AS STRING) AS state,
    TO_DATE(COALESCE(last_updated_on, CURRENT_DATE())) AS valid_from,
    TO_DATE('9999-12-31') AS valid_to,
    TRUE AS is_current,
    1 AS version
FROM bi_silver.epm_projects
WHERE sysid IS NOT NULL
""")
dim_proj.repartition(1).write.mode("overwrite").format("parquet") \
    .option("path", f"{base_tgt_path}/dim_epm_project") \
    .saveAsTable("bi_silver.dim_epm_project")

# -------------------------------------------------------------
# 4. dim_epm_task
# -------------------------------------------------------------
dim_task = spark.sql("""
SELECT DISTINCT
    ABS(HASH(sysid)) AS task_key,
    CAST(sysid AS STRING) AS task_id,
    CAST(name AS STRING) AS task_name,
    CAST(task_type AS STRING) AS task_type,
    CAST(parent_project AS STRING) AS parent_project,
    CAST(COALESCE(track_status, status) AS STRING) AS jira_status
FROM bi_silver.epm_tasks
WHERE sysid IS NOT NULL
""")
dim_task.repartition(1).write.mode("overwrite").format("parquet") \
    .option("path", f"{base_tgt_path}/dim_epm_task") \
    .saveAsTable("bi_silver.dim_epm_task")

# -------------------------------------------------------------
# 5. dim_epm_objective
# -------------------------------------------------------------
dim_obj = spark.sql("""
SELECT DISTINCT
    ABS(HASH(sysid)) AS objective_key,
    CAST(sysid AS STRING) AS objective_id,
    CAST(name AS STRING) AS objective_name,
    CAST(c_objective_type AS STRING) AS objective_type,
    CAST(c_department AS STRING) AS department,
    CAST(parent_objective AS STRING) AS parent_objective,
    CASE WHEN parent_objective IS NULL THEN 1 ELSE 2 END AS hierarchy_level
FROM bi_silver.epm_objectives
WHERE sysid IS NOT NULL
""")
dim_obj.repartition(1).write.mode("overwrite").format("parquet") \
    .option("path", f"{base_tgt_path}/dim_epm_objective") \
    .saveAsTable("bi_silver.dim_epm_objective")

# -------------------------------------------------------------
# 6. dim_epm_assignment
# -------------------------------------------------------------
dim_asn = spark.sql("""
SELECT DISTINCT
    ABS(HASH(sysid)) AS assignment_key,
    CAST(sysid AS STRING) AS assignment_id,
    CAST(name AS STRING) AS assignment_name,
    CAST(c_assignor AS STRING) AS assignor,
    CAST(c_assignee AS STRING) AS assignee,
    CAST(c_department AS STRING) AS department
FROM bi_silver.epm_assignments
WHERE sysid IS NOT NULL
""")
dim_asn.repartition(1).write.mode("overwrite").format("parquet") \
    .option("path", f"{base_tgt_path}/dim_epm_assignment") \
    .saveAsTable("bi_silver.dim_epm_assignment")

print("DONE: Successfully created 6 EPM Dimensions in bi_silver.")