"""
Builds:
- 7 conformed dimesions:
    1. dim_date
    2. dim_department
    3. dim_resource 
    4. dim_project (scd type 2: PM, Dept, Status tracking)
    5. dim_task 
    6. dim_objective (BSC)
    7. dim_assignment (PGNV)
- 5 periodic snapshot facts:
    1. fact_target_bsc_snapshot (bussiness_requirement 1, 6)
    2. fact_cvct_execution_snapshot (bussiness_requirement 2)
    3. fact_task_execution_snapshot (bussiness_requirement 3)
    4. fact_project_progress_snapshot (bussiness_requirement 4)
    5. fact_epm_user_access (bussiness_requirement 5)

"""

import os
import sys
import logging
import argparse
from datetime import datetime, timedelta


ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from config.env_loader import load_env_file
load_env_file()

from transform.spark_session import SparkSessionFactory
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType, DoubleType, DateType, IntegerType, LongType, StructType, StructField, BooleanType
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("build_silver_dims_facts")


def load_silver_table(spark, base_path: str, table_name: str):
    primary = f"{base_path.rstrip('/')}/{table_name}"
    local = os.path.join(ROOT_DIR, "data", "silver", "epm", table_name)

    if not base_path.startswith("./") and os.getenv('MINIO_ENDPOINT'):
        try:
            return spark.read.parquet(primary)
        except Exception:
            pass

    if os.path.exists(local):
        return spark.read.parquet(local)

    return spark.createDataFrame([], StructType([StructField("sysid", StringType(), True)]))


def save_silver_dataset(df, base_path: str, dataset_name: str, partition_cols=None):
    output_path = f"{base_path.rstrip('/')}/{dataset_name}"
    local_output = os.path.join(ROOT_DIR, "data", "silver", "epm", dataset_name)

    if base_path.startswith("./") or not os.getenv("MINIO_ENDPOINT"):
        output_path = local_output

    os.makedirs(os.path.dirname(local_output), exist_ok=True)
    count = df.count()
    logger.info(f"Writing {dataset_name} ({count} rows) -> {output_path}")

    writer = df.write.mode("overwrite").format("parquet").option("compression", "snappy")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.save(
        output_path
    )
    return {"name": dataset_name, "count": count, "path":  output_path}


def build_dim_date(spark, start_year=2020, end_year=2030):
    logger.info("Generating dim_date 2020 - 2030")
    start_date = datetime(start_year, 1, 1)
    end_date = datetime(end_year, 12, 31)

    days = (end_date - start_date).days + 1

    date_rows = []
    for i in range(days):
        dt = start_date + timedelta(days=i)
        date_key = int(dt.strftime("%Y%m%d"))
        date_rows.append((
            date_key,
            dt.date(),
            dt.year,
            (dt.month - 1) // 3 + 1,
            dt.month,
            dt.day,
            dt.weekday() + 1,
            dt.strftime("%A"),
            dt.strftime("%B"),
            1 if dt.weekday() >= 5 else 0,
            dt.year if dt.month >= 4 else dt.year - 1, # Fiscal Year (April start)
            ((dt.month - 4) % 12) // 3 + 1
        ))

    schema = StructType([
        StructField("date_key", IntegerType(), False),
        StructField("full_date", DateType(), False),
        StructField("year", IntegerType(), False),
        StructField("quarter", IntegerType(), False),
        StructField("month", IntegerType(), False),
        StructField("day", IntegerType(), False),
        StructField("day_of_week", IntegerType(), False),
        StructField("day_name", StringType(), False),
        StructField("month_name", StringType(), False),
        StructField("is_weekend", IntegerType(), False),
        StructField("fiscal_year", IntegerType(), False),
        StructField("fiscal_quarter", IntegerType(), False)
    ])

    return spark.createDataFrame(date_rows, schema=schema)

def build_dim_department(projects_df, tasks_df, targets_df, objectives_df, assignments_df):
    logger.info("Building conformed dim_department")
    depts = []

    for df, col_name in [
        (projects_df, "c_department"),
        (tasks_df, "c_department"),
        (targets_df, "c_department"),
        (objectives_df, "c_department"),
        (assignments_df, "c_department")
    ]:
        if col_name in df.columns:
            depts.append(
                df.select(F.col(col_name).cast(StringType()).alias("department_name")).filter("department_name is not null")
            )

    if not depts:
        return projects_df.sql_ctx.createDataFrame([], StructType([
            StructField("department_key", IntegerType(), False),
            StructField("department_name", StringType(), False)
        ]))

    from functools import reduce
    union_df = reduce(lambda a, b: a.union(b), depts).distinct()
    union_df = union_df.filter("department_name !=''")

    # add surrogate key using monotonically increasing id or hash
    dim_dept = union_df.withColumn("department_key", F.abs(F.hash(F.col("department_name"))))
    return dim_dept.select("department_key", "department_name")

def build_dim_resource(projects_df, tasks_df, targets_df, assignments_df, access_log_df):
    logger.info("Building conformed dim_resource")
    resources = []
    col_pairs = [
        (projects_df, "project_manager"),
        (projects_df, "c_assignee"),
        (projects_df, "c_assignor"),
        (tasks_df, "manager"),
        (tasks_df, "c_assignee"),
        (targets_df, "c_assignee"),
        (targets_df, "entity_owner"),
        (assignments_df, "c_assignee"),
        (assignments_df, "c_assignor"),
    ]
    # (access_log_df, "name")


    for df, col_name in col_pairs:
        if col_name in df.columns:
            resources.append(df.select(F.col(col_name).cast(StringType()).alias("resource_name")).filter("resource_name is not null"))

    from functools import reduce
    if not resources:
        return projects_df.sql_ctx.createDataFrame([], StructType([
            StructField("resource_key", IntegerType(), False),
            StructField("resource_name", StringType(), False)
        ]))

    union_df = reduce(lambda a, b: a.union(b), resources).distinct()
    union_df = union_df.filter("resource_name != ''")
    dim_resource = union_df.withColumn("resource_key", F.abs(F.hash(F.col("resource_name"))))
    return dim_resource.select("resource_key", "resource_name")

def build_dim_project(projects_df):
    logger.info("Building dim_project (SCD Type 2 ready)...")
    p = projects_df
    sysid_col = p["sysid"] if "sysid" in p.columns else F.lit("UNKNOWN")
    name_col = p["name"] if "name" in p.columns else F.lit("UNKNOWN")
    type_col = p["c_internal_type"] if "c_internal_type" in p.columns else (p["project_type"] if "project_type" in p.columns else F.lit(None))
    pm_col = p["project_manager"] if "project_manager" in p.columns else (p["manager"] if "manager" in p.columns else F.lit(None))
    dept_col = p["c_department"] if "c_department" in p.columns else F.lit(None)
    status_col = p["track_status"] if "track_status" in p.columns else (p["status"] if "status" in p.columns else F.lit(None))
    state_col = p["state"] if "state" in p.columns else F.lit(None)
    wm_col = p["last_updated_on"] if "last_updated_on" in p.columns else F.current_date()

    dim_proj = p.select(
        F.abs(F.hash(sysid_col)).alias("project_key"),
        sysid_col.cast(StringType()).alias("project_id"),
        name_col.cast(StringType()).alias("project_name"),
        type_col.cast(StringType()).alias("project_type"),
        pm_col.cast(StringType()).alias("project_manager"),
        dept_col.cast(StringType()).alias("department"),
        status_col.cast(StringType()).alias("track_status"),
        state_col.cast(StringType()).alias("state"),
        F.to_date(wm_col).alias("valid_from"),
        F.to_date(F.lit("9999-12-31")).alias("valid_to"),
        F.lit(True).alias("is_current"),
        F.lit(1).alias("version")
    ).distinct()
    return dim_proj

def build_dim_task(tasks_df):
    logger.info("Building dim_task")
    t = tasks_df
    sysid_col = t["sysid"] if "sysid" in t.columns else F.lit("UNKNOWN")
    name_col = t["name"] if "name" in t.columns else F.lit("UNKNOWN")
    task_type = t["task_type"] if "task_type" in t.columns else F.lit(None)
    parent_proj = t["parent_project"] if "parent_project" in t.columns else F.lit(None)
    jira_status = t["track_status"] if "track_status" in t.columns else (t["status"] if "status" in t.columns else F.lit(None))

    dim_t = t.select(
        F.abs(F.hash(sysid_col)).alias("task_key"),
        sysid_col.cast(StringType()).alias("task_id"),
        name_col.cast(StringType()).alias("task_name"),
        task_type.cast(StringType()).alias("task_type"),
        parent_proj.cast(StringType()).alias("parent_project"),
        jira_status.cast(StringType()).alias("jira_status")
    ).distinct()
    return dim_t


def build_dim_objective(objectives_df):
    """Builds dim_objective for BSC strategic alignment."""
    logger.info("Building dim_objective...")
    o = objectives_df
    sysid_col = o["sysid"] if "sysid" in o.columns else F.lit("UNKNOWN")
    name_col = o["name"] if "name" in o.columns else F.lit("UNKNOWN")
    type_col = o["c_objective_type"] if "c_objective_type" in o.columns else F.lit(None)
    dept_col = o["c_department"] if "c_department" in o.columns else F.lit(None)
    parent_obj = o["parent_objective"] if "parent_objective" in o.columns else F.lit(None)

    dim_o = o.select(
        F.abs(F.hash(sysid_col)).alias("objective_key"),
        sysid_col.cast(StringType()).alias("objective_id"),
        name_col.cast(StringType()).alias("objective_name"),
        type_col.cast(StringType()).alias("objective_type"),
        dept_col.cast(StringType()).alias("department"),
        parent_obj.cast(StringType()).alias("parent_objective"),
        F.when(parent_obj.isNull(), F.lit(1)).otherwise(F.lit(2)).alias("hierarchy_level")
    ).distinct()
    return dim_o


def build_dim_assignment(assignments_df):
    """Builds dim_assignment for PGNV KPI allocations."""
    logger.info("Building dim_assignment...")
    a = assignments_df
    sysid_col = a["sysid"] if "sysid" in a.columns else F.lit("UNKNOWN")
    name_col = a["name"] if "name" in a.columns else F.lit("UNKNOWN")
    assignor = a["c_assignor"] if "c_assignor" in a.columns else F.lit(None)
    assignee = a["c_assignee"] if "c_assignee" in a.columns else F.lit(None)
    dept = a["c_department"] if "c_department" in a.columns else F.lit(None)

    dim_a = a.select(
        F.abs(F.hash(sysid_col)).alias("assignment_key"),
        sysid_col.cast(StringType()).alias("assignment_id"),
        name_col.cast(StringType()).alias("assignment_name"),
        assignor.cast(StringType()).alias("assignor"),
        assignee.cast(StringType()).alias("assignee"),
        dept.cast(StringType()).alias("department")
    ).distinct()
    return dim_a


## FACT BUILDER
def build_fact_target_bsc_snapshot(targets_df, assignments_df):
    logger.info("Buidling fact target bsc snapshot")
    t = targets_df
    a = assignments_df.select(
        assignments_df["sysid"].alias("asn_sysid"),
        assignments_df["c_assignor"].alias("asn_assignor")
    ) if "sysid" in assignments_df.columns and "c_assignor" in assignments_df.columns else None

    if a is not None and "c_associated_assignment" in t.columns:
        t = t.join(a, t["c_associated_assignment"] == a["asn_sysid"], how="left")
        assignor_col = F.coalesce(t["asn_assignor"], t["c_assignor"] if "c_assignor" in t.columns else F.lit(None), t["created_by"] if "created_by" in t.columns else F.lit(None))
    else:
        assignor_col = F.coalesce(t["c_assignor"] if "c_assignor" in t.columns else F.lit(None), t["created_by"] if "created_by" in t.columns else F.lit(None))

    snapshot_date = F.coalesce(F.to_date(t["last_updated_on"]), F.current_date()) if "last_updated_on" in t.columns else F.current_date()
    date_key = F.date_format(snapshot_date, "yyyyMMdd").cast(IntegerType())

    res_m = F.coalesce(
        t["c_target_result_value_m"].cast(DoubleType()) if "c_target_result_value_m" in t.columns else F.lit(None),
        t["c_target_result_m"].cast(DoubleType()) if "c_target_result_m" in t.columns else F.lit(None)
    )

    res_n = F.coalesce(
        t["c_target_result_value_n"].cast(DoubleType()) if "c_target_result_value_n" in t.columns else F.lit(None),
        t["c_target_result_n"].cast(DoubleType()) if "c_target_result_n" in t.columns else F.lit(None)
    )

    fact = t.select(
        date_key.alias("date_key"),
        snapshot_date.alias("snapshot_date"),
        F.abs(F.hash(t["sysid"])).alias("target_key"),
        t["sysid"].cast(StringType()).alias("target_id"),
        t["associated_objective"].cast(StringType()).alias("associated_objective"),
        t["associated_item"].cast(StringType()).alias("associated_item"),
        t["target_type"].cast(StringType()).alias("target_type"),
        t["parent_target"].cast(StringType()).alias("parent_target"),
        t["c_department"].cast(StringType()).alias("c_department"),
        t["name"].cast(StringType()).alias("name"),
        t["c_assignee"].cast(StringType()).alias("c_assignee"),
        t["unit"].cast(StringType()).alias("unit"),
        F.to_date(t["c_target_date_m"] if "c_target_date_m" in t.columns else t["target_date"]).alias("target_date_m"),
        F.to_date(t["c_target_date_n"] if "c_target_date_n" in t.columns else F.lit(None)).alias("target_date_n"),
        t["c_target_value_m"].cast(DoubleType()).alias("target_value_m") if "c_target_value_m" in t.columns else t["target_value"].cast(DoubleType()).alias("target_value_m"),
        t["c_target_value_n"].cast(DoubleType()).alias("target_value_n") if "c_target_value_n" in t.columns else F.lit(None).cast(DoubleType()).alias("target_value_n"),
        res_m.alias("target_result_m"),
        res_n.alias("target_result_n"),
        t["state"].cast(StringType()).alias("state") if "state" in t.columns else F.lit(None).alias("state"),
        t["status"].cast(StringType()).alias("status") if "status" in t.columns else F.lit(None).alias("status"),
        assignor_col.cast(StringType()).alias("assignor")
    )
    return fact

def build_fact_cvct_execution_snapshot(targets_df):
    """Builds fact_cvct_execution_snapshot for BR 2."""
    logger.info("Building fact_cvct_execution_snapshot...")
    t = targets_df
    snapshot_date = F.coalesce(F.to_date(t["last_updated_on"]), F.current_date()) if "last_updated_on" in t.columns else F.current_date()
    date_key = F.date_format(snapshot_date, "yyyyMMdd").cast(IntegerType())

    fact = t.select(
        date_key.alias("date_key"),
        snapshot_date.alias("snapshot_date"),
        F.abs(F.hash(t["sysid"])).alias("cvct_key"),
        t["c_assignee"].cast(StringType()).alias("assignee") if "c_assignee" in t.columns else t["entity_owner"].cast(StringType()).alias("assignee"),
        t["resources_and_placeholders_count"].cast(StringType()).alias("resources") if "resources_and_placeholders_count" in t.columns else F.lit(None).cast(StringType()).alias("resources"),
        t["name"].cast(StringType()).alias("name"),
        t["status"].cast(StringType()).alias("status"),
        F.to_date(t["c_target_date_m"] if "c_target_date_m" in t.columns else t["target_date"]).alias("due_date"),
        t["percent_completed"].cast(DoubleType()).alias("percent_completed") if "percent_completed" in t.columns else F.lit(None).cast(DoubleType()).alias("percent_completed"),
        t["c_target_value_m"].cast(DoubleType()).alias("target_value_m") if "c_target_value_m" in t.columns else t["target_value"].cast(DoubleType()).alias("target_value_m"),
        F.to_date(t["c_target_date_m"] if "c_target_date_m" in t.columns else t["target_date"]).alias("target_date_m")
    )
    return fact


def build_fact_task_execution_snapshot(tasks_df):
    """Builds fact_task_execution_snapshot for BR 3."""
    logger.info("Building fact_task_execution_snapshot...")
    t = tasks_df
    snapshot_date = F.coalesce(F.to_date(t["last_updated_on"]), F.current_date()) if "last_updated_on" in t.columns else F.current_date()
    date_key = F.date_format(snapshot_date, "yyyyMMdd").cast(IntegerType())

    # fact = t.select(
    #     date_key.alias("date_key"),
    #     snapshot_date.alias("snapshot_date"),
    #     F.abs(F.hash(t["sysid"])).alias("task_key"),
    #     t["task_type"].cast(StringType()).alias("task_type") if "task_type" in t.columns else F.lit(None).alias("task_type"),
    #     F.coalesce(t["manager"], t["c_assignee"]).cast(StringType()).alias("assignee") if "manager" in t.columns and "c_assignee" in t.columns else F.lit(None).alias("assignee"),
    #     t["c_department"].cast(StringType()).alias("department") if "c_department" in t.columns else F.lit(None).alias("department"),
    #     t["all_user_resources_count"].cast(StringType()).alias("resources") if "all_user_resources_count" in t.columns else F.lit(None).alias("resources"),
    #     t["parent_project"].cast(StringType()).alias("parent_project") if "parent_project" in t.columns else F.lit(None).alias("parent_project"),
    #     F.coalesce(t["track_status"], t["status"]).cast(StringType()).alias("jira_status") if "track_status" in t.columns and "status" in t.columns else F.lit(None).alias("jira_status"),
    #     t["name"].cast(StringType()).alias("name"),
    #     t["description"].cast(StringType()).alias("description") if "description" in t.columns else F.lit(None).alias("description"),
    #     t["work"].cast(DoubleType()).alias("work") if "work" in t.columns else F.lit(None).cast(DoubleType()).alias("work"),
    #     t["duration"].cast(DoubleType()).alias("duration") if "duration" in t.columns else F.lit(None).cast(DoubleType()).alias("duration"),
    #     F.coalesce(t["c_epm_default"], t["default_integration_path"]).cast(StringType()).alias("epm_default") if "c_epm_default" in t.columns and "default_integration_path" in t.columns else F.lit(None).alias("epm_default"),
    #     F.to_date(t["start_date"]).alias("start_date") if "start_date" in t.columns else F.lit(None).cast(DateType()).alias("start_date"),
    #     F.to_date(t["due_date"]).alias("due_date") if "due_date" in t.columns else F.lit(None).cast(DateType()).alias("due_date"),
    #     t["percent_completed"].cast(DoubleType()).alias("percent_completed") if "percent_completed" in t.columns else F.lit(None).cast(DoubleType()).alias("percent_completed"),
    #     F.coalesce(t["c_update_description"], t["overview"]).cast(StringType()).alias("update_description") if "c_update_description" in t.columns and "overview" in t.columns else F.lit(None).alias("update_description")
    # )

    fact = t.select(
        date_key.alias("date_key"),
        snapshot_date.alias("snapshot_date"),
        F.abs(F.hash(t["sysid"])).alias("task_key"),
        
        (t["task_type"].cast(StringType()) if "task_type" in t.columns else F.lit(None).cast(StringType())).alias("task_type"),
        
        (F.coalesce(t["manager"], t["c_assignee"]).cast(StringType()) if "manager" in t.columns and "c_assignee" in t.columns else F.lit(None).cast(StringType())).alias("assignee"),
        
        (t["c_department"].cast(StringType()) if "c_department" in t.columns else F.lit(None).cast(StringType())).alias("department"),
        
        (t["all_user_resources_count"].cast(StringType()) if "all_user_resources_count" in t.columns else F.lit(None).cast(StringType())).alias("resources"),
        
        (t["parent_project"].cast(StringType()) if "parent_project" in t.columns else F.lit(None).cast(StringType())).alias("parent_project"),
        
        (F.coalesce(t["track_status"], t["status"]).cast(StringType()) if "track_status" in t.columns and "status" in t.columns else F.lit(None).cast(StringType())).alias("jira_status"),
        
        t["name"].cast(StringType()).alias("name"),
        
        (t["description"].cast(StringType()) if "description" in t.columns else F.lit(None).cast(StringType())).alias("description"),
        
        (t["work"].cast(DoubleType()) if "work" in t.columns else F.lit(None).cast(DoubleType())).alias("work"),
        
        (t["duration"].cast(DoubleType()) if "duration" in t.columns else F.lit(None).cast(DoubleType())).alias("duration"),
        
        (F.coalesce(t["c_epm_default"], t["default_integration_path"]).cast(StringType()) if "c_epm_default" in t.columns and "default_integration_path" in t.columns else F.lit(None).cast(StringType())).alias("epm_default"),
        
        (F.to_date(t["start_date"]) if "start_date" in t.columns else F.lit(None).cast(DateType())).alias("start_date"),
        
        (F.to_date(t["due_date"]) if "due_date" in t.columns else F.lit(None).cast(DateType())).alias("due_date"),
        
        (t["percent_completed"].cast(DoubleType()) if "percent_completed" in t.columns else F.lit(None).cast(DoubleType())).alias("percent_completed"),
        
        (F.coalesce(t["c_update_description"], t["overview"]).cast(StringType()) if "c_update_description" in t.columns and "overview" in t.columns else F.lit(None).cast(StringType())).alias("update_description")
    )
    return fact


def build_fact_project_progress_snapshot(projects_df):
    """Builds fact_project_progress_snapshot for BR 4."""
    logger.info("Building fact_project_progress_snapshot...")
    p = projects_df
    snapshot_date = F.coalesce(F.to_date(p["last_updated_on"]), F.current_date()) if "last_updated_on" in p.columns else F.current_date()
    date_key = F.date_format(snapshot_date, "yyyyMMdd").cast(IntegerType())

    # fact = p.select(
    #     date_key.alias("date_key"),
    #     snapshot_date.alias("snapshot_date"),
    #     F.abs(F.hash(p["sysid"])).alias("project_key"),
    #     p["name"].cast(StringType()).alias("name"),
    #     F.coalesce(p["c_internal_type"], p["project_type"]).cast(StringType()).alias("project_type") if "c_internal_type" in p.columns and "project_type" in p.columns else F.lit(None).alias("project_type"),
    #     F.to_date(p["due_date"]).alias("due_date") if "due_date" in p.columns else F.lit(None).cast(DateType()).alias("due_date"),
    #     p["state"].cast(StringType()).alias("state") if "state" in p.columns else F.lit(None).alias("state"),
    #     F.coalesce(p["track_status"], p["status"]).cast(StringType()).alias("status") if "track_status" in p.columns and "status" in p.columns else F.lit(None).alias("status"),
    #     p["percent_completed"].cast(DoubleType()).alias("percent_completed") if "percent_completed" in p.columns else F.lit(None).cast(DoubleType()).alias("percent_completed"),
    #     p["c_department"].cast(StringType()).alias("department") if "c_department" in p.columns else F.lit(None).alias("department"),
    #     F.coalesce(p["c_assignor"], p["created_by"]).cast(StringType()).alias("assignor") if "c_assignor" in p.columns and "created_by" in p.columns else F.lit(None).alias("assignor"),
    #     p["c_assignee"].cast(StringType()).alias("assignee") if "c_assignee" in p.columns else F.lit(None).alias("assignee"),
    #     F.coalesce(p["project_manager"], p["manager"]).cast(StringType()).alias("project_manager") if "project_manager" in p.columns and "manager" in p.columns else F.lit(None).alias("project_manager"),
    #     p["c_action_resources"].cast(StringType()).alias("resources") if "c_action_resources" in p.columns else F.lit(None).alias("resources")
    # )
    fact = p.select(
        date_key.alias("date_key"),
        snapshot_date.alias("snapshot_date"),
        F.abs(F.hash(p["sysid"])).alias("project_key"),
        p["name"].cast(StringType()).alias("name"),
        
        (F.coalesce(p["c_internal_type"], p["project_type"]).cast(StringType()) if "c_internal_type" in p.columns and "project_type" in p.columns else F.lit(None).cast(StringType())).alias("project_type"),
        
        (F.to_date(p["due_date"]) if "due_date" in p.columns else F.lit(None).cast(DateType())).alias("due_date"),
        
        (p["state"].cast(StringType()) if "state" in p.columns else F.lit(None).cast(StringType())).alias("state"),
        
        (F.coalesce(p["track_status"], p["status"]).cast(StringType()) if "track_status" in p.columns and "status" in p.columns else F.lit(None).cast(StringType())).alias("status"),
        
        (p["percent_completed"].cast(DoubleType()) if "percent_completed" in p.columns else F.lit(None).cast(DoubleType())).alias("percent_completed"),
        
        (p["c_department"].cast(StringType()) if "c_department" in p.columns else F.lit(None).cast(StringType())).alias("department"),
        
        (F.coalesce(p["c_assignor"], p["created_by"]).cast(StringType()) if "c_assignor" in p.columns and "created_by" in p.columns else F.lit(None).cast(StringType())).alias("assignor"),
        
        (p["c_assignee"].cast(StringType()) if "c_assignee" in p.columns else F.lit(None).cast(StringType())).alias("assignee"),
        
        (F.coalesce(p["project_manager"], p["manager"]).cast(StringType()) if "project_manager" in p.columns and "manager" in p.columns else F.lit(None).cast(StringType())).alias("project_manager"),
        
        (p["c_action_resources"].cast(StringType()) if "c_action_resources" in p.columns else F.lit(None).cast(StringType())).alias("resources")
    )
    return fact


def build_fact_epm_user_access(access_log_df):
    """Builds fact_epm_user_access for BR 5."""
    logger.info("Building fact_epm_user_access...")
    u = access_log_df
    log_date = F.to_date(u["login_date"]) if "login_date" in u.columns else F.current_date()
    date_key = F.date_format(log_date, "yyyyMMdd").cast(IntegerType())

    fact = u.select(
        date_key.alias("date_key"),
        log_date.alias("login_date"),
        u["name"].cast(StringType()).alias("name"),
        u["first_name"].cast(StringType()).alias("first_name") if "first_name" in u.columns else F.lit(None).cast(StringType()).alias("first_name"),
        u["last_name"].cast(StringType()).alias("last_name") if "last_name" in u.columns else F.lit(None).cast(StringType()).alias("last_name"),
        u["groups"].cast(StringType()).alias("groups") if "groups" in u.columns else F.lit(None).cast(StringType()).alias("groups"),
        u["direct_manager"].cast(StringType()).alias("direct_manager") if "direct_manager" in u.columns else F.lit(None).cast(StringType()).alias("direct_manager"),
        u["job_title"].cast(StringType()).alias("job_title") if "job_title" in u.columns else F.lit(None).cast(StringType()).alias("job_title")
    )
    return fact


## Main runner

def main():
    parser = argparse.ArgumentParser(description="Build Silver Dimensions and Facts (Kimball Model)")
    parser.add_argument("--silver-base-path", default="s3a://lakehouse/silver/epm", help="Silver Parquet path")
    parser.add_argument("--spark-master", default="local[4]", help="Spark Master")
    args = parser.parse_args()

    spark = SparkSessionFactory.create(
        app_name="SilverDimsFactsETL",
        master=args.spark_master,
        enable_hive=False
    )

    logger.info("Loading Silver Base entities...")
    projects_df = load_silver_table(spark, args.silver_base_path, "epm_projects")
    tasks_df = load_silver_table(spark, args.silver_base_path, "epm_tasks")
    targets_df = load_silver_table(spark, args.silver_base_path, "epm_targets")
    objectives_df = load_silver_table(spark, args.silver_base_path, "epm_objectives")
    assignments_df = load_silver_table(spark, args.silver_base_path, "epm_c_assignments")
    access_log_df = load_silver_table(spark, args.silver_base_path, "epm_user_access_log")

    # 1. Build Dimensions
    dim_date = build_dim_date(spark)
    dim_dept = build_dim_department(projects_df, tasks_df, targets_df, objectives_df, assignments_df)
    dim_res = build_dim_resource(projects_df, tasks_df, targets_df, assignments_df, access_log_df)
    dim_proj = build_dim_project(projects_df)
    dim_task = build_dim_task(tasks_df)
    dim_obj = build_dim_objective(objectives_df)
    dim_asn = build_dim_assignment(assignments_df)

    save_silver_dataset(dim_date, args.silver_base_path, "dim_date")
    save_silver_dataset(dim_dept, args.silver_base_path, "dim_department")
    save_silver_dataset(dim_res, args.silver_base_path, "dim_resource")
    save_silver_dataset(dim_proj, args.silver_base_path, "dim_project")
    save_silver_dataset(dim_task, args.silver_base_path, "dim_task")
    save_silver_dataset(dim_obj, args.silver_base_path, "dim_objective")
    save_silver_dataset(dim_asn, args.silver_base_path, "dim_assignment")

    # 2. Build Facts
    fact_target = build_fact_target_bsc_snapshot(targets_df, assignments_df)
    fact_cvct = build_fact_cvct_execution_snapshot(targets_df)
    fact_task = build_fact_task_execution_snapshot(tasks_df)
    fact_proj = build_fact_project_progress_snapshot(projects_df)
    # fact_access = build_fact_epm_user_access(access_log_df)

    save_silver_dataset(fact_target, args.silver_base_path, "fact_target_bsc_snapshot", partition_cols=["snapshot_date"])
    save_silver_dataset(fact_cvct, args.silver_base_path, "fact_cvct_execution_snapshot", partition_cols=["snapshot_date"])
    save_silver_dataset(fact_task, args.silver_base_path, "fact_task_execution_snapshot", partition_cols=["snapshot_date"])
    save_silver_dataset(fact_proj, args.silver_base_path, "fact_project_progress_snapshot", partition_cols=["snapshot_date"])
    # save_silver_dataset(fact_access, args.silver_base_path, "fact_epm_user_access", partition_cols=["login_date"])

    logger.info("=== Finished Building Silver Dimensions & Facts (Option B) ===")
    spark.stop()


if __name__ == "__main__":
    main()