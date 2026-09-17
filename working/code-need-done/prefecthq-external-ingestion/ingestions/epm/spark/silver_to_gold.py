"""
Spark ETL Pipeline: Silver (Cleaned Lakehouse Tables) -> Gold (6 Business Requirement Views / Tables)
Author: Data Engineering Team
Target Environment: Python 3.7.1 | Apache Spark 2.3.2 | Java 8

Strict Data Engineering Policy:
  - The Data Engineering team DOES NOT derive, invent, or hardcode calculated business measures
    (e.g., achievement_rate, gap, is_overdue, health_score, work_efficiency) into physical DW tables/views.
  - That responsibility belongs to Data Analysts via dbt Semantic Layer and GenBI metrics.
  - The 6 Gold DW Views contain ONLY and STRICTLY the exact conformed columns requested by the 6 Business Requirements (BRs).
"""

import os
import sys
import json
import logging
import argparse
from datetime import datetime

# Add root directory to sys.path for module resolution
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from config.env_loader import load_env_file
load_env_file()

from transform.spark_session import SparkSessionFactory
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType, DoubleType, DateType, IntegerType, LongType
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("silver_to_gold")


def safe_col(df, col_name, default_col=None, target_type="string"):
    """
    Safely retrieves a column if present; otherwise falls back to default_col or a typed NULL literal.
    Ensures Spark 2.3.2 compatibility without throwing AnalysisException.
    """
    if col_name in df.columns:
        c = F.col(col_name)
    elif default_col and default_col in df.columns:
        c = F.col(default_col)
    else:
        c = F.lit(None)

    if target_type == "string":
        return c.cast(StringType())
    elif target_type == "double":
        return c.cast(DoubleType())
    elif target_type == "date":
        return F.to_date(c)
    elif target_type == "integer":
        return c.cast(IntegerType())
    return c


def load_silver_table(spark, silver_base_path: str, table_name: str):
    """
    Loads a Silver layer Parquet table, with local fallback.
    """
    primary_path = f"{silver_base_path.rstrip('/')}/{table_name}"
    local_path = os.path.join(ROOT_DIR, "data", "silver", "epm", table_name)

    if not silver_base_path.startswith("./") and os.getenv("MINIO_ENDPOINT"):
        try:
            df = spark.read.parquet(primary_path)
            logger.info(f"Loaded {table_name} from S3: {primary_path}")
            return df
        except Exception as e:
            logger.warning(f"Could not load {primary_path} from S3: {e}. Checking local.")

    if os.path.exists(local_path):
        logger.info(f"Loaded {table_name} from local: {local_path}")
        return spark.read.parquet(local_path)

    # Fallback to empty DataFrame if not found
    logger.warning(f"Table {table_name} not found at {primary_path} or {local_path}. Returning empty DataFrame.")
    from pyspark.sql.types import StructType, StructField
    return spark.createDataFrame([], StructType([StructField("sysid", StringType(), True)]))


def build_br01_bsc_yearly(targets_df, objectives_df, assignments_df):
    """
    Business Requirement 1: Báo cáo BSC trong năm
    Strict Column Contract (17 columns):
      1. associated_objective (STRING)
      2. associated_item (STRING)
      3. target_type (STRING)
      4. parent_target (STRING)
      5. c_department (STRING)
      6. name (STRING)
      7. c_assignee (STRING)
      8. unit (STRING)
      9. target_date_m (DATE)
      10. target_value_m (DOUBLE)
      11. target_date_n (DATE)
      12. target_value_n (DOUBLE)
      13. state (STRING)
      14. status (STRING)
      15. target_result_m (DOUBLE)
      16. target_result_n (DOUBLE)
      17. assignor (STRING)
    """
    logger.info("Building BR-01: Báo cáo BSC trong năm...")

    # Base: Targets
    t = targets_df
    # Join with Assignments for assignor if available
    a = assignments_df.select(
        safe_col(assignments_df, "sysid").alias("asn_sysid"),
        safe_col(assignments_df, "c_assignor").alias("asn_assignor")
    ) if "sysid" in assignments_df.columns else None

    if a is not None and "c_associated_assignment" in t.columns:
        t = t.join(a, t["c_associated_assignment"] == a["asn_sysid"], how="left")
        assignor_col = F.coalesce(
            safe_col(t, "asn_assignor"),
            safe_col(t, "c_assignor"),
            safe_col(t, "created_by")
        )
    else:
        assignor_col = F.coalesce(
            safe_col(t, "c_assignor"),
            safe_col(t, "created_by")
        )

    res_m = F.coalesce(
        safe_col(t, "c_target_result_value_m", target_type="double"),
        safe_col(t, "c_target_result_m", target_type="double")
    )
    res_n = F.coalesce(
        safe_col(t, "c_target_result_value_n", target_type="double"),
        safe_col(t, "c_target_result_n", target_type="double")
    )

    br01_df = t.select(
        safe_col(t, "associated_objective", target_type="string").alias("associated_objective"),
        safe_col(t, "associated_item", target_type="string").alias("associated_item"),
        safe_col(t, "target_type", target_type="string").alias("target_type"),
        safe_col(t, "parent_target", target_type="string").alias("parent_target"),
        safe_col(t, "c_department", target_type="string").alias("c_department"),
        safe_col(t, "name", target_type="string").alias("name"),
        safe_col(t, "c_assignee", target_type="string").alias("c_assignee"),
        safe_col(t, "unit", target_type="string").alias("unit"),
        safe_col(t, "c_target_date_m", default_col="target_date", target_type="date").alias("target_date_m"),
        safe_col(t, "c_target_value_m", default_col="target_value", target_type="double").alias("target_value_m"),
        safe_col(t, "c_target_date_n", target_type="date").alias("target_date_n"),
        safe_col(t, "c_target_value_n", target_type="double").alias("target_value_n"),
        safe_col(t, "state", target_type="string").alias("state"),
        safe_col(t, "status", target_type="string").alias("status"),
        res_m.alias("target_result_m"),
        res_n.alias("target_result_n"),
        assignor_col.cast(StringType()).alias("assignor")
    )
    return br01_df


def build_br02_dieu_hanh_cvct_klcd(tasks_df, targets_df):
    """
    Business Requirement 2: Báo cáo điều hành CVCT/KLCĐ
    Strict Column Contract (8 columns):
      1. assignee (STRING)
      2. resources (STRING)
      3. name (STRING)
      4. status (STRING)
      5. due_date (DATE)
      6. percent_completed (DOUBLE)
      7. target_value_m (DOUBLE)
      8. target_date_m (DATE)
    """
    logger.info("Building BR-02: Báo cáo điều hành CVCT/KLCĐ...")

    # Primary source: targets (with M/N values) or union with tasks
    t = targets_df
    br02_df = t.select(
        safe_col(t, "c_assignee", default_col="entity_owner", target_type="string").alias("assignee"),
        safe_col(t, "resources_and_placeholders_count", target_type="string").alias("resources"),
        safe_col(t, "name", target_type="string").alias("name"),
        safe_col(t, "status", target_type="string").alias("status"),
        safe_col(t, "c_target_date_m", default_col="target_date", target_type="date").alias("due_date"),
        safe_col(t, "percent_completed", target_type="double").alias("percent_completed"),
        safe_col(t, "c_target_value_m", default_col="target_value", target_type="double").alias("target_value_m"),
        safe_col(t, "c_target_date_m", default_col="target_date", target_type="date").alias("target_date_m")
    )
    return br02_df


def build_br03_task_report(tasks_df):
    """
    Business Requirement 3: Báo cáo Task
    Strict Column Contract (15 columns):
      1. task_type (STRING)
      2. assignee (STRING)
      3. department (STRING)
      4. resources (STRING)
      5. parent_project (STRING)
      6. jira_status (STRING)
      7. name (STRING)
      8. description (STRING)
      9. work (DOUBLE)
      10. duration (DOUBLE)
      11. epm_default (STRING)
      12. start_date (DATE)
      13. due_date (DATE)
      14. percent_completed (DOUBLE)
      15. update_description (STRING)
    """
    logger.info("Building BR-03: Báo cáo Task...")

    t = tasks_df
    br03_df = t.select(
        safe_col(t, "task_type", target_type="string").alias("task_type"),
        safe_col(t, "manager", default_col="c_assignee", target_type="string").alias("assignee"),
        safe_col(t, "c_department", target_type="string").alias("department"),
        safe_col(t, "all_user_resources_count", default_col="resources_and_placeholders_count", target_type="string").alias("resources"),
        safe_col(t, "parent_project", target_type="string").alias("parent_project"),
        safe_col(t, "track_status", default_col="status", target_type="string").alias("jira_status"),
        safe_col(t, "name", target_type="string").alias("name"),
        safe_col(t, "description", target_type="string").alias("description"),
        safe_col(t, "work", target_type="double").alias("work"),
        safe_col(t, "duration", target_type="double").alias("duration"),
        safe_col(t, "c_epm_default", default_col="default_integration_path", target_type="string").alias("epm_default"),
        safe_col(t, "start_date", target_type="date").alias("start_date"),
        safe_col(t, "due_date", target_type="date").alias("due_date"),
        safe_col(t, "percent_completed", target_type="double").alias("percent_completed"),
        safe_col(t, "c_update_description", default_col="overview", target_type="string").alias("update_description")
    )
    return br03_df


def build_br04_project_report(projects_df):
    """
    Business Requirement 4: Báo cáo Dự án
    Strict Column Contract (11 columns):
      1. name (STRING)
      2. project_type (STRING)
      3. due_date (DATE)
      4. state (STRING)
      5. status (STRING)
      6. percent_completed (DOUBLE)
      7. department (STRING)
      8. assignor (STRING)
      9. assignee (STRING)
      10. project_manager (STRING)
      11. resources (STRING)
    """
    logger.info("Building BR-04: Báo cáo Dự án...")

    p = projects_df
    br04_df = p.select(
        safe_col(p, "name", target_type="string").alias("name"),
        safe_col(p, "c_internal_type", default_col="project_type", target_type="string").alias("project_type"),
        safe_col(p, "due_date", target_type="date").alias("due_date"),
        safe_col(p, "state", target_type="string").alias("state"),
        safe_col(p, "track_status", default_col="status", target_type="string").alias("status"),
        safe_col(p, "percent_completed", target_type="double").alias("percent_completed"),
        safe_col(p, "c_department", target_type="string").alias("department"),
        safe_col(p, "c_assignor", default_col="created_by", target_type="string").alias("assignor"),
        safe_col(p, "c_assignee", target_type="string").alias("assignee"),
        safe_col(p, "project_manager", default_col="manager", target_type="string").alias("project_manager"),
        safe_col(p, "c_action_resources", default_col="resources_and_placeholders_count", target_type="string").alias("resources")
    )
    return br04_df


def build_br05_user_access_traffic(access_log_df):
    """
    Business Requirement 5: Báo cáo Lưu lượng truy cập
    Strict Column Contract (7 columns):
      1. login_date (DATE)
      2. name (STRING)
      3. first_name (STRING)
      4. last_name (STRING)
      5. groups (STRING)
      6. direct_manager (STRING)
      7. job_title (STRING)
    """
    logger.info("Building BR-05: Báo cáo Lưu lượng truy cập...")

    u = access_log_df
    br05_df = u.select(
        safe_col(u, "login_date", default_col="login_timestamp", target_type="date").alias("login_date"),
        safe_col(u, "name", target_type="string").alias("name"),
        safe_col(u, "first_name", target_type="string").alias("first_name"),
        safe_col(u, "last_name", target_type="string").alias("last_name"),
        safe_col(u, "groups", target_type="string").alias("groups"),
        safe_col(u, "direct_manager", target_type="string").alias("direct_manager"),
        safe_col(u, "job_title", target_type="string").alias("job_title")
    )
    return br05_df


def build_br06_board_objectives(targets_df, objectives_df, assignments_df):
    """
    Business Requirement 6: Báo cáo Mục tiêu Ban Giám đốc (Mục tiêu BG)
    Strict Column Contract (17 columns):
      1. associated_objective (STRING)
      2. associated_item (STRING)
      3. target_type (STRING)
      4. parent_target (STRING)
      5. c_department (STRING)
      6. name (STRING)
      7. c_assignee (STRING)
      8. unit (STRING)
      9. c_target_date_m (DATE)
      10. c_target_date_n (DATE)
      11. c_target_value_m (DOUBLE)
      12. c_target_value_n (DOUBLE)
      13. c_target_result_m (DOUBLE)
      14. c_target_result_n (DOUBLE)
      15. state (STRING)
      16. status (STRING)
      17. c_assignor (STRING)
    """
    logger.info("Building BR-06: Báo cáo Mục tiêu Ban Giám đốc...")

    t = targets_df
    a = assignments_df.select(
        safe_col(assignments_df, "sysid").alias("asn_sysid"),
        safe_col(assignments_df, "c_assignor").alias("asn_assignor")
    ) if "sysid" in assignments_df.columns else None

    if a is not None and "c_associated_assignment" in t.columns:
        t = t.join(a, t["c_associated_assignment"] == a["asn_sysid"], how="left")
        assignor_col = F.coalesce(
            safe_col(t, "asn_assignor"),
            safe_col(t, "c_assignor"),
            safe_col(t, "created_by")
        )
    else:
        assignor_col = F.coalesce(
            safe_col(t, "c_assignor"),
            safe_col(t, "created_by")
        )

    res_m = F.coalesce(
        safe_col(t, "c_target_result_value_m", target_type="double"),
        safe_col(t, "c_target_result_m", target_type="double")
    )
    res_n = F.coalesce(
        safe_col(t, "c_target_result_value_n", target_type="double"),
        safe_col(t, "c_target_result_n", target_type="double")
    )

    br06_df = t.select(
        safe_col(t, "associated_objective", target_type="string").alias("associated_objective"),
        safe_col(t, "associated_item", target_type="string").alias("associated_item"),
        safe_col(t, "target_type", target_type="string").alias("target_type"),
        safe_col(t, "parent_target", target_type="string").alias("parent_target"),
        safe_col(t, "c_department", target_type="string").alias("c_department"),
        safe_col(t, "name", target_type="string").alias("name"),
        safe_col(t, "c_assignee", target_type="string").alias("c_assignee"),
        safe_col(t, "unit", target_type="string").alias("unit"),
        safe_col(t, "c_target_date_m", target_type="date").alias("c_target_date_m"),
        safe_col(t, "c_target_date_n", target_type="date").alias("c_target_date_n"),
        safe_col(t, "c_target_value_m", target_type="double").alias("c_target_value_m"),
        safe_col(t, "c_target_value_n", target_type="double").alias("c_target_value_n"),
        res_m.alias("c_target_result_m"),
        res_n.alias("c_target_result_n"),
        safe_col(t, "state", target_type="string").alias("state"),
        safe_col(t, "status", target_type="string").alias("status"),
        assignor_col.cast(StringType()).alias("c_assignor")
    )
    return br06_df


def save_gold_table(df, gold_base_path: str, table_name: str):
    """
    Saves a Gold Data Warehouse table as Parquet.
    """
    output_path = f"{gold_base_path.rstrip('/')}/{table_name}"
    local_output = os.path.join(ROOT_DIR, "data", "gold", "epm", table_name)

    if gold_base_path.startswith("./") or not os.getenv("MINIO_ENDPOINT"):
        output_path = local_output

    os.makedirs(os.path.dirname(local_output), exist_ok=True)

    count = df.count()
    logger.info(f"Writing Gold table {table_name} ({count} rows) -> {output_path}")

    (
        df.write
        .mode("overwrite")
        .format("parquet")
        .option("compression", "snappy")
        .save(output_path)
    )
    logger.info(f"Successfully written Gold table: {table_name}")
    return {"table": table_name, "count": count, "path": output_path}


def main():
    parser = argparse.ArgumentParser(description="Silver to Gold Spark ETL Job for 6 Business Requirements")
    parser.add_argument("--silver-path", default="s3a://lakehouse/silver/epm", help="Silver Parquet base path")
    parser.add_argument("--gold-path", default="s3a://lakehouse/gold/epm", help="Gold Parquet base path")
    parser.add_argument("--spark-master", default="local[4]", help="Spark Master URL")
    args = parser.parse_args()

    spark = SparkSessionFactory.create(
        app_name="SilverToGoldETL",
        master=args.spark_master,
        enable_hive=False
    )

    # 1. Load Silver Tables
    logger.info("Loading Silver tables...")
    tasks_df = load_silver_table(spark, args.silver_path, "epm_tasks")
    projects_df = load_silver_table(spark, args.silver_path, "epm_projects")
    targets_df = load_silver_table(spark, args.silver_path, "epm_targets")
    objectives_df = load_silver_table(spark, args.silver_path, "epm_objectives")
    assignments_df = load_silver_table(spark, args.silver_path, "epm_assignments")
    access_log_df = load_silver_table(spark, args.silver_path, "epm_user_access_log")

    # 2. Build 6 Clean Business Requirements
    br01 = build_br01_bsc_yearly(targets_df, objectives_df, assignments_df)
    br02 = build_br02_dieu_hanh_cvct_klcd(tasks_df, targets_df)
    br03 = build_br03_task_report(tasks_df)
    br04 = build_br04_project_report(projects_df)
    br05 = build_br05_user_access_traffic(access_log_df)
    br06 = build_br06_board_objectives(targets_df, objectives_df, assignments_df)

    # 3. Save to Gold Layer
    results = {}
    results["br01_bsc_yearly"] = save_gold_table(br01, args.gold_path, "br01_bsc_yearly")
    results["br02_dieu_hanh_cvct_klcd"] = save_gold_table(br02, args.gold_path, "br02_dieu_hanh_cvct_klcd")
    results["br03_task_report"] = save_gold_table(br03, args.gold_path, "br03_task_report")
    results["br04_project_report"] = save_gold_table(br04, args.gold_path, "br04_project_report")
    results["br05_user_access_traffic"] = save_gold_table(br05, args.gold_path, "br05_user_access_traffic")
    results["br06_board_objectives"] = save_gold_table(br06, args.gold_path, "br06_board_objectives")

    logger.info("=== Gold Processing Complete ===")
    for k, v in results.items():
        logger.info(f"  {k}: {v['count']} records saved to {v['path']}")

    spark.stop()


if __name__ == "__main__":
    main()
