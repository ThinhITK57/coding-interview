"""
dbt Data Modeling & EVM Metrics Execution Engine.
Thực thi toàn bộ logic của dbt 3 tầng:
1. stg_planview__tasks: Bóc tách ID đối tượng, ép kiểu dữ liệu
2. int_tasks__evm_metrics: Tính toán công thức EVM chuẩn PMI (PV, EV, AC, CV, SV, CPI, SPI, EAC, ETC)
3. dim_tasks: Bảng chiều WBS
4. fct_task_daily_snapshot: Bảng fact ảnh chụp tiến độ hàng ngày

Đầu ra:
- Lưu trữ Parquet cho Trino Marts Layer
- In bảng Dashboard Preview hiển thị sức khỏe dự án và các chỉ số EVM
"""

import sys
import os
import json
import logging
from datetime import datetime

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("DBTExecutionEngine")


def run_dbt_simulation(
    curated_parquet_path: str = "./storage_data/warehouse/hive/global_clean/tasks",
    output_marts_dir: str = "./storage_data/warehouse/marts",
):
    print("=" * 80)
    print("🏛️ KHỞI CHẠY DBT DATA MODELING & EVM METRICS PIPELINE")
    print(f"Source: {curated_parquet_path}")
    print(f"Target Marts: {output_marts_dir}")
    print("=" * 80)

    spark = (
        SparkSession.builder.appName("PlanviewEPM_dbt_Execution")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # 1. Đọc Curated Parquet
    logger.info("Step 1: Reading Curated Parquet from Trino global_clean...")
    df_curated = spark.read.parquet(curated_parquet_path)
    print(f"✅ Loaded {df_curated.count()} records from Trino curated layer.")

    # 2. Tầng Staging: stg_planview__tasks
    logger.info("Step 2: Transforming to Staging View (stg_planview__tasks)...")
    df_stg = (
        df_curated
        .withColumnRenamed("id", "task_id")
        .withColumnRenamed("sysid", "task_code")
        .withColumnRenamed("name", "task_name")
        .withColumnRenamed("description", "task_description")
        .withColumnRenamed("parent_id", "parent_task_id")
        .withColumn("is_milestone", F.col("milestone").cast("boolean"))
        .withColumn("is_on_critical_path", F.col("on_critical_path").cast("boolean"))
        .withColumn("planned_start_date", F.col("start_date").cast("timestamp"))
        .withColumn("planned_due_date", F.col("due_date").cast("timestamp"))
        .withColumn("planned_budget", F.coalesce(F.col("planned_budget"), F.lit(0.0)).cast("double"))
        .withColumn("actual_cost", F.coalesce(F.col("actual_cost"), F.lit(0.0)).cast("double"))
        .withColumn("percent_completed", F.coalesce(F.col("percent_completed"), F.lit(0.0)).cast("double"))
        .withColumn("expected_progress", F.coalesce(F.col("expected_progress"), F.lit(0.0)).cast("double"))
        .withColumn("budgeted_work_hours", F.coalesce(F.col("budgeted_hours"), F.col("work"), F.lit(0.0)).cast("double"))
        .withColumn("actual_effort_hours", F.coalesce(F.col("actual_effort"), F.lit(0.0)).cast("double"))
        .withColumn("remaining_effort_hours", F.coalesce(F.col("remaining_effort"), F.lit(0.0)).cast("double"))
        .withColumn("track_status", F.coalesce(F.col("track_status"), F.lit("Unknown")))
        .withColumn("issues_count", F.coalesce(F.col("issues_count"), F.lit(0)).cast("int"))
    )

    # 3. Tầng Intermediate: int_tasks__evm_metrics (Chuẩn PMI)
    logger.info("Step 3: Calculating EVM Metrics (PV, EV, AC, CV, SV, CPI, SPI, EAC)...")
    df_evm = (
        df_stg
        # Planned Value (PV)
        .withColumn(
            "planned_value_pv",
            F.when(F.col("expected_progress") > 0, F.round(F.col("planned_budget") * (F.col("expected_progress") / 100.0), 2))
            .otherwise(F.col("planned_budget"))
        )
        # Earned Value (EV)
        .withColumn(
            "earned_value_ev",
            F.when(F.col("percent_completed") > 0, F.round(F.col("planned_budget") * (F.col("percent_completed") / 100.0), 2))
            .otherwise(F.lit(0.0))
        )
        # Actual Cost (AC)
        .withColumn("actual_cost_ac", F.col("actual_cost"))
        # Cost Variance (CV) = EV - AC
        .withColumn("cost_variance_cv", F.round(F.col("earned_value_ev") - F.col("actual_cost_ac"), 2))
        # Schedule Variance (SV) = EV - PV
        .withColumn("schedule_variance_sv", F.round(F.col("earned_value_ev") - F.col("planned_value_pv"), 2))
        # Cost Performance Index (CPI) = EV / AC
        .withColumn(
            "cpi",
            F.when(F.col("actual_cost_ac") > 0, F.round(F.col("earned_value_ev") / F.col("actual_cost_ac"), 3))
            .when(F.col("earned_value_ev") > 0, F.lit(1.0))
            .otherwise(F.lit(None).cast("double"))
        )
        # Schedule Performance Index (SPI) = EV / PV
        .withColumn(
            "spi",
            F.when(F.col("planned_value_pv") > 0, F.round(F.col("earned_value_ev") / F.col("planned_value_pv"), 3))
            .when(F.col("earned_value_ev") > 0, F.lit(1.0))
            .otherwise(F.lit(None).cast("double"))
        )
        # Estimate At Completion (EAC) = Planned Budget / CPI
        .withColumn(
            "estimate_at_completion_eac",
            F.when(F.col("cpi") > 0, F.round(F.col("planned_budget") / F.col("cpi"), 2))
            .otherwise(F.col("planned_budget"))
        )
        # Health Status Tagging
        .withColumn(
            "evm_health_status",
            F.when(F.col("percent_completed") >= 100.0, F.lit("COMPLETED"))
            .when(F.col("is_on_critical_path") & ((F.col("spi") < 0.9) | (F.col("cpi") < 0.9)), F.lit("CRITICAL_DELAY"))
            .when((F.col("spi") < 0.85) | (F.col("cpi") < 0.85), F.lit("HIGH_RISK"))
            .when((F.col("spi") < 1.0) | (F.col("cpi") < 1.0), F.lit("AT_RISK"))
            .otherwise(F.lit("ON_TRACK"))
        )
    )

    # 4. Tầng Marts Core: dim_tasks
    logger.info("Step 4: Materializing Marts Core: dim_tasks...")
    dim_tasks = df_evm.select(
        "task_id",
        "task_code",
        "task_name",
        "project_id",
        "parent_task_id",
        "manager_id",
        "state_id",
        "is_milestone",
        "is_on_critical_path",
        "planned_start_date",
        "planned_due_date",
        "budgeted_work_hours",
        "planned_budget",
    )
    dim_tasks_path = os.path.join(output_marts_dir, "core", "dim_tasks")
    dim_tasks.write.mode("overwrite").parquet(dim_tasks_path)
    print(f"📁 Materialized dim_tasks -> {dim_tasks_path} ({dim_tasks.count()} rows)")

    # 5. Tầng Marts EVM: fct_task_daily_snapshot
    logger.info("Step 5: Materializing Marts EVM: fct_task_daily_snapshot...")
    today_date = datetime.utcnow().strftime("%Y-%m-%d")
    fct_snapshot = df_evm.select(
        F.lit(today_date).cast("date").alias("snapshot_date"),
        "task_id",
        "task_code",
        "project_id",
        "state_id",
        "track_status",
        "evm_health_status",
        "is_milestone",
        "is_on_critical_path",
        "percent_completed",
        "planned_value_pv",
        "earned_value_ev",
        "actual_cost_ac",
        "cost_variance_cv",
        "schedule_variance_sv",
        "cpi",
        "spi",
        "estimate_at_completion_eac",
        "budgeted_work_hours",
        "actual_effort_hours",
        "remaining_effort_hours",
        "issues_count",
    )
    fct_snapshot_path = os.path.join(output_marts_dir, "evm", "fct_task_daily_snapshot")
    fct_snapshot.write.mode("overwrite").partitionBy("snapshot_date").parquet(fct_snapshot_path)
    print(f"📁 Materialized fct_task_daily_snapshot -> {fct_snapshot_path} ({fct_snapshot.count()} rows)")

    # 6. In Bảng Dashboard Preview
    print("\n" + "=" * 100)
    print("🎯 BẢNG PREVIEW DỮ LIỆU FINAL: 5 BẢN GHI SNAPSHOT TIẾN ĐỘ & EVM (CHUẨN PMI)")
    print("=" * 100)
    sample_rows = fct_snapshot.limit(5).collect()
    for row in sample_rows:
        r = row.asDict()
        print(
            f"📌 [{r['task_code']}] {r['task_id'][:20]}... | Critical: {r['is_on_critical_path']} | "
            f"Progress: {r['percent_completed']}% | Health: {r['evm_health_status']}"
        )
        print(
            f"   💰 Budget(PV): ${r['planned_value_pv']:,.0f} | Earned(EV): ${r['earned_value_ev']:,.0f} | "
            f"Actual(AC): ${r['actual_cost_ac']:,.0f}"
        )
        print(
            f"   📈 CV: ${r['cost_variance_cv']:,.0f} | SV: ${r['schedule_variance_sv']:,.0f} | "
            f"CPI: {r['cpi']} | SPI: {r['spi']} | EAC: ${r['estimate_at_completion_eac']:,.0f}"
        )
        print("-" * 100)

    print("=" * 100)
    print("🎉 DBT TRANSFORMATION ĐÃ HOÀN THÀNH TOÀN BỘ LOGIC KHO DỮ LIỆU!")
    spark.stop()


if __name__ == "__main__":
    curated = sys.argv[1] if len(sys.argv) > 1 else "./storage_data/warehouse/hive/global_clean/tasks"
    run_dbt_simulation(curated_parquet_path=curated)
