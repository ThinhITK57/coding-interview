"""
Spark 2.3.2 Transformation & Storage Pipeline Experiment.
Thực thi toàn bộ chu trình chuyển đổi dữ liệu trên PySpark 2.3.2:
1. Đọc Raw JSON từ Ingestion / Mock data
2. Làm phẳng đệ quy (JSONFlattener)
3. Phân luồng dữ liệu lỗi vào Dead Letter Queue (DeadLetterQueueRouter)
4. Khử trùng lặp Idempotent qua Window Ranking (DedupEngine)
5. Tạo Inferred Dimension Stub cho Kimball Race Condition (InferredDimensionRouter)
6. Ghi Parquet vào cấu trúc kho dữ liệu giả lập Trino:
   - hive.personal_raw.tasks
   - hive.global_clean.tasks (phân vùng dt=YYYY-MM-DD)
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

from transform import JSONFlattener, DedupEngine, InferredDimensionRouter
from storage.dlq_router import DLQRouter

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("SparkTransformExperiment")


def run_transform_experiment(
    raw_json_path: str = "./storage_data/raw_backup/tasks_raw.json",
    output_base_dir: str = "./storage_data/warehouse",
):
    print("=" * 80)
    print("⚡ KHỞI CHẠY SPARK 2.3.2 TRANSFORM & DEDUP EXPERIMENT")
    print(f"Raw Data: {raw_json_path}")
    print(f"Output Warehouse: {output_base_dir}")
    print("=" * 80)

    # 1. Khởi tạo Spark 2.3.2 Session cục bộ
    spark = (
        SparkSession.builder.appName("PlanviewEPM_Transform_Experiment")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(f"✅ Spark Session initialized! Version: {spark.version}")

    # 2. Đọc dữ liệu Raw JSON
    logger.info("Reading raw JSON dataset...")
    df_raw = spark.read.option("multiline", "true").json(raw_json_path)
    raw_count = df_raw.count()
    print(f"📊 Tổng số bản ghi thô nạp vào Spark: {raw_count} records")

    # 3. Làm phẳng JSON đệ quy (StructType unnesting)
    logger.info("Flattening nested JSON structs (State.id, Project.id, Parent.id)...")
    flattener = JSONFlattener()
    df_flattened = flattener.flatten(df_raw)
    print(f"✅ Đã làm phẳng schema. Số lượng cột sau khi unnest: {len(df_flattened.columns)}")

    # 4. Phân luồng dữ liệu lỗi (Quarantined DLQ)
    logger.info("Validating primary keys and routing corrupt records to DLQ...")
    dlq = DLQRouter(base_storage_dir=os.path.join(output_base_dir, "dlq"))

    df_valid = df_flattened.filter(F.col("id").isNotNull() & (F.col("id") != ""))
    df_invalid = df_flattened.filter(F.col("id").isNull() | (F.col("id") == ""))

    valid_count = df_valid.count()
    invalid_count = df_invalid.count()

    if invalid_count > 0:
        df_invalid_tagged = df_invalid.withColumn("_error_tags", F.array(F.lit("MISSING_PRIMARY_KEY_ID")))
        dlq_stats = dlq.route_dlq(
            dlq_df=df_invalid_tagged,
            table_name="tasks",
            batch_id="batch_exp_001",
        )
        print(f"⚠️ Đã cách ly {invalid_count} bản ghi lỗi vào DLQ: {dlq_stats}")
    else:
        print("✅ 100% bản ghi hợp lệ, không có dữ liệu rác.")

    # 5. Khử trùng lặp Idempotent bằng Spark 2.3.2 Window Ranking
    logger.info("Executing Idempotent Window Ranking Dedup (DedupEngine)...")
    dedup_engine = DedupEngine(primary_key="id", watermark_col="LastUpdatedOn")
    df_deduped, dedup_stats = dedup_engine.deduplicate(
        df=df_valid,
        primary_key="id",
        watermark_col="LastUpdatedOn",
    )
    clean_count = df_deduped.count()
    duplicates_removed = valid_count - clean_count
    print(f"🎯 Kết quả Dedup: Giữ lại {clean_count} bản ghi duy nhất, đã loại bỏ {duplicates_removed} bản ghi trùng!")
    print(f"   Dedup Engine Stats: {json.dumps(dedup_stats, indent=2)}")

    # 6. Xử lý Kimball Inferred Dimension Stubs
    logger.info("Detecting Inferred Dimension stubs for Projects / Parents...")
    proj_fk_col = "Project_id" if "Project_id" in df_deduped.columns else "Project"
    dim_router = InferredDimensionRouter()
    inferred_stubs = dim_router.generate_inferred_stubs(
        fact_df=df_deduped,
        fk_column=proj_fk_col,
        dim_pk_column="project_id",
        dim_name_column="project_name",
    )
    stubs_count = inferred_stubs.count() if inferred_stubs is not None else 0
    print(f"🏛️ Kimball Inferred Dimension Stubs generated: {stubs_count} project stubs")

    # 7. Ghi dữ liệu Parquet chuẩn Trino Warehouse
    logger.info("Writing clean Parquet outputs to Trino Warehouse directories...")
    today_str = datetime.utcnow().strftime("%Y-%m-%d")

    # DB 1: hive.personal_raw (Staging Parquet)
    personal_raw_path = os.path.join(output_base_dir, "hive", "personal_raw", "tasks")
    df_valid.write.mode("overwrite").parquet(personal_raw_path)
    print(f"📁 Ghi thành công Trino DB 1 (personal_raw): {personal_raw_path}")

    # DB 2: hive.global_clean (Curated Parquet partitioned by dt)
    global_clean_path = os.path.join(output_base_dir, "hive", "global_clean", "tasks")
    df_deduped_with_dt = df_deduped.withColumn("dt", F.lit(today_str))
    df_deduped_with_dt.write.mode("overwrite").partitionBy("dt").parquet(global_clean_path)
    print(f"📁 Ghi thành công Trino DB 2 (global_clean): {global_clean_path}")

    print("=" * 80)
    print("🎉 TOÀN BỘ CHU TRÌNH SPARK 2.3.2 TRANSFORM & STORAGE HOÀN TẤT XUẤT SẮC!")
    print(f"  - Input Raw Records:       {raw_count}")
    print(f"  - Quarantined to DLQ:      {invalid_count}")
    print(f"  - Duplicates Pruned:       {duplicates_removed}")
    print(f"  - Final Clean Output:      {clean_count}")
    print(f"  - Inferred Project Stubs:  {stubs_count}")
    print("=" * 80)

    spark.stop()
    return {
        "raw_count": raw_count,
        "invalid_count": invalid_count,
        "duplicates_removed": duplicates_removed,
        "clean_count": clean_count,
        "stubs_count": stubs_count,
    }


if __name__ == "__main__":
    raw_file = sys.argv[1] if len(sys.argv) > 1 else "./storage_data/raw_backup/tasks_raw.json"
    run_transform_experiment(raw_json_path=raw_file)
