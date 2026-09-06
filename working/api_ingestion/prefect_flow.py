"""
Prefect Flow: API Ingestion Pipeline

Orchestrates the full ELT pipeline from API extraction through Spark
transformation to data quality checks, with daily progress reporting.

Prefect HQ Features Showcased:
    - @flow / @task decorators with dependency tracking
    - Structured logging via get_run_logger()
    - Markdown Artifacts published to HQ dashboard
    - Task retries with configurable delays
    - Task caching with cache_key_fn
    - Concurrency limits to prevent API overload
    - Task tags for HQ filtering
    - Flow parameters for HQ UI triggering

Usage:
    # CLI
    python prefect_flow.py --env dev --endpoint tasks --mode full

    # Prefect deployment
    prefect deployment build prefect_flow.py:api_ingestion_pipeline \
        --name "clarizen-tasks-daily" --cron "0 2 * * *"
"""

import os
import sys
import json
import logging
import argparse
from datetime import timedelta

from prefect import flow, task, get_run_logger
from prefect.artifacts import create_markdown_artifact, create_table_artifact
from prefect.concurrency.sync import concurrency

from config.loader import ConfigLoader
from config.validator import ConfigValidator
from config.job_config import ExtractionMode
from ingestion.extractor import Extractor
from ingestion.writer import BatchWriter, PartitionedParquetWriter
from storage.tri_storage_sink import TriStorageSink
from storage.trino_ddl_generator import TrinoDDLGenerator
from transform.spark_session import SparkSessionFactory
from transform.json_flattener import JSONFlattener
from transform.docstring_registry import DocstringRegistry
from quality.validator import SchemaValidator
from quality.statistics import StatisticsProfiler
from quality.profiler import QualityReport
from monitoring.job_tracker import JobTracker
from monitoring.metrics import MetricsCollector

logger = logging.getLogger(__name__)


# ── Task: Load & Validate Configuration ──────────────────────────────

@task(
    name="load_config",
    tags=["config", "setup"],
    retries=0,
)
def load_and_validate_config(config_path="config.json", env="dev"):
    """Load and validate pipeline configuration.

    Args:
        config_path: Path to config.json.
        env: Environment name (dev, staging, prod).

    Returns:
        IngestionConfig: Validated configuration object.
    """
    log = get_run_logger()

    loader = ConfigLoader()
    config = loader.load(config_path)

    validator = ConfigValidator()
    config = validator.validate(config)

    log.info(
        "Config loaded and validated: api=%s, endpoints=%d, env=%s",
        config.api.name,
        len(config.api.endpoints),
        env,
    )

    return config


# ── Task: Extract from API ───────────────────────────────────────────

@task(
    name="extract_from_api",
    tags=["extraction", "api"],
    retries=2,
    retry_delay_seconds=[30, 120],
)
def extract_from_api(
    config,
    endpoint_name,
    staging_dir,
    window_start=None,
    window_end=None,
):
    """Extract paginated records from API endpoint to staging.

    Uses ResilientHTTPClient (rate limit + retry + circuit breaker)
    and CheckpointStore for crash-safe resume.

    Args:
        config: IngestionConfig.
        endpoint_name: Name of the endpoint to extract.
        staging_dir: Directory to write raw JSON batches.
        window_start: Optional ISO timestamp override for incremental start.
        window_end: Optional ISO timestamp override for incremental end.

    Returns:
        dict: Extraction summary with total_records, total_pages, client_stats.
    """
    log = get_run_logger()

    # Find endpoint config
    endpoint_config = None
    for ep in config.api.endpoints:
        if ep.name == endpoint_name:
            endpoint_config = ep
            break

    if endpoint_config is None:
        raise ValueError(f"Endpoint '{endpoint_name}' not found in config")

    # Use concurrency limit to prevent multiple flows hitting same API
    with concurrency(f"{config.api.name}-api", occupy=1):
        log.info("Acquired concurrency slot for %s API", config.api.name)

        extractor = Extractor(config)
        writer = BatchWriter(staging_dir)
        tri_sink = TriStorageSink(local_base_dir="./data")

        total_records = 0
        total_pages = 0
        last_batch = None
        all_raw_records = []

        for batch in extractor.iterate_batches(
            endpoint_config,
            window_start=window_start,
            window_end=window_end,
        ):
            writer.write_batch(batch)
            all_raw_records.extend(batch.records)
            total_records += batch.record_count
            total_pages += 1
            last_batch = batch

            log.info(
                "Page %d extracted: %d records (total: %d)",
                batch.page_number,
                batch.record_count,
                total_records,
            )

        if all_raw_records:
            backup_file = tri_sink.sink_minio_raw_backup(
                table_name=endpoint_name,
                raw_records=all_raw_records,
                batch_id=last_batch.batch_id if last_batch else "batch_init",
            )
            log.info("MinIO Raw Backup written: %s", backup_file)

    client_stats = extractor._client.get_stats()

    summary = {
        "endpoint": endpoint_name,
        "total_records": total_records,
        "total_pages": total_pages,
        "staging_dir": staging_dir,
        "client_stats": client_stats,
        "batch_id": last_batch.batch_id if last_batch else "",
        "window_start": last_batch.window_start if last_batch else window_start,
        "window_end": last_batch.window_end if last_batch else window_end,
    }

    log.info(
        "Extraction complete: %d records across %d pages",
        total_records,
        total_pages,
    )

    return summary


# ── Task: Spark Transform + Flatten JSON ─────────────────────────────

@task(
    name="transform_with_spark",
    tags=["transform", "spark"],
    retries=1,
    retry_delay_seconds=60,
)
def transform_with_spark(
    staging_dir,
    output_path,
    source_name,
    endpoint_name,
    api_version,
    batch_id="",
    spark_master="local[*]",
):
    """Read raw JSON, flatten nested structures, write partitioned Parquet.

    Also generates dbt schema.yml for genBI column documentation.

    Args:
        staging_dir: Directory containing raw JSON batch files.
        output_path: Target path for partitioned Parquet output.
        source_name: Data source name (e.g. "clarizen").
        endpoint_name: Endpoint name (e.g. "tasks").
        api_version: API version string.
        batch_id: Unique batch identifier.
        spark_master: Spark master URL.

    Returns:
        dict: Transform summary with record_count, output_path, schema_path.
    """
    log = get_run_logger()

    # Create SparkSession
    spark = SparkSessionFactory.create(
        app_name=f"api_ingestion_{source_name}_{endpoint_name}",
        master=spark_master,
    )

    try:
        # Read all JSON files from staging
        log.info("Reading JSON from staging: %s", staging_dir)
        df_raw = spark.read.json(staging_dir)

        if df_raw.rdd.isEmpty():
            log.warning("No data found in staging directory")
            return {"record_count": 0, "output_path": output_path}

        log.info(
            "Raw data loaded: %d columns, schema: %s",
            len(df_raw.columns),
            df_raw.schema.simpleString()[:200],
        )

        # Flatten nested JSON
        flattener = JSONFlattener()
        df_flat = flattener.flatten(df_raw)

        log.info(
            "Flattened: %d -> %d columns",
            len(df_raw.columns),
            len(df_flat.columns),
        )

        # Generate dbt schema.yml
        registry = DocstringRegistry(
            source_name=source_name,
            endpoint_name=endpoint_name,
            api_version=api_version,
        )
        registry.register_columns(df_flat, flattener.flatten_map)

        schema_path = os.path.join(
            os.path.dirname(output_path) or ".",
            f"schema_{source_name}_{endpoint_name}.yml",
        )
        registry.export_dbt_schema(schema_path)

        log.info("dbt schema.yml exported to: %s", schema_path)

        # Write partitioned Parquet
        writer = PartitionedParquetWriter(
            partition_columns=["_ingest_date"],
            compression="snappy",
            source_name=source_name,
        )
        write_result = writer.write(df_flat, output_path, batch_id=batch_id)

        log.info(
            "Parquet written: %d records to %s",
            write_result["record_count"],
            output_path,
        )

        # Tri-Storage Sinks: DB 1 (personal_raw) + DB 2 (global_clean) + Trino DDL
        tri_sink = TriStorageSink(
            spark_session=spark,
            local_base_dir="./data",
            trino_catalog="hive",
            personal_schema="personal_raw",
            clean_schema="global_clean",
            ddl_output_dir="./generated_ddl",
        )
        today_date = datetime.utcnow().strftime("%Y-%m-%d")

        # Sink to DB 1: personal_raw
        db1_path = tri_sink.sink_trino_personal_raw(
            table_name=endpoint_name,
            raw_df=df_raw,
            partition_col="ingest_date",
            partition_val=today_date,
        )

        # Sink to DB 2: global_clean
        db2_path = tri_sink.sink_trino_global_clean(
            table_name=endpoint_name,
            clean_df=df_flat,
            partition_col="ingest_date",
            partition_val=today_date,
        )

        # Generate DDL for trino.exe
        ddl_path = tri_sink.ddl_generator.export_sql_file(
            output_path="./generated_ddl",
            table_name=endpoint_name,
            schema_or_fields=df_flat.schema,
            partition_col="ingest_date",
        )
        log.info("Trino DDL generated for trino.exe: %s", ddl_path)

        return {
            "record_count": write_result["record_count"],
            "output_path": output_path,
            "db1_personal_raw_path": db1_path,
            "db2_global_clean_path": db2_path,
            "trino_ddl_file": ddl_path,
            "schema_path": schema_path,
            "columns": df_flat.columns,
        }

    finally:
        spark.stop()


# ── Task: Data Quality Checks ────────────────────────────────────────

@task(
    name="run_quality_checks",
    tags=["quality", "validation"],
)
def run_quality_checks(
    output_path,
    endpoint_name,
    batch_id="",
    spark_master="local[*]",
):
    """Run data quality validation and statistical profiling on written data.

    Args:
        output_path: Path to partitioned Parquet data.
        endpoint_name: Endpoint name for report labeling.
        batch_id: Batch identifier.
        spark_master: Spark master URL.

    Returns:
        QualityReport: Report with validation results and column statistics.
    """
    log = get_run_logger()

    spark = SparkSessionFactory.create(
        app_name=f"quality_check_{endpoint_name}",
        master=spark_master,
    )

    try:
        df = spark.read.parquet(output_path)

        if df.rdd.isEmpty():
            log.warning("No data to quality check at %s", output_path)
            return QualityReport(endpoint_name=endpoint_name)

        # Validate
        validator = SchemaValidator()
        # Add default not-null rules for common fields
        # (can be customized per endpoint via config later)
        for col_name in df.columns:
            if not col_name.startswith("_"):
                validator.add_rule(col_name, "not_null")

        valid_df, dlq_df = validator.validate(df)

        valid_count = valid_df.count()
        dlq_count = dlq_df.count() if dlq_df is not None else 0

        # Profile
        profiler = StatisticsProfiler()
        profile_result = profiler.profile(df)

        # Create report
        report = QualityReport.from_results(
            endpoint_name=endpoint_name,
            profile_result=profile_result,
            valid_count=valid_count,
            dlq_count=dlq_count,
            batch_id=batch_id,
        )

        report.log_summary()

        log.info(
            "Quality check complete: %d valid, %d DLQ out of %d total",
            valid_count,
            dlq_count,
            profile_result["total_records"],
        )

        return report

    finally:
        spark.stop()


# ── Task: Publish Reports to Prefect HQ ──────────────────────────────

@task(
    name="publish_daily_report",
    tags=["reporting", "prefect-hq"],
)
def publish_daily_report(job_summary, quality_report):
    """Publish daily ingestion report as Prefect Markdown Artifact.

    This report is visible directly on Prefect HQ dashboard under
    the flow run's Artifacts tab.

    Args:
        job_summary: Dict from JobTracker.finish().
        quality_report: QualityReport instance.
    """
    log = get_run_logger()

    tracker = JobTracker(
        job_name=job_summary.get("job_name", "api_ingestion"),
        endpoint_name=job_summary.get("endpoint", ""),
    )

    # Populate tracker from summary
    tracker._total_pages = job_summary.get("total_pages", 0)
    tracker._total_records_extracted = job_summary.get("total_records_extracted", 0)
    tracker._total_records_written = job_summary.get("total_records_written", 0)
    tracker._client_stats = job_summary.get("client_stats")
    tracker._start_timestamp = job_summary.get("start_time", "")
    tracker._quality_report = quality_report
    tracker._valid_records = quality_report.valid_records if quality_report else 0
    tracker._dlq_records = quality_report.dlq_records if quality_report else 0

    # Generate markdown report
    markdown_report = tracker.to_daily_report_markdown(job_summary)

    # Publish as Prefect Markdown Artifact
    create_markdown_artifact(
        key=f"daily-report-{job_summary.get('endpoint', 'unknown')}",
        markdown=markdown_report,
        description=f"Daily ingestion report for {job_summary.get('endpoint', '')}",
    )

    log.info("Daily report published to Prefect HQ Artifacts")

    # Also publish column stats as Prefect Table Artifact
    if quality_report and quality_report.column_stats:
        table_data = []
        for col_name, stats in quality_report.column_stats.items():
            table_data.append({
                "Column": col_name,
                "Null %": f"{stats.get('null_ratio', 0) * 100:.2f}%",
                "Distinct": stats.get("distinct_count", 0),
                "Min": str(stats.get("min", ""))[:30],
                "Max": str(stats.get("max", ""))[:30],
                "Mean": str(stats.get("mean", "")) if stats.get("mean") is not None else "—",
            })

        create_table_artifact(
            key=f"column-stats-{job_summary.get('endpoint', 'unknown')}",
            table=table_data,
            description=f"Column statistics for {job_summary.get('endpoint', '')}",
        )

        log.info("Column stats table published to Prefect HQ Artifacts")


# ── Main Flow ────────────────────────────────────────────────────────

@flow(
    name="api_ingestion_pipeline",
    description=(
        "Production API ingestion pipeline: extract from REST API with rate limiting, "
        "flatten nested JSON via Spark 2.3.2, write partitioned Parquet, "
        "validate data quality, and publish daily reports to Prefect HQ."
    ),
    retries=0,
)
def api_ingestion_pipeline(
    config_path: str = "config.json",
    endpoint_name: str = "muc_1",
    env: str = "dev",
    mode: str = "incremental",
    window_start: Optional[str] = None,
    window_end: Optional[str] = None,
    spark_master: str = "local[*]",
    dry_run: bool = False,
):
    """Main orchestration flow for API data ingestion.

    Prefect HQ Features Demonstrated:
        1. Flow parameters visible in HQ UI for manual triggering
        2. Task dependency graph visible in HQ flow run view
        3. Structured logs searchable in HQ
        4. Markdown + Table artifacts on HQ dashboard
        5. Concurrency limits prevent API overload
        6. Task retries with progressive delays
        7. Tags for filtering in HQ task runs

    Args:
        config_path: Path to pipeline config JSON.
        endpoint_name: API endpoint to extract from (e.g. muc_1, muc_2, muc_3, muc_4).
        env: Environment (dev, staging, prod).
        mode: Extraction mode (full, incremental).
        window_start: Optional ISO timestamp override for incremental start.
        window_end: Optional ISO timestamp override for incremental end.
        spark_master: Spark master URL.
        dry_run: If True, load config and validate but don't execute.
    """
    log = get_run_logger()
    log.info(
        "Pipeline started: endpoint=%s, env=%s, mode=%s, window=[%s, %s]",
        endpoint_name, env, mode, window_start, window_end,
    )

    # Initialize job tracker
    tracker = JobTracker(
        job_name=f"{env}_api_ingestion",
        endpoint_name=endpoint_name,
    )
    tracker.start()

    try:
        # Step 1: Load config
        config = load_and_validate_config(
            config_path=config_path,
            env=env,
        )

        if dry_run:
            log.info("Dry run mode: config validated, exiting")
            return {"status": "DRY_RUN", "config_valid": True}

        # Apply runtime mode & window overrides
        if mode:
            config.job.extraction.mode = ExtractionMode(mode)
        if window_start:
            config.job.extraction.window_start = window_start
        if window_end:
            config.job.extraction.window_end = window_end

        # Derive paths from config
        source_name = config.api.name
        api_version = config.api.version
        storage = config.job.storage

        staging_dir = os.path.join(
            "./data/staging", source_name, endpoint_name
        )
        output_path = storage.path or os.path.join(
            "./data/bronze", source_name, endpoint_name
        )

        # Step 2: Extract from API with window bounds
        extraction_result = extract_from_api(
            config=config,
            endpoint_name=endpoint_name,
            staging_dir=staging_dir,
            window_start=window_start,
            window_end=window_end,
        )

        tracker._total_pages = extraction_result["total_pages"]
        tracker._total_records_extracted = extraction_result["total_records"]
        tracker._client_stats = extraction_result.get("client_stats")

        if extraction_result["total_records"] == 0:
            log.warning("No records extracted, skipping transform")
            summary = tracker.finish(status="EMPTY")
            return summary

        # Step 3: Spark Transform + Flatten + Write Parquet
        transform_result = transform_with_spark(
            staging_dir=staging_dir,
            output_path=output_path,
            source_name=source_name,
            endpoint_name=endpoint_name,
            api_version=api_version,
            batch_id=extraction_result.get("batch_id", ""),
            spark_master=spark_master,
        )

        tracker.track_transform(transform_result.get("record_count", 0))

        # Step 4: Quality Checks
        quality_report = run_quality_checks(
            output_path=output_path,
            endpoint_name=endpoint_name,
            batch_id=extraction_result.get("batch_id", ""),
            spark_master=spark_master,
        )

        tracker.track_quality(quality_report)

        # Step 5: Finish tracking and publish reports
        summary = tracker.finish(status="SUCCESS")

        publish_daily_report(
            job_summary=summary,
            quality_report=quality_report,
        )

        log.info(
            "Pipeline complete: %d records extracted, %d written, %d DLQ",
            summary["total_records_extracted"],
            summary["total_records_written"],
            summary["dlq_records"],
        )

        return summary

    except Exception as e:
        log.error("Pipeline failed: %s", str(e))
        summary = tracker.finish(status="FAILED")

        # Still publish report on failure
        publish_daily_report(
            job_summary=summary,
            quality_report=tracker._quality_report,
        )

        raise


# ── CLI Entrypoint ───────────────────────────────────────────────────

def parse_args():
    """Parse command-line arguments for pipeline execution."""
    parser = argparse.ArgumentParser(
        description="API Ingestion Pipeline - Prefect Flow",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run extraction for tasks endpoint (dev)
  python prefect_flow.py --endpoint entity_query --env dev

  # Run all endpoints
  python prefect_flow.py --all --env prod

  # Dry run (validate config only)
  python prefect_flow.py --endpoint entity_query --dry-run

  # Custom Spark master
  python prefect_flow.py --endpoint entity_query --spark-master spark://master:7077
        """,
    )

    parser.add_argument(
        "--config-path",
        default="config.json",
        help="Path to pipeline config JSON (default: config.json)",
    )
    parser.add_argument(
        "--endpoint",
        default=None,
        help="Endpoint name to extract from (e.g. entity_query, tasks)",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        dest="all_endpoints",
        help="Extract from all configured endpoints",
    )
    parser.add_argument(
        "--env",
        default="dev",
        choices=["dev", "staging", "prod"],
        help="Environment (default: dev)",
    )
    parser.add_argument(
        "--mode",
        default="incremental",
        choices=["full", "incremental"],
        help="Extraction mode (default: incremental)",
    )
    parser.add_argument(
        "--window-start",
        default=None,
        help="ISO timestamp for incremental window start (e.g. 2026-09-05T00:00:00Z)",
    )
    parser.add_argument(
        "--window-end",
        default=None,
        help="ISO timestamp for incremental window end (e.g. 2026-09-06T00:00:00Z)",
    )
    parser.add_argument(
        "--spark-master",
        default="local[*]",
        help="Spark master URL (default: local[*])",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate config and exit without executing",
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    if args.all_endpoints:
        # Run pipeline for each configured endpoint
        loader = ConfigLoader()
        config = loader.load(args.config_path)
        for ep in config.api.endpoints:
            api_ingestion_pipeline(
                config_path=args.config_path,
                endpoint_name=ep.name,
                env=args.env,
                mode=args.mode,
                window_start=args.window_start,
                window_end=args.window_end,
                spark_master=args.spark_master,
                dry_run=args.dry_run,
            )
    elif args.endpoint:
        api_ingestion_pipeline(
            config_path=args.config_path,
            endpoint_name=args.endpoint,
            env=args.env,
            mode=args.mode,
            window_start=args.window_start,
            window_end=args.window_end,
            spark_master=args.spark_master,
            dry_run=args.dry_run,
        )
    else:
        print("Error: specify --endpoint NAME or --all")
        sys.exit(1)
