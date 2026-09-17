import os
import sys
import json
import logging
import argparse
from contextlib import contextmanager
from datetime import timedelta, datetime
from typing import Optional, List, Dict, Any
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel
from config.env_loader import load_env_file
load_env_file()

STANDALONE_MODE = (
    "--standalone" in sys.argv
    or "--no-prefect" in sys.argv
    or os.environ.get("PREFECT_STANDALONE", "").lower() in ("1", "true", "yes")
)

HAS_PREFECT = False

if not STANDALONE_MODE:
    try:
        from prefect import flow, task, get_run_logger
        HAS_PREFECT = True
    except ImportError:
        HAS_PREFECT = False

if not HAS_PREFECT:
    def flow(*args, **kwargs):
        def decorator(fn):
            fn.fn = fn
            return fn
        if args and callable(args[0]):
            args[0].fn = args[0]
            return args[0]
        return decorator

    def task(*args, **kwargs):
        def decorator(fn):
            fn.fn = fn
            return fn
        if args and callable(args[0]):
            args[0].fn = args[0]
            return args[0]
        return decorator

    def get_run_logger():
        return logging.getLogger("prefect_flow")

else:
    _raw_get_run_logger = get_run_logger
    def get_run_logger():
        try:
            return _raw_get_run_logger()
        except Exception:
            return logging.getLogger("prefect_flow")

def create_markdown_artifact(key=None, markdown=None, description=None):
    if HAS_PREFECT:
        try:
            from prefect.artifacts import create_markdown_artifact as _prefect_create_md
            return _prefect_create_md(key=key, markdown=markdown, description=description)
        except Exception as e:
            logging.getLogger("prefect_flow").warning("Prefect markdown artifact not published: %s", e)
    return None

def create_table_artifact(key=None, table=None, description=None):
    if HAS_PREFECT:
        try:
            from prefect.artifacts import create_table_artifact as _prefect_create_tbl
            return _prefect_create_tbl(key=key, table=table, description=description)
        except Exception as e:
            logging.getLogger("prefect_flow").warning("Prefect table artifact not published : %s", e)
    return None

@contextmanager
def safe_concurrency(name, occupy=1):
    if HAS_PREFECT:
        try:
            from prefect.concurrency.sync import concurrency as _prefect_concurrency
            with _prefect_concurrency(name, occupy=occupy):
                yield
            return
        except Exception:
            pass
    yield

concurrency = safe_concurrency


from config.loader import ConfigLoader, resolve_endpoint_name
from config.validator import ConfigValidator
from config.job_config import ExtractionMode
from ingestion.extractor import Extractor
from ingestion.writer import BatchWriter, PartitionedParquetWriter

from storage.tri_storage_sink import TriStorageSink
from storage.trino_ddl_generator import TrinoDDLGenerator
from storage.dlq_router import DLQRouter

from transform.spark_session import SparkSessionFactory
from transform.json_flattener import JSONFlattener
from transform.docstring_registry import DocstringRegistry
from transform.dedup_engine import DedupEngine
from transform.race_condition_router import InferredDimensionRouter
from transform.genbi_context_packer import GenBIContextPacker
from transform.schema_contract import SchemaContract
from transform.contract_conformer import read_conformed, assert_matches_contract

from quality.validator import SchemaValidator
from quality.statistics import StatisticsProfiler
from quality.profiler import QualityReport

from monitoring.job_tracker import JobTracker
from monitoring.metrics import MetricsCollector

# try:
#     from prefect import flow, task, get_run_logger
# except ImportError:
#     def flow(*args, **kwargs):
#         def decorator(fn):
#             return fn
#         if args and callable(args[0]):
#             return args[0]
#         return decorator
#     def task(*args, **kwargs):
#         def decorator(fn):
#             return fn
#         if args and callable(args[0]):
#             return args[0]
#         return decorator
#     def get_run_logger():
#         return logging.getLogger("prefect_flow")

#     def create_markdown_artifact(key=None, markdown=None, description=None):
#         pass

#     def create_table_artifact(key=None, table=None, description=None):
#         pass


logger = logging.getLogger(__name__)

@task(
    name="load_config",
    tags=["config", "setup"],
    retries=0
)
def load_and_validate_config(config_path="config.json", env="dev", endpoint_name=None):
    log = get_run_logger()
    loader = ConfigLoader()
    config = loader.load(config_path)

    for ep in config.api.endpoints:
        if ep.name == "tasks":
            logger.info(
                "RAW CONFIG TASK FIELDS: %s",
                ep.body.get("fields", [])
            )

            logger.info(
                "RAW CONFIG HAS CHI: %s",
                "Chi" in ep.body.get("fields", [])
            )

            break

    # Doi chieu voi danh sach endpoint THAT trong config thay vi mot bang alias
    # viet cung - bang do lech moi lan doi ten endpoint
    available = [ep.name for ep in config.api.endpoints]
    wanted = endpoint_name or config.job.endpoint
    if wanted:
        resolved = resolve_endpoint_name(wanted, config.api.endpoints)
        if resolved is None:
            raise ValueError(
                f"Endpoint '{wanted}' khong co trong {config_path}. "
                f"Cac endpoint hien co: {', '.join(available)}"
            )
        if resolved != wanted:
            log.info("Endpoint '%s' -> '%s'", wanted, resolved)
        config.job.endpoint = resolved

    validator = ConfigValidator()
    config = validator.validate(config)

    log.info(
        "Config loaded and validated : api=%s , endpoint=%d, env=%s",
        config.api.name,
        len(config.api.endpoints),
        env,
    )

    return config

@task(
    name="extract_from_api",
    tags=["extraction", "api"],
    retries=2,
    retry_delay_seconds=[30, 120]
)
def extract_from_api(
    config, endpoint_name, staging_dir, window_start=None, window_end=None
):
    """return Extraction summary with total records, total pages, client_stats"""
    log = get_run_logger()

    endpoint_config = None
    for ep  in config.api.endpoints:
        if ep.name == endpoint_name:
            endpoint_config = ep
            break

    if endpoint_config is None:
        raise ValueError(f"Endpoint '{endpoint_name}' not found in config")

    with concurrency(f"{config.api.name}-api", occupy=1):
        log.info("Acquired concurrency slot for %s API ", config.api.name)

        extractor = Extractor(config)
        writer = BatchWriter(staging_dir)
        tri_sink = TriStorageSink(local_base_dir='./data')

        total_records = 0
        total_pages = 0
        last_batch = None
        all_raw_records = []


        for batch in extractor.iterate_batches(
            endpoint_config, window_start=window_start, window_end=window_end
        ):
            writer.write_batch(batch)
            all_raw_records.extend(batch.records)
            total_records += batch.record_count
            total_pages += 1
            last_batch = batch


            log.info(
                "Page %d extracted : %d records (total : %d)",
                batch.page_number,
                batch.record_count,
                total_records
            )

        if all_raw_records:
            backup_file = tri_sink.sink_minio_raw_backup(
                table_name=endpoint_name,
                raw_records=all_raw_records,
                batch_id=last_batch.batch_id if last_batch else "batch_init"
            )
            log.info("MinIO Raw backup written %s", backup_file)

    client_stats = extractor._client.get_stats()

    summary = {
        "endpoint": endpoint_name,
        "total_records": total_records,
        "total_pages": total_pages,
        "staging_dir": staging_dir,
        "client_stats": client_stats,
        "batch_id": last_batch.batch_id if last_batch else "",
        "window_start": last_batch.window_start if last_batch else window_start,
        "window_end": last_batch.window_end if last_batch else window_end
    }

    log.info(
        "Extraction complete :%d records across %d pages",
        total_records, total_pages
    )

    return summary


@task(
    name="transform_with_spark",
    tags=["transform", "spark"],
    retries=1,
    retry_delay_seconds=60
)
def transform_with_spark(
    staging_dir,
    output_path,
    source_name,
    endpoint_name,
    api_version,
    batch_id="",
    spark_master="local[*]"
):
    log = get_run_logger()

    spark = SparkSessionFactory.create(
        app_name=f"api_ingestion_{source_name}_{endpoint_name}",
        master=spark_master
    )

    try:
        log.info("Reading JSON from staging : %s", staging_dir)

        contract = (
            SchemaContract.load(endpoint_name)
            if SchemaContract.has_contract(endpoint_name)
            else None
        )

        if contract is not None:
            # Doc bang schema tuong minh tu data_type/<bang>_dataType.sql.
            # Khong de Spark infer: ket qua infer doi theo du lieu tung batch
            # (cot toan null -> void, so nguyen -> bigint) nen Parquet lech dan
            # khoi DDL Trino va vo khi query xuyen partition.
            log.info(
                "Contract '%s': %d cot - bo qua buoc infer schema",
                endpoint_name, len(contract)
            )
            df_flat = read_conformed(spark, staging_dir, contract)

            corrupt_col = SchemaContract.CORRUPT_COL
            if corrupt_col in df_flat.columns:
                # Phai tham chieu it nhat mot cot thuong trong cung query: Spark
                # cam query chi doc moi cot corrupt record tu file JSON tho.
                probe = contract.names[0]
                counts = df_flat.agg(
                    F.count(F.col(probe)).alias("probe_non_null"),
                    F.sum(
                        F.when(F.col(corrupt_col).isNotNull(), 1).otherwise(0)
                    ).alias("corrupt")
                ).collect()[0]

                if counts["corrupt"]:
                    log.warning(
                        "%d ban ghi khong parse duoc theo contract - API co the da doi shape",
                        counts["corrupt"]
                    )
                df_flat = df_flat.drop(corrupt_col)

            # personal_raw va global_clean dung chung schema contract, khac nhau
            # o cho global_clean da dedup
            df_raw = df_flat
            flatten_map = {c.name: c.src_name for c in contract.columns}
        else:
            log.warning(
                "Khong co contract data_type cho '%s' - quay ve infer schema",
                endpoint_name
            )
            df_raw = spark.read.json(staging_dir)
            flattener = JSONFlattener()
            df_flat = flattener.flatten(df_raw)
            flatten_map = flattener.flatten_map
            log.info(
                "Flattened: %d --> %d columns",
                len(df_raw.columns),
                len(df_flat.columns)
            )

        log.info(
            "df_flat partitions=%d, columns=%d",
            df_flat.rdd.getNumPartitions(),
            len(df_flat.columns)
        )

        if df_flat.limit(1).count() == 0:
            log.warning("No data found in staging directory")
            return {"record_count": 0, "output_path": output_path}


        # Idempotent dedup
        dedup_engine = DedupEngine(primary_key="sysid", watermark_col="last_updated_on")
        df_clean, dedup_stats = dedup_engine.deduplicate(df_flat)

        if contract is not None:
            # Chan som: schema lech contract thi dung o day, dung de lot xuong
            # Parquet roi moi vo luc query tren Trino
            assert_matches_contract(df_clean, contract)

        log.info(
            "df_clean partitions=%d, columns=%d",
            df_clean.rdd.getNumPartitions(),
            len(df_clean.columns)
        )

        log.info(
            "Dedup complete : %d input --> %d unique records (%d duplicates removed)",
            dedup_stats["input_count"],
            dedup_stats["deduped_count"],
            dedup_stats["duplicates_removed"]
        )

        if "c_project" in df_clean.columns:
            inferred_router = InferredDimensionRouter()
            stubs = inferred_router.generate_inferred_stubs(
                fact_df=df_clean,
                fk_column="c_project",
                dim_pk_column="sysid",
                dim_name_column="name"
            )
            if stubs is not None:
                dim_path = os.path.join("./data/warehouse", "global_clean", "projects")
                inferred_router.reconcile_dimension(
                    stubs_df=stubs,
                    dim_table_path=dim_path,
                    spark_session=spark,
                    dedup_engine=dedup_engine
                )
                log.info("Reconciled inferred dimension stubs into %s", dim_path)

        registry = DocstringRegistry(
            source_name=source_name,
            endpoint_name=endpoint_name,
            api_version=api_version
        )
        registry.register_columns(df_clean, flatten_map)

        schema_dir = "./generated_schema"
        os.makedirs(schema_dir, exist_ok=True)
        schema_path = os.path.join(
            schema_dir,
            f"schema_{source_name}_{endpoint_name}.yml"
        )

        registry.export_dbt_schema(schema_path)
        log.info("dbt schema.yml exported to : %s", schema_path)

        packer = GenBIContextPacker(trino_catalog="hive", clean_schema="global_clean")
        context_pack_dir = "./generated_context"
        os.makedirs(context_pack_dir, exist_ok=True)
        context_pack_path = os.path.join(
            context_pack_dir,
            f"genbi_context_pack_{endpoint_name}.json"
        )
        sample_records = [r.asDict() for r in df_clean.limit(3).collect()]
        packer.export_pack(
            output_path=context_pack_path,
            table_name=endpoint_name,
            columns_meta=registry._columns,
            table_description=f"Curated {endpoint_name} dataset from {source_name} API",
            sample_rows=sample_records
        )

        log.info("GenBI Context Pack for AI .. to : %s", context_pack_path)

        writer = PartitionedParquetWriter(
            partition_columns=["ingest_date"],
            compression="snappy",
            source_name=source_name
        )

        # ================================
        # S3A RUNTIME DEBUG
        # ================================
        hconf = spark.sparkContext._jsc.hadoopConfiguration()

        test_path = "s3a://lakehouse/prefect_s3a_debug"

        try:
            jvm = spark.sparkContext._jvm

            path = jvm.org.apache.hadoop.fs.Path(test_path)
            fs = path.getFileSystem(hconf)

            log.info("Testing FileSystem.exists(): %s", test_path)

            exists = fs.exists(path)

            log.info("FileSystem.exists() RESULT = %s", exists)

        except Exception as e:
            log.exception("S3A EXISTS TEST FAILED: %s", e)

        try:
            log.info("Testing S3A WRITE: %s", test_path)

            (
                spark.createDataFrame(
                    [(1, "prefect_test")],
                    ["id", "value"]
                )
                .write
                .mode("overwrite")
                .parquet(test_path)
            )

            log.info("========== PREFECT S3A WRITE SUCCESS ==========")

        except Exception as e:
            log.exception("S3A WRITE TEST FAILED: %s", e)

        log.info("===============================================")

        write_result = writer.write(df_clean, output_path, batch_id=batch_id)
        log.info(
            "Parquet written: %d records to %s",
            write_result["record_count"],
            output_path
        )
        # WAREHOUSE_URI      : noi Spark ghi parquet (mac dinh MinIO)
        # DDL_LOCATION_BASE  : duong dan ghi trong DDL - la cho tren Ambari sau
        #                      khi file duoc tai ve tu MinIO va upload tay len
        tri_sink = TriStorageSink(
            spark_session=spark,
            local_base_dir="./data",
            trino_catalog="hive",
            personal_schema="personal_raw",
            clean_schema="global_clean",
            ddl_output_dir="./generated_ddl",
            warehouse_uri=os.getenv("WAREHOUSE_URI", "s3a://lakehouse/warehouse"),
            ddl_location_base=os.getenv("DDL_LOCATION_BASE") or None,
            partitioned=os.getenv("ENABLE_PARTITIONING", "").lower() in ("1", "true", "yes")
        )

        today_date = datetime.utcnow().strftime("%Y-%m-%d")

        # Sink to DB personal
        db1_path = tri_sink.sink_trino_personal_raw(
            table_name=endpoint_name,
            raw_df=df_raw,
            partition_col="ingest_date",
            partition_val=today_date
        )

        # Sink to DB global. _raw_payload chi thuoc ve personal_raw - DDL cua
        # global_clean khong khai bao cot nay, giu lai chi ton dung mot ban
        # payload thu hai ma Trino khong doc den.
        db2_path = tri_sink.sink_trino_global_clean(
            table_name=endpoint_name,
            clean_df=df_clean.drop(SchemaContract.RAW_PAYLOAD_COL),
            partition_col="ingest_date",
            partition_val=today_date
        )

        # Sinh ca hai dialect tu cung contract: Spark SQL de chay tren
        # Zeppelin, Trino cho job dong bo phia sau
        ddl_paths = tri_sink.export_ddl(
            table_name=endpoint_name,
            fallback_df=df_clean,
            partition_col="ingest_date"
        )
        ddl_path = ddl_paths["trino_ddl"]
        log.info("Spark SQL DDL (Zeppelin): %s", ddl_paths["spark_ddl"])
        log.info("Trino DDL generated for trino exe : %s", ddl_path)

        return {
            "record_count": write_result["record_count"],
            "output_path": output_path,
            "db1_personal_raw_path": db1_path,
            "db2_global_clean_path": db2_path,
            "trino_ddl_file": ddl_path,
            "schema_path": schema_path,
            "genbi_context_pack_path": context_pack_path,
            "columns": df_clean.columns
        }

    finally:
        spark.stop()


@task(
    name="run_quality_checks",
    tags=["quality", "validation"]
)
def run_quality_checks(
    output_path, 
    endpoint_name,
    batch_id="",
    spark_master="local[*]"
):
    log = get_run_logger()
    spark = SparkSessionFactory.create(
        app_name=f"quality_check_{endpoint_name}",
        master=spark_master
    )

    try:
        df = spark.read.parquet(output_path)
        if batch_id:
            df = df.filter(F.col("_batch_id") == batch_id)

        df = df.persist(StorageLevel.MEMORY_AND_DISK)

        if df.limit(1).count() == 0:
            log.warning(
                "No data to quality check at %s",
                output_path
            )
            return QualityReport(
                endpoint_name=endpoint_name
            )

        # if df.rdd.isEmpty():
        #     log.warning("No data to quality check at %s", output_path)
        #     return QualityReport(endpoint_name=endpoint_name)

        validator = SchemaValidator()
        # quality_config = config.get("quality", {})
        # rules = quality_config.get("rules", [])

        # # for col_name in df.columns:
        # #     if not col_name.startswith("_"):
        # #         validator.add_rule(col_name, "not_null")
        # for rule in rules:
        #     validator.add_rule(
        #         rule["column"],
        #         rule["rule"],
        #         **{
        #             k: v
        #             for k, v in rule.items()
        #             if k not in ("column", "rule")
        #         }
        #     )
        critical_columns = [ "SYSID", "Name", "LastUpdatedOn" ]
        column_map = { col_name.lower(): col_name for col_name in df.columns } 
        for critical_column in critical_columns: 
            actual_column = column_map.get( critical_column.lower() ) 
            if actual_column: 
                validator.add_rule( actual_column, "not_null" ) 
            else: 
                log.warning( 
                    "Critical column '%s' not found in endpoint '%s'; " "NOT NULL validation skipped", 
                    critical_column, 
                    endpoint_name
                    )


        valid_df, dlq_df = validator.validate(df)
        valid_count = valid_df.count()
        dlq_count = 0
        if dlq_df is not None:
            dlq_router = DLQRouter(base_storage_dir="./data")
            dlq_stats = dlq_router.route_dlq(
                dlq_df=dlq_df,
                table_name=endpoint_name,
                batch_id=batch_id
            )
            dlq_count = dlq_stats.get("dlq_count", 0)
            log.warning(
                "Quarantined %d records into DLQ (%s): %s",
                dlq_count,
                dlq_stats.get("dlq_path"),
                dlq_stats.get("error_breakdown")
            )

        profiler = StatisticsProfiler()
        profile_result = profiler.profile(df)

        report = QualityReport.from_results(
            endpoint_name=endpoint_name, 
            profile_result=profile_result,
            valid_count=valid_count, 
            dlq_count=dlq_count,
            batch_id=batch_id
        )
        report.log_summary()

        log.info(
            "Quality check complete: %d valid, %d DLQ out of %d total",
            valid_count, dlq_count, profile_result["total_records"]
        )
        return report
    finally:
        try:
            df.unpersist()
        except Exception:
            pass
        spark.stop()


@task(
    name="publish_daily_report",
    tags=["reporting", "prefect-hq"]
)
def publish_daily_report(job_summary, quality_report):
    log = get_run_logger()

    tracker = JobTracker(
        job_name=job_summary.get("job_name", "api_ingestion"),
        endpoint_name=job_summary.get("endpoint", "")
    )

    tracker._total_pages = job_summary.get("total_pages", 0)
    tracker._total_records_extracted = job_summary.get("total_records_extracted", 0)
    tracker._total_records_written = job_summary.get("total_records_written", 0)
    tracker._client_stats = job_summary.get("client_stats")
    tracker._start_timestamp = job_summary.get("start_time", "")
    tracker._quality_report = quality_report
    tracker._valid_records = quality_report.valid_records if quality_report else 0
    tracker._dlq_records = quality_report.dlq_records if quality_report else 0


    markdown_report = tracker._to_daily_report_markdown(job_summary)

    try:
        reports_dir = "./data/reports"
        os.makedirs(reports_dir, exist_ok=True)
        local_report_path = os.path.join(
            reports_dir, f"daily_report_{job_summary.get('endpoint', 'unknown')}.md"
        )
        with open(local_report_path, "w", encoding="utf-8") as f:
            f.write(markdown_report)
        log.info("Local markdown report save to : %s", local_report_path)
    except Exception as e:
        log.warning("Could not save local markdown report : %s", e)
    
    try:
        create_markdown_artifact(
            key=f"daily-report-{job_summary.get('endpoint', 'unknown')}",
            markdown=markdown_report,
            description=f"Daily ingestion report for {job_summary.get('endpoint', '')}"
        )
        log.info("Daily report publised to Prefect HQ")
    except Exception as e:
        log.warning("Could not publish daily report to Prefect HQ: %s", e)

    if quality_report and quality_report.column_stats:
        table_data = []
        for col_name, stats in quality_report.column_stats.items():
            table_data.append({
                "Column": col_name,
                "Null %": f"{stats.get('null_ratio', 0) * 100:.2f}%",
                "Distinct": stats.get("distinct_count", 0),
                "Min": str(stats.get("min", ""))[:30],
                "Max": str(stats.get("max", ""))[:30],
                "Mean": str(stats.get("mean", "")) if stats.get("mean") is not None else "-"
            })
        try:
            create_table_artifact(
                key=f"column-stats-{job_summary.get('endpoint', 'unknown')}",
                table=table_data,
                description=f"Column statistics for {job_summary.get('endpoint', '')}"
            )
            log.info("Column stats table published to Prefect HQ artifact")
        except Exception as e:
            log.warning("Could not publish column stats to Prefec HQ: %s", e)


@flow(
    name="api_ingestion_pipeline",
    description=(
        """"""
    ),
    retries=0
)
def api_ingestion_pipeline(
    config_path: str = "config.json",
    endpoint_name: str = "Task",
    env: str = "dev",
    mode: str = "incremental",
    window_start: Optional[str] = None,
    window_end: Optional[str] = None,
    spark_master: str = "local[*]",
    dry_run: bool = False
):
    log = get_run_logger()
    log.info(
        "Pipeline started endpoint=%s , env=%s , mode=%s , window=[%s, %s]", 
        endpoint_name, env, mode, window_start, window_end
    )
    tracker = JobTracker(
        job_name=f"{env}_api_ingestion",
        endpoint_name=endpoint_name
    )
    tracker.start()
    logger.info(
        "CONFIG PATH ABSOLUTE: %s",
        os.path.abspath(config_path)
    )
    try:
        config = load_and_validate_config(
            config_path=config_path, 
            env=env, endpoint_name=endpoint_name
        )

        if dry_run:
            log.info("Dry run mode: config validated, exiting")
            return {"status": "DRY_RUN", "config_valid": True}

        if mode:
            config.job.extraction.mode = ExtractionMode(mode)

        if window_start:
            config.job.extraction.window_start = window_start
        if window_end:
            config.job.extraction.window_end = window_end

        source_name = config.api.name
        api_version = config.api.version
        storage = config.job.storage

        staging_dir = os.path.join(
            "./data/staging", source_name, endpoint_name
        )

        if storage.path:
            output_path = (storage.path.rstrip("/") + "/" + endpoint_name.lower())
        else:
            output_path = os.path.join("./data/bronze", source_name, endpoint_name)    

        extraction_result = extract_from_api(
            config=config,
            endpoint_name=endpoint_name,
            staging_dir=staging_dir,
            window_start=window_start,
            window_end=window_end
        )

        tracker._total_pages = extraction_result["total_pages"]
        tracker._total_records_extracted = extraction_result["total_records"]
        tracker._client_stats = extraction_result.get("client_stats")

        if extraction_result["total_records"] == 0:
            log.warning("No records extracted , skipping transform")
            summary = tracker.finish(status="EMPTY")
            return summary

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

        quality_report = run_quality_checks(
            output_path=output_path,
            endpoint_name=endpoint_name,
            batch_id=extraction_result.get("batch_id", ""),
            spark_master=spark_master
        )

        tracker.track_quality(quality_report)
        summary = tracker.finish(status="SUCCESS")

        publish_daily_report(
            job_summary=summary,
            quality_report=quality_report
        )
        log.info(
            "Pipeline complete: %d records extracted, %d written, %d DLQ",
            summary["total_records_extracted"],
            summary["total_records_written"],
            summary["dlq_records"]
        )

        return summary
    except Exception as e:
        log.error("Pipeline failed : %s", str(e))
        summary = tracker.finish(status="FAILED")

        publish_daily_report(
            job_summary=summary,
            quality_report=tracker._quality_report
        )
        raise


def resolve_dag_execution_waves(
        endpoint_names: List[str],
        registry_path: str = "tables_registry.json"
)-> List[List[str]]:
    dependencies = {ep: [] for ep in endpoint_names}
    if os.path.exists(registry_path):
        try:
            with open(registry_path, "r", encoding="utf-8") as f:
                reg = json.load(f)

            # Ten endpoint trong config khong phai luc nao cung trung table_name
            # trong registry ('objective' vs 'bsc', 'c_assignments' vs
            # 'assignments'). Doi chieu qua aliases, neu khong cac phu thuoc se
            # bi loai bo am tham va thu tu chay sai.
            to_endpoint = {}
            for t in reg.get("tables", []):
                keys = [t.get("table_name")] + list(t.get("aliases") or [])
                for ep in endpoint_names:
                    if ep.lower() in {str(k).lower() for k in keys if k}:
                        to_endpoint[str(t.get("table_name")).lower()] = ep
                        for k in keys:
                            if k:
                                to_endpoint[str(k).lower()] = ep

            for t in reg.get("tables", []):
                owner = to_endpoint.get(str(t.get("table_name")).lower())
                if owner is None:
                    continue
                deps = []
                for d in t.get("depends_on", []):
                    mapped = to_endpoint.get(str(d).lower())
                    if mapped and mapped != owner and mapped not in deps:
                        deps.append(mapped)
                dependencies[owner] = deps

            unmatched = [
                t.get("table_name") for t in reg.get("tables", [])
                if to_endpoint.get(str(t.get("table_name")).lower()) is None
            ]
            if unmatched:
                logger.warning(
                    "Registry co bang khong khop endpoint nao: %s", unmatched
                )
        except Exception as e:
            logger.warning("Failed to parse dependencies from %s: %s", registry_path, str(e))

    waves = []
    resolved = set()

    remaining = set(endpoint_names)
    while remaining:
        current_wave = [ep for ep in remaining if all(dep in resolved for dep in dependencies.get(ep, []))]
        if not current_wave:
            waves.append(sorted(list(remaining)))
            break
        current_wave.sort()
        waves.append(current_wave)
        for ep in current_wave:
            resolved.add(ep)
            remaining.remove(ep)

    return waves


@flow(
    name="api_ingestion_dag_pipeline",
    description="",
    retries=0
)
def api_ingestion_dag_pipeline(
    config_path: str = "config.json",
    env: str = "dev", 
    mode: str = "incremental",
    window_start: Optional[str] = None,
    window_end: Optional[str] = None,
    spark_master: str = "local[*]",
    dry_run: bool = False
):
    log = get_run_logger()
    log.info("Starting Master Dag orchestration pipeline")

    loader = ConfigLoader()
    config = loader.load(config_path)

    endpoint_names = [ep.name for ep in config.api.endpoints]
    registry_path = os.path.join(os.path.dirname(config_path) or ".", "tables_registry.json")
    waves = resolve_dag_execution_waves(endpoint_names, registry_path)
    print("Enpoint in ", waves)
    overall_results = {}

    for wave_num, wave in enumerate(waves, 1):
        for ep_name in wave:
            res = api_ingestion_pipeline(
                config_path=config_path,
                endpoint_name=ep_name,
                env=env,
                mode=mode,
                window_start=window_start,
                window_end=window_end,
                spark_master=spark_master,
                dry_run=dry_run
            )
            overall_results[ep_name] = res
    return overall_results


def parse_args():
    parser = argparse.ArgumentParser(
        description="",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--config-path", default="config.json"
    )
    parser.add_argument(
        "--endpoint", default=None
    )
    parser.add_argument(
        "--all", action="store_true", dest="all_endpoints"
    )
    parser.add_argument(
        "--env", default="dev", choices=["dev", "staging", "prod"],
    )
    parser.add_argument(
        "--mode", default="incremental", choices=["initial", "incremental", "backfill", "full"]
    )

    parser.add_argument(
        "--window-start", default=None
    )

    parser.add_argument(
        "--window-end", default=None
    )

    parser.add_argument(
        "--spark-master", default="local[4]"
    )

    parser.add_argument(
        "--dry-run", action="store_true"
    )

    parser.add_argument(
        "--standalone", 
        "--no-prefect",
        action="store_true",
        dest="standalone",
    )
    return parser.parse_args()

if __name__ == '__main__':
    args = parse_args()

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    runner_log = logging.getLogger("pipeline runner")
    if args.standalone:
        runner_log.info("Execution mode: Standalone")

    try:
        if args.all_endpoints:
            api_ingestion_dag_pipeline(
                config_path=args.config_path,
                env=args.env,
                mode=args.mode,
                window_start=args.window_start,
                window_end=args.window_end,
                spark_master=args.spark_master,
                dry_run=args.dry_run
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
                dry_run=args.dry_run
            )
        
        else:
            print("Error: specify --endpoint Name or --all")
            sys.exit(1)
    except Exception as e:
        runner_log.error("Pipeline run failed with [%s]: %s", type(e).__name__, str(e))
        sys.exit(1)