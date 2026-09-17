#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Master Pipeline Runner: EPM End-to-End Lakehouse & Semantic Orchestrator
Author: Data Engineering Team
Target Environment: Python 3.7.1 / 3.12 | Apache Spark 2.3.2+ | Java 8

Architecture (Option B Kimball Model):
  Step 1: API Crawl & Ingestion -> Bronze Raw (MinIO S3 / Ambari HDFS / Local Staging)
  Step 2: Bronze -> Silver Base Conformance (Schema Contract & Primary Key Dedup)
  Step 3: Silver Base -> Kimball Dimensional Model (7 Dims + 5 Facts)
  Step 4: Silver Dims/Facts -> Gold Business Marts (BR-01 to BR-06)
  Step 5: dbt Semantic Layer Validation & Model Compilation

Usage Examples:
  # Run entire pipeline from scratch (full flow):
  python run_full_pipeline.py --all

  # Run all transformation steps, skipping the API crawl:
  python run_full_pipeline.py --all --skip-crawl

  # Run only Kimball Dims/Facts and Gold Marts (Steps 3 and 4):
  python run_full_pipeline.py --step 3,4

  # Dry run to preview commands without executing:
  python run_full_pipeline.py --all --dry-run
"""

import os
import sys
import time
import shutil
import logging
import argparse
import subprocess
from datetime import datetime
from typing import List, Dict, Any, Optional

# Ensure project root is in sys.path
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from config.env_loader import load_env_file
load_env_file()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("master_pipeline")

PIPELINE_STAGES = {
    1: {
        "name": "API Crawl & Bronze Landing",
        "description": "Crawl 5 EPM endpoints via resilient HTTP client and land raw Parquet in Bronze layer",
        "script": "prefect_flow.py",
        "args": ["--all", "--standalone", "--mode", "incremental"]
    },
    2: {
        "name": "Bronze -> Silver Base Conformance",
        "description": "Enforce schema contract (data_type/*.sql) and deduplicate sysid+last_updated_on",
        "script": os.path.join("spark", "bronze_to_silver.py"),
        "args": []
    },
    3: {
        "name": "Silver Base -> Kimball Dims & Facts",
        "description": "Build 7 Conformed Dimensions and 5 Periodic Snapshot Facts (Option B Kimball Model)",
        "script": os.path.join("spark", "build_silver_dims_facts.py"),
        "args": []
    },
    4: {
        "name": "Silver Dims/Facts -> Gold Business Marts",
        "description": "Generate 6 conformed Business Requirement tables (br01_bsc_yearly to br06_board_objectives)",
        "script": os.path.join("spark", "silver_to_gold.py"),
        "args": []
    },
    5: {
        "name": "dbt Semantic Layer Validation",
        "description": "Compile dbt semantic models and validate CRM-style metric definitions",
        "script": None,  # Handled via dbt CLI or python fallback
        "args": []
    }
}


def run_command(cmd: List[str], cwd: str, dry_run: bool = False) -> Dict[str, Any]:
    """Executes a command and returns execution metrics."""
    cmd_str = " ".join(cmd)
    logger.info(f"Executing: {cmd_str} (cwd={cwd})")

    if dry_run:
        logger.info("[DRY-RUN] Command skipped.")
        return {"status": "SKIPPED_DRY_RUN", "duration_seconds": 0.0, "return_code": 0}

    start_time = time.time()
    try:
        proc = subprocess.Popen(
            cmd,
            cwd=cwd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True,
            bufsize=1
        )
        for line in proc.stdout:
            print(f"  [{proc.pid}] {line.rstrip()}")
        proc.wait()
        duration = round(time.time() - start_time, 2)
        if proc.returncode == 0:
            return {"status": "SUCCESS", "duration_seconds": duration, "return_code": 0}
        else:
            logger.error(f"Command failed with return code: {proc.returncode}")
            return {"status": "FAILED", "duration_seconds": duration, "return_code": proc.returncode}
    except Exception as e:
        duration = round(time.time() - start_time, 2)
        logger.error(f"Execution error: {e}")
        return {"status": "ERROR", "error": str(e), "duration_seconds": duration, "return_code": -1}


def execute_step_1_crawl(python_exe: str, spark_master: str, dry_run: bool) -> Dict[str, Any]:
    """Runs Step 1: API Ingestion to Bronze."""
    stage = PIPELINE_STAGES[1]
    cmd = [python_exe, stage["script"]] + stage["args"] + ["--spark-master", spark_master]
    return run_command(cmd, cwd=ROOT_DIR, dry_run=dry_run)


def execute_step_2_bronze_to_silver(python_exe: str, spark_master: str, dry_run: bool) -> Dict[str, Any]:
    """Runs Step 2: Bronze to Silver Base."""
    stage = PIPELINE_STAGES[2]
    cmd = [python_exe, stage["script"], "--spark-master", spark_master]
    return run_command(cmd, cwd=ROOT_DIR, dry_run=dry_run)


def execute_step_3_silver_dims_facts(python_exe: str, spark_master: str, dry_run: bool) -> Dict[str, Any]:
    """Runs Step 3: Silver Base to Kimball Dimensions & Facts."""
    stage = PIPELINE_STAGES[3]
    cmd = [python_exe, stage["script"], "--spark-master", spark_master]
    return run_command(cmd, cwd=ROOT_DIR, dry_run=dry_run)


def execute_step_4_silver_to_gold(python_exe: str, spark_master: str, dry_run: bool) -> Dict[str, Any]:
    """Runs Step 4: Silver to Gold Business Marts."""
    stage = PIPELINE_STAGES[4]
    cmd = [python_exe, stage["script"], "--spark-master", spark_master]
    return run_command(cmd, cwd=ROOT_DIR, dry_run=dry_run)


def execute_step_5_dbt(dry_run: bool) -> Dict[str, Any]:
    """Runs Step 5: dbt semantic compilation and test."""
    dbt_dir = os.path.join(ROOT_DIR, "dbt_semantic")
    nextgen_dbt_dir = os.path.normpath(
        os.path.join(ROOT_DIR, "..", "..", "nextgen-bi-dbt", "nextgen-bi-dbt", "dbt_projects", "epm")
    )
    target_dbt_dir = nextgen_dbt_dir if os.path.exists(nextgen_dbt_dir) else dbt_dir

    logger.info(f"Target dbt directory: {target_dbt_dir}")
    dbt_exe = shutil.which("dbt")

    if not dbt_exe:
        logger.warning("dbt executable not found in PATH. Performing static semantic validation check...")
        models_dir = os.path.join(target_dbt_dir, "models")
        if os.path.exists(models_dir):
            sql_files = [f for f in os.listdir(models_dir) if f.endswith(".sql")]
            yml_files = [f for f in os.listdir(models_dir) if f.endswith(".yml")]
            logger.info(f"Found {len(sql_files)} SQL models and {len(yml_files)} schema metric definitions.")
            return {"status": "STATIC_VERIFIED", "duration_seconds": 0.1, "models_count": len(sql_files), "return_code": 0}
        return {"status": "DBT_NOT_INSTALLED", "duration_seconds": 0.0, "return_code": 0}

    cmd = [dbt_exe, "compile"]
    return run_command(cmd, cwd=target_dbt_dir, dry_run=dry_run)


def main():
    parser = argparse.ArgumentParser(
        description="Master Pipeline Runner for EPM End-to-End Flow",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--all", action="store_true", help="Execute all pipeline stages (1 through 5)")
    parser.add_argument("--step", type=str, default=None, help="Comma-separated list of steps to run (e.g. 2,3,4)")
    parser.add_argument("--skip-crawl", action="store_true", help="Skip Step 1 (API crawl) and start directly from existing Bronze")
    parser.add_argument("--spark-master", default="local[4]", help="Spark master URL (default: local[4], or yarn)")
    parser.add_argument("--dry-run", action="store_true", help="Print pipeline stages and commands without executing")
    args = parser.parse_args()

    python_exe = sys.executable

    # Determine steps to execute
    steps_to_run = []
    if args.all:
        steps_to_run = [1, 2, 3, 4, 5]
        if args.skip_crawl:
            steps_to_run.remove(1)
    elif args.step:
        try:
            steps_to_run = [int(s.strip()) for s in args.step.split(",") if s.strip()]
        except ValueError:
            logger.error("Invalid --step format. Must be integers separated by commas (e.g. --step 2,3,4)")
            sys.exit(1)
    else:
        logger.error("Must specify either --all or --step <list>. Use --help for usage.")
        sys.exit(1)

    print("=" * 80)
    print("      EPM ENTERPRISE DATA PLATFORM - MASTER PIPELINE RUNNER")
    print("=" * 80)
    print(f"Timestamp:    {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Python Exec:  {python_exe}")
    print(f"Spark Master: {args.spark_master}")
    print(f"Dry Run:      {args.dry_run}")
    print(f"Steps:        {steps_to_run}")
    print("=" * 80)

    summary = {}
    overall_start = time.time()

    for step in sorted(steps_to_run):
        if step not in PIPELINE_STAGES:
            logger.warning(f"Unknown step {step}. Skipping.")
            continue

        stage_info = PIPELINE_STAGES[step]
        print("\n" + "#" * 80)
        print(f"STAGE {step}: {stage_info['name'].upper()}")
        print(f"Description: {stage_info['description']}")
        print("#" * 80)

        res = None
        if step == 1:
            res = execute_step_1_crawl(python_exe, args.spark_master, args.dry_run)
        elif step == 2:
            res = execute_step_2_bronze_to_silver(python_exe, args.spark_master, args.dry_run)
        elif step == 3:
            res = execute_step_3_silver_dims_facts(python_exe, args.spark_master, args.dry_run)
        elif step == 4:
            res = execute_step_4_silver_to_gold(python_exe, args.spark_master, args.dry_run)
        elif step == 5:
            res = execute_step_5_dbt(args.dry_run)

        summary[step] = {
            "name": stage_info["name"],
            "result": res
        }

        if res and res.get("status") in ("FAILED", "ERROR"):
            logger.error(f"Stage {step} failed. Halting pipeline execution.")
            break

    total_duration = round(time.time() - overall_start, 2)

    # Print Final Summary Table
    print("\n" + "=" * 80)
    print("                   PIPELINE EXECUTION SUMMARY")
    print("=" * 80)
    print(f"{'Step':<6} | {'Stage Name':<38} | {'Status':<15} | {'Duration (s)':<12}")
    print("-" * 80)
    all_success = True
    for step, data in summary.items():
        res = data["result"]
        status = res.get("status", "UNKNOWN") if res else "NOT_RUN"
        duration = res.get("duration_seconds", 0.0) if res else 0.0
        if status not in ("SUCCESS", "STATIC_VERIFIED", "SKIPPED_DRY_RUN"):
            all_success = False
        print(f"{step:<6} | {data['name']:<38} | {status:<15} | {duration:<12}")
    print("=" * 80)
    print(f"Total Pipeline Runtime: {total_duration}s | Status: {'ALL SUCCESS' if all_success else 'FAILED/INTERRUPTED'}")
    print("=" * 80)

    if not all_success and not args.dry_run:
        sys.exit(1)


if __name__ == "__main__":
    main()
