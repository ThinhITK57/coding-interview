"""
Prefect 3.0 Orchestration Flow for Enterprise ELT Pipeline
Phối hợp thứ tự nạp giữa Dimension (muc-1) và Facts (muc-2, muc-3, muc-4).
"""

import os
import subprocess
from prefect import flow, task, get_run_logger

@task(retries=2, retry_delay_seconds=30, name="submit-spark-job")
def run_spark_elt_job(domain_target: str, env: str = "prod", dlq_threshold: float = 0.02):
    logger = get_run_logger()
    logger.info(f"Preparing spark-submit for domain target: {domain_target} in env: {env}")

    cmd = [
        "python", "-m", "enterprise_elt.main",
        "--env", env,
        "--domain-target", domain_target,
        "--dlq-threshold-ratio", str(dlq_threshold),
        "--rate-limit-rps", "10"
    ]

    logger.info(f"Executing command: {' '.join(cmd)}")
    
    env_vars = os.environ.copy()
    env_vars["PYTHONPATH"] = f".:{env_vars.get('PYTHONPATH', '')}"

    result = subprocess.run(cmd, capture_output=True, text=True, env=env_vars)

    if result.returncode != 0:
        logger.error(f"Spark job failed with error:\n{result.stderr}")
        raise RuntimeError(f"Job failed for {domain_target}: {result.stderr}")

    logger.info(f"Spark job completed successfully for {domain_target}!")
    return result.stdout

@flow(name="enterprise_elt_master_flow")
def master_elt_pipeline(env: str = "prod"):
    """
    Quy tắc điều phối bắt buộc:
    - BƯỚC 1: Nạp Dimension (muc-1) hoàn tất trước.
    - BƯỚC 2: Nạp các bảng Facts (muc-2, muc-3, muc-4) sau khi Dimension đã sẵn sàng.
    """
    logger = get_run_logger()
    logger.info(f"=== Starting Master ELT Pipeline for environment: {env} ===")

    # 1. Chạy Dimension trước
    dim_job = run_spark_elt_job(domain_target="muc-1", env=env, dlq_threshold=0.01)

    # 2. Chạy các Fact jobs (đợi dim_job hoàn tất)
    fact_2 = run_spark_elt_job(domain_target="muc-2", env=env, dlq_threshold=0.02, wait_for=[dim_job])
    fact_3 = run_spark_elt_job(domain_target="muc-3", env=env, dlq_threshold=0.02, wait_for=[dim_job])
    fact_4 = run_spark_elt_job(domain_target="muc-4", env=env, dlq_threshold=0.05, wait_for=[dim_job])

    logger.info("=== All domains processed successfully! ===")

if __name__ == "__main__":
    master_elt_pipeline(env="dev")
