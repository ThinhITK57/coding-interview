import os
import sys
import logging

from datetime import datetime

from dotenv import load_dotenv

env_file = os.path.join(os.path.dirname(__file__), ".env.epm")
if os.path.exists(env_file):
    load_dotenv(env_file)
else:
    load_dotenv()

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
EPM_DIR = os.path.join(CURRENT_DIR, "epm")
if EPM_DIR not in sys.path:
    sys.path.insert(0, EPM_DIR)
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

from prefect import flow, serve
from epm.prefect_flow import api_ingestion_pipeline, api_ingestion_dag_pipeline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("epm_crawler_flow")
CONFIG_PATH = os.path.join(EPM_DIR, "config.json")


# ==============================================================================
# FLOWS CHÍNH
# ==============================================================================
@flow(name="[EPM] Crawl Endpoint Pipeline", log_prints=True)
def crawl_epm_endpoint(endpoint_name: str, mode: str = "incremental"):
    """Thực thi crawl cho 1 endpoint cụ thể (tasks, projects, targets, objectives, c_assignments)."""
    logger.info(f"Triggering EPM crawl: endpoint={endpoint_name}, mode={mode}")
    return api_ingestion_pipeline(
        config_path=CONFIG_PATH,
        endpoint_name=endpoint_name,
        env=os.getenv("ENV", "prod"),
        mode=mode,
        spark_master="local[2]"
    )


@flow(name="[EPM] Master DAG Daily Pipeline", log_prints=True)
def crawl_epm_master_dag():
    """Thực thi toàn bộ 5 endpoints theo thứ tự phụ thuộc (DAG)."""
    logger.info("Triggering EPM Master DAG Pipeline...")
    return api_ingestion_dag_pipeline(
        config_path=CONFIG_PATH,
        env=os.getenv("ENV", "prod"),
        mode="incremental",
        spark_master="local[2]"
    )


# ==============================================================================
# KHỞI CHẠY VÀ PHỤC VỤ (SERVE) 8 LỊCH TRÌNH CRON
# ==============================================================================
if __name__ == "__main__":
    logger.info("Configuring EPM Deployments matching crawl strategy...")
    # Lịch 1: Tasks - Incremental mỗi 2 giờ trong giờ hành chính (08:00 - 18:00, Thứ 2 đến Thứ 6)
    d_tasks_2h = crawl_epm_endpoint.to_deployment(
        name="[2-Hourly]-EPM Tasks Incremental",
        cron="0 8-18/2 * * 1-5",
        parameters={"endpoint_name": "tasks", "mode": "incremental"},
        tags=["production", "epm", "tasks", "incremental"]
    )
    # Lịch 2: Tasks - Full Sync đối soát cuối tuần (Chủ Nhật 23:00)
    d_tasks_weekly = crawl_epm_endpoint.to_deployment(
        name="[Weekly]-EPM Tasks Full Reconciliation",
        cron="0 23 * * 0",
        parameters={"endpoint_name": "tasks", "mode": "full"},
        tags=["production", "epm", "tasks", "full"]
    )
    # Lịch 3: Projects - Giữa ngày (12:00) & Cuối ngày (18:30)
    d_proj_midday = crawl_epm_endpoint.to_deployment(
        name="[Midday]-EPM Projects Incremental",
        cron="0 12 * * 1-5",
        parameters={"endpoint_name": "projects", "mode": "incremental"},
        tags=["production", "epm", "projects"]
    )
    d_proj_evening = crawl_epm_endpoint.to_deployment(
        name="[Evening]-EPM Projects Incremental",
        cron="30 18 * * 1-5",
        parameters={"endpoint_name": "projects", "mode": "incremental"},
        tags=["production", "epm", "projects"]
    )
    # Lịch 4: Targets - Giữa ngày (12:15) & Cuối ngày (18:45)
    d_target_midday = crawl_epm_endpoint.to_deployment(
        name="[Midday]-EPM Targets Incremental",
        cron="15 12 * * 1-5",
        parameters={"endpoint_name": "targets", "mode": "incremental"},
        tags=["production", "epm", "targets"]
    )
    d_target_evening = crawl_epm_endpoint.to_deployment(
        name="[Evening]-EPM Targets Incremental",
        cron="45 18 * * 1-5",
        parameters={"endpoint_name": "targets", "mode": "incremental"},
        tags=["production", "epm", "targets"]
    )
    # Lịch 5: Objectives - Full hàng đêm (01:00 AM)
    d_obj_daily = crawl_epm_endpoint.to_deployment(
        name="[Daily]-EPM Objectives Full",
        cron="0 1 * * *",
        parameters={"endpoint_name": "objectives", "mode": "full"},
        tags=["production", "epm", "objectives"]
    )
    # Lịch 6: Assignments (PGNV) - Full hàng đêm (01:30 AM)
    d_asn_daily = crawl_epm_endpoint.to_deployment(
        name="[Daily]-EPM Assignments Full",
        cron="30 1 * * *",
        parameters={"endpoint_name": "assignments", "mode": "full"},
        tags=["production", "epm", "assignments"]
    )
    # Lịch 7: User Access Log - Hàng đêm (02:00 AM)
    d_access_daily = crawl_epm_endpoint.to_deployment(
        name="[Daily]-EPM User Access Log",
        cron="0 2 * * *",
        parameters={"endpoint_name": "user_access_log", "mode": "incremental"},
        tags=["production", "epm", "access_log"]
    )
    # Lịch 8: Master DAG Pipeline - Chạy tổng hợp toàn bộ các làn phụ thuộc (03:00 AM)
    d_master_dag = crawl_epm_master_dag.to_deployment(
        name="[Daily]-EPM Master DAG Pipeline",
        cron="0 3 * * *",
        tags=["production", "epm", "master-dag"]
    )
    # Lịch 9: Ad-hoc - Cho phép trigger bằng tay bất kỳ lúc nào trên UI
    d_adhoc = crawl_epm_endpoint.to_deployment(
        name="[Adhoc]-EPM Manual Run Single Endpoint",
        parameters={"endpoint_name": "tasks", "mode": "incremental"},
        tags=["adhoc", "epm", "manual"]
    )
    logger.info("Serving all 9 EPM Deployments to Prefect Server...")
    serve(
        d_tasks_2h,
        d_tasks_weekly,
        d_proj_midday,
        d_proj_evening,
        d_target_midday,
        d_target_evening,
        d_obj_daily,
        d_asn_daily,
        d_access_daily,
        d_master_dag,
        d_adhoc
    )
