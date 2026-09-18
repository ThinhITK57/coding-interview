import os
import sys
import argparse
import logging
from typing import List

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

from prefect import serve
from prefect.client.schemas.schedules import CronSchedule

from epm.prefect_flow import (
    crawl_epm_endpoint,
    crawl_epm_master_dag,
    background_backfill_flow,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
    force=True,
)
logger = logging.getLogger("epm_crawler_flow")


def get_deployments():
    """Configures the enterprise EPM deployments adhering strictly to CRAWL-DATA-PLAN.md."""

    # 1. Tasks - Incremental mỗi 2 giờ (08:00 - 18:00, Thứ 2 đến Thứ 6)
    d_tasks_2h = crawl_epm_endpoint.to_deployment(
        name="[2-Hourly]-EPM Tasks Incremental",
        description="2-Hourly incremental extraction for tasks (Mon-Fri 08:00 - 18:00)",
        schedules=[CronSchedule(cron="0 8,10,12,14,16,18 * * 1-5")],
        parameters={"endpoint_name": "tasks", "mode": "incremental"},
        tags=["production", "epm", "tasks", "incremental"],
    )

    # 2. Tasks - Full Sync đối soát cuối tuần (Chủ Nhật 02:00 / 23:00)
    d_tasks_weekly = crawl_epm_endpoint.to_deployment(
        name="[Weekly]-EPM Tasks Full Reconciliation",
        description="Weekly full reconciliation crawl for tasks (Sunday 02:00)",
        schedules=[CronSchedule(cron="0 2 * * 0")],
        parameters={"endpoint_name": "tasks", "mode": "full"},
        tags=["production", "epm", "tasks", "full"],
    )

    # 3. Projects - Giữa ngày (12:00) & Cuối ngày (18:30)
    d_proj_midday = crawl_epm_endpoint.to_deployment(
        name="[Midday]-EPM Projects Incremental",
        description="Midday incremental extraction for projects (12:00)",
        schedules=[CronSchedule(cron="0 12 * * *")],
        parameters={"endpoint_name": "projects", "mode": "incremental"},
        tags=["production", "epm", "projects"],
    )
    d_proj_evening = crawl_epm_endpoint.to_deployment(
        name="[Evening]-EPM Projects Incremental",
        description="Evening incremental extraction for projects (18:30)",
        schedules=[CronSchedule(cron="30 18 * * *")],
        parameters={"endpoint_name": "projects", "mode": "incremental"},
        tags=["production", "epm", "projects"],
    )

    # 4. Targets - Giữa ngày (12:30) & Cuối ngày (19:00)
    d_target_midday = crawl_epm_endpoint.to_deployment(
        name="[Midday]-EPM Targets Incremental",
        description="Midday incremental extraction for targets (12:30)",
        schedules=[CronSchedule(cron="30 12 * * *")],
        parameters={"endpoint_name": "targets", "mode": "incremental"},
        tags=["production", "epm", "targets"],
    )
    d_target_evening = crawl_epm_endpoint.to_deployment(
        name="[Evening]-EPM Targets Incremental",
        description="Evening incremental extraction for targets (19:00)",
        schedules=[CronSchedule(cron="0 19 * * *")],
        parameters={"endpoint_name": "targets", "mode": "incremental"},
        tags=["production", "epm", "targets"],
    )

    # 5. Objectives - Full hàng đêm (03:30 AM)
    d_obj_daily = crawl_epm_endpoint.to_deployment(
        name="[Daily]-EPM Objectives Full",
        description="Daily full extraction for BSC objectives (03:30 AM)",
        schedules=[CronSchedule(cron="30 3 * * *")],
        parameters={"endpoint_name": "objectives", "mode": "full"},
        tags=["production", "epm", "objectives"],
    )

    # 6. Assignments - Full hàng đêm (03:00 AM)
    d_asn_daily = crawl_epm_endpoint.to_deployment(
        name="[Daily]-EPM Assignments Full",
        description="Daily full extraction for project assignments (03:00 AM)",
        schedules=[CronSchedule(cron="0 3 * * *")],
        parameters={"endpoint_name": "assignments", "mode": "full"},
        tags=["production", "epm", "assignments"],
    )

    # 7. Master DAG Pipeline - Chạy tổng hợp theo chuỗi phụ thuộc (04:00 AM)
    d_master_dag = crawl_epm_master_dag.to_deployment(
        name="[Daily]-EPM Master DAG Pipeline",
        description="Consolidated sequential DAG: projects -> tasks -> targets -> objectives -> assignments (04:00 AM)",
        schedules=[CronSchedule(cron="0 4 * * *")],
        tags=["production", "epm", "master-dag"],
    )

    # 8. Background Backfill Worker - Quét và đẩy bù buffer định kỳ mỗi 30 phút
    d_backfill = background_backfill_flow.to_deployment(
        name="[Background]-EPM Buffer Backfill",
        description="Decoupled background worker draining local fallback buffers to MinIO",
        schedules=[CronSchedule(cron="*/30 * * * *")],
        tags=["production", "epm", "backfill", "maintenance"],
    )

    # 9. Ad-hoc - Cho phép trigger bằng tay bất kỳ lúc nào trên UI
    d_adhoc = crawl_epm_endpoint.to_deployment(
        name="[Adhoc]-EPM Manual Run Single Endpoint",
        description="On-demand manual trigger for any EPM endpoint and mode",
        parameters={"endpoint_name": "tasks", "mode": "incremental"},
        tags=["adhoc", "epm", "manual"],
    )

    return [
        d_tasks_2h,
        d_tasks_weekly,
        d_proj_midday,
        d_proj_evening,
        d_target_midday,
        d_target_evening,
        d_obj_daily,
        d_asn_daily,
        d_master_dag,
        d_backfill,
        d_adhoc,
    ]


def main():
    parser = argparse.ArgumentParser(description="EPM Crawler Prefect Serve Runner")
    parser.add_argument("--dry-run", action="store_true", help="Validate deployments and exit without blocking")
    args = parser.parse_args()

    deployments = get_deployments()
    logger.info(f"Loaded {len(deployments)} production deployments:")
    for dep in deployments:
        schedules_str = ", ".join([str(s.schedule) for s in dep.schedules]) if dep.schedules else "MANUAL"
        logger.info(f"  • {dep.name} [{dep.flow_name}] -> {schedules_str}")

    if args.dry_run:
        logger.info("Dry run validation completed successfully.")
        sys.exit(0)

    logger.info("Serving all EPM Deployments to Prefect Server...")
    serve(*deployments)


if __name__ == "__main__":
    main()
