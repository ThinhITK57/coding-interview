import time
import subprocess
import json
from datetime import datetime
from prefect import get_run_logger, task
from utils.spark_linage_visitor import *
from utils.om_client import OpenMetadataSDK
from utils.job_logger import JobLogger

SNAPSHOT_JOBS = [
    "jobs/bi_silver_snapshot/cx_cso_mart_daily_ticket_summary_snapshot.py",
    "jobs/bi_silver_snapshot/cx_cso_support_ticket_ttr_snapshot.py",
]


@task
def run_snapshot_task(snapshot_date):
    logger = JobLogger()
    logger2 = get_run_logger()

    try:
        for job in SNAPSHOT_JOBS:
            logger2.info(
                f"🚀 START SNAPSHOT {job} "
                f"for {snapshot_date} "
                f"at {datetime.now()}"
            )

            logger.start_job(job)

            params = {
                "snapshot_date": snapshot_date
            }

            cmd = [
                "bash",
                "/entrypoint.sh",
                "run",
                job,
                json.dumps(params),
            ]

            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )

            error_lines = []

            for line in process.stdout:
                logger2.info(f"[{job}] {line}")

                if " ERROR " in line:
                    error_lines.append(line)

            process.wait()

            if process.returncode != 0:
                raise Exception(
                    "".join(error_lines[-50:])
                )

            logger.success()

            logger2.info(
                f"✅ SUCCESS SNAPSHOT {job} "
                f"({snapshot_date})"
            )

    except Exception as e:
        logger.fail(e)

        logger2.error(
            f"❌ FAILED SNAPSHOT {job} "
            f"({snapshot_date})"
        )

        raise

    finally:
        logger.close()