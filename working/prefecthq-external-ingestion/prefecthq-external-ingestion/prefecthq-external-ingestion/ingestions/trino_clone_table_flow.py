
from dotenv import load_dotenv

load_dotenv()
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import os
import sys

from resources import RESOURCE_BASE_DIR
from state import STATE_BASE_DIR
from data import DATA_BASE_DIR

from prefect import flow, task, get_run_logger

from common.trino_clone_service import TrinoCloneService


@task(
    retries=2,
    retry_delay_seconds=30,
)
def clone_table_task(
    schema: str,
    table: str,
    truncate: bool = False,
    clone_schema: bool = False,
):
    logger = get_run_logger()

    logger.info(
        f"Clone table: {schema}.{table}"
    )

    service = TrinoCloneService(
        schema=schema,
        table=table,
        resource_dir=RESOURCE_BASE_DIR,
    )

    try:
        service.run(
            truncate=truncate,
            clone_schema=clone_schema,
        )

    finally:
        service.close()



@flow(log_prints=True, name="[Manual] Clone trino table")
def trino_clone_manual_flow(
    schema: str,
    table: str,
    truncate: bool = False,
    clone_schema: bool = False,
):
    clone_table_task(
        schema=schema,
        table=table,
        truncate=truncate,
        clone_schema=clone_schema,
    )


@flow(log_prints=True, name="Clone trino table [Daily]")
def trino_clone_daily_flow():

    tables = [
        (
            "bi_silver",
            "cx_mixpanel_product_feature",
            True,
            False,
        ),
        (
            "bi_silver",
            "cx_mixpanel_product_feature_usage_mart",
            True,
            False,
        ),
    ]

    for (
        schema,
        table,
        truncate,
        clone_schema,
    ) in tables:

        clone_table_task(
            schema=schema,
            table=table,
            truncate=truncate,
            clone_schema=clone_schema,
        )


if __name__ == "__main__":
    d1 = trino_clone_manual_flow.to_deployment(
        name="[Manual] Clone Trino Tables",
        tags=["production", "TRINO", "CLONE", "MANUAL"],
    )

    d3 = trino_clone_daily_flow.to_deployment(
        name="[Daily] Clone Trino Tables",
        cron="0 22 * * *",
        tags=["production", "TRINO", "CLONE", "DAILY"],
    )
    from prefect import serve
    serve(d1, d3)
