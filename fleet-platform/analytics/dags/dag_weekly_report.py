"""
Fleet Platform — Airflow Weekly Report Batch DAG
=================================================
DAG điều phối công việc tổng hợp báo cáo doanh thu & tồn kho HÀNG TUẦN (Weekly):
  1. Trigger PySpark SCD Type 2 Customer Dimension (`scd2_customer_dimension.py`)
  2. Trigger PySpark Batch DWH Aggregation (`batch_dwh_aggregation.py --granularity weekly`)
  3. Verify Redis aggregate output
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "fleet_de_team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="fleet_weekly_report_dag",
    default_args=default_args,
    description="Airflow Weekly Financial & Inventory DWH Batch Ingestion",
    schedule_interval="0 1 * * 1",  # 01:00 AM vào mỗi Thứ Hai hàng tuần
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["fleet", "weekly", "dwh", "spark"],
) as dag:

    # Task 1: SCD Type 2 Customer Dimension Update
    t1_scd2_update = BashOperator(
        task_id="update_scd2_customer_dimension",
        bash_command="""
        spark-submit \
          --master local[*] \
          /path/to/fleet-platform/analytics/jobs/scd2_customer_dimension.py
        """,
    )

    # Task 2: Weekly DWH Aggregation Job
    t2_weekly_aggregation = BashOperator(
        task_id="aggregate_weekly_financials",
        bash_command="""
        spark-submit \
          --master local[*] \
          /path/to/fleet-platform/analytics/jobs/batch_dwh_aggregation.py --granularity weekly
        """,
    )

    # Dependency Flow
    t1_scd2_update >> t2_weekly_aggregation
