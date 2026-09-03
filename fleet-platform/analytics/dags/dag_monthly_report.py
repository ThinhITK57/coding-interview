"""
Fleet Platform — Airflow Monthly Report Batch DAG
=================================================
DAG điều phối công việc tổng hợp báo cáo HÀNG THÁNG (Monthly):
  1. Trigger PySpark SCD Type 2 Customer Dimension (`scd2_customer_dimension.py`)
  2. Trigger PySpark Batch DWH Aggregation (`batch_dwh_aggregation.py --granularity monthly`)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "fleet_de_team",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="fleet_monthly_report_dag",
    default_args=default_args,
    description="Airflow Monthly Financial & Profit Margin Aggregation",
    schedule_interval="0 2 1 * *",  # 02:00 AM ngày đầu tiên của mỗi tháng
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["fleet", "monthly", "dwh", "spark"],
) as dag:

    t1_scd2 = BashOperator(
        task_id="update_scd2_customers",
        bash_command="spark-submit --master local[*] /path/to/fleet-platform/analytics/jobs/scd2_customer_dimension.py",
    )

    t2_monthly_agg = BashOperator(
        task_id="aggregate_monthly_report",
        bash_command="spark-submit --master local[*] /path/to/fleet-platform/analytics/jobs/batch_dwh_aggregation.py --granularity monthly",
    )

    t1_scd2 >> t2_monthly_agg
