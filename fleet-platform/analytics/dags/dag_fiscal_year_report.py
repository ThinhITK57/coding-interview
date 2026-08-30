"""
Fleet Platform — Airflow Fiscal Year Report Batch DAG
======================================================
DAG điều phối tổng hợp báo cáo NĂM TÀI KHÓA (Fiscal Year: 01/10 năm trước đến 30/09 năm sau):
  - Chạy tự động vào 03:00 AM ngày 01/10 hàng năm.
  - Tổng hợp doanh thu, chi phí, lợi nhuận ròng cho Ban Giám Đốc.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "fleet_de_team",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=15),
}

with DAG(
    dag_id="fleet_fiscal_year_report_dag",
    default_args=default_args,
    description="Airflow Fiscal Year Financial Audit & Strategic Report Aggregation",
    schedule_interval="0 3 1 10 *",  # 03:00 AM ngày 1 tháng 10 hàng năm (Năm tài khóa mới)
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["fleet", "fiscal_year", "audit", "spark"],
) as dag:

    t1_fiscal_agg = BashOperator(
        task_id="aggregate_fiscal_year_report",
        bash_command="spark-submit --master local[*] /path/to/fleet-platform/analytics/jobs/batch_dwh_aggregation.py --granularity fiscal_year",
    )

    t1_fiscal_agg
