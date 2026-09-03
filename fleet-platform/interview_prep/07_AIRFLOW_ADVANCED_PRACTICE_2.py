"""
===============================================================================
 🛠️ BÀI TẬP THỰC HÀNH APACHE AIRFLOW DAGS (SET 2 - MIDDLE / SENIOR DE)
===============================================================================
File này bổ sung 3 kịch bản lập trình Airflow nâng cao:
  - Bài 4: Viết Custom Airflow Operator (`DebeziumHealthCheckOperator`).
  - Bài 5: Dynamic DAG Generation tự động sinh 10 DAGs cho 10 Trạm dịch vụ (Heads).
  - Bài 6: SLA Miss Callback Handler giám sát quá hạn báo cáo.

Usage: Copy các mã nguồn này vào dags/ của Airflow để kiểm thử.
===============================================================================
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.models.baseoperator import BaseOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import requests


# =============================================================================
# BÀI TẬP 4: CUSTOM AIRFLOW OPERATOR (KIỂM TRA DEBEZIUM CONNECTOR HEALTH)
# =============================================================================
class DebeziumHealthCheckOperator(BaseOperator):
    """Custom Operator kiểm tra trạng thái RUNNING của Debezium Connector qua REST API."""

    def __init__(self, connect_url: str, connector_name: str, **kwargs):
        super().__init__(**kwargs)
        self.connect_url = connect_url
        self.connector_name = connector_name

    def execute(self, context):
        endpoint = f"{self.connect_url}/connectors/{self.connector_name}/status"
        self.log.info(f"Checking Debezium health at: {endpoint}")

        try:
            response = requests.get(endpoint, timeout=10)
            if response.status_code != 200:
                raise Exception(f"Debezium REST API returned HTTP {response.status_code}")

            data = response.json()
            connector_state = data.get("connector", {}).get("state")
            tasks = data.get("tasks", [])

            if connector_state != "RUNNING":
                raise Exception(f"Debezium Connector '{self.connector_name}' is in state: {connector_state}")

            for task in tasks:
                task_state = task.get("state")
                task_id = task.get("id")
                if task_state != "RUNNING":
                    raise Exception(f"Debezium Task #{task_id} is FAILED or STOPPED (state={task_state})")

            self.log.info(f"✅ Debezium Connector '{self.connector_name}' and all tasks are RUNNING healthy!")
            return True

        except Exception as e:
            self.log.error(f"❌ Debezium Health Check Failed: {e}")
            raise


# =============================================================================
# BÀI TẬP 5: DYNAMIC DAG GENERATION (TỰ ĐỘNG SINH DAGS THEO CONFIG)
# =============================================================================
HEAD_CONFIGS = [
    {"head_id": 1, "name": "BinhTan", "schedule": "0 1 * * *"},
    {"head_id": 2, "name": "ThuDuc",  "schedule": "0 2 * * *"},
    {"head_id": 4, "name": "BinhDuong", "schedule": "0 3 * * *"},
]

def create_head_reporting_dag(head_id: int, head_name: str, schedule: str):
    dag_id = f"fleet_head_report_{head_name.lower()}_dag"

    default_args = {
        "owner": "fleet_de_team",
        "start_date": datetime(2026, 1, 1),
        "retries": 1,
    }

    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        schedule_interval=schedule,
        catchup=False,
        tags=["fleet", "dynamic_head", head_name],
    )

    with dag:
        task_agg = BashOperator(
            task_id=f"aggregate_head_{head_id}_metrics",
            bash_command=f"spark-submit --master local[*] /path/to/batch_dwh_aggregation.py --head-id {head_id}",
        )

    return dag


# Thêm các DAG động vào namespace toàn cục
for config in HEAD_CONFIGS:
    generated_dag = create_head_reporting_dag(config["head_id"], config["name"], config["schedule"])
    globals()[generated_dag.dag_id] = generated_dag


# =============================================================================
# BÀI TẬP 6: SLA MISS CALLBACK HANDLER
# =============================================================================
def sla_miss_alert_handler(dag, task_list, blocking_task_list, slas, blocking_tis):
    """Hàm xử lý khi DAG bị vi phạm SLA (quá hạn hoàn thành báo cáo)."""
    print(f"🚨 SLA MISS DETECTED in DAG '{dag.dag_id}'! Tasks violating SLA: {task_list}")


with DAG(
    dag_id="fleet_sla_monitored_report_dag",
    default_args={"owner": "fleet_de_team", "start_date": datetime(2026, 1, 1)},
    schedule_interval="0 3 1 * *",
    sla_miss_callback=sla_miss_alert_handler,
    catchup=False,
    tags=["fleet", "sla_monitored"],
) as dag6:

    health_check_task = DebeziumHealthCheckOperator(
        task_id="check_debezium_cdc_health",
        connect_url="http://master:8083",
        connector_name="fleet-cdc-connector",
    )

    run_report_task = BashOperator(
        task_id="run_critical_monthly_report",
        bash_command="spark-submit --master local[*] /path/to/batch_dwh_aggregation.py --granularity monthly",
        sla=timedelta(hours=2), # Phải chạy xong trong vòng 2 tiếng
    )

    health_check_task >> run_report_task
