"""
===============================================================================
 🛠️ BÀI TẬP THỰC HÀNH APACHE AIRFLOW DAGS (LEVEL MIDDLE / SENIOR DE)
===============================================================================
File này chứa 3 mẫu thiết kế Airflow DAGs nâng cao phục vụ dự án:
  - Bài 1: BranchPythonOperator điều phối luồng báo cáo động (Weekly/Monthly/Fiscal Year).
  - Bài 2: Failure Webhook Callback + SLA Monitoring + Retry Strategy.
  - Bài 3: Cross-DAG Orchestration sử dụng TriggerDagRunOperator & ExternalTaskSensor.

Usage: Copy các DAG này vào thư mục dags/ của Airflow để kiểm thử.
===============================================================================
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor

# =============================================================================
# DEFAULT ARGS VÀ FAILURE CALLBACK
# =============================================================================
def notify_slack_on_failure(context):
    """Hàm Callback gửi thông báo alert tới Telegram/Slack khi Task thất bại."""
    dag_id = context.get("task_instance").dag_id
    task_id = context.get("task_instance").task_id
    execution_date = context.get("execution_date")
    log_url = context.get("task_instance").log_url

    alert_msg = f"""
    🚨 *AIRFLOW TASK FAILED ALERT* 🚨
    *DAG*: {dag_id}
    *Task*: {task_id}
    *Execution Date*: {execution_date}
    *Log*: <{log_url}|View Logs>
    """
    print(f"DEBUG: Gửi Telegram Webhook: {alert_msg}")


DEFAULT_ARGS = {
    "owner": "fleet_de_team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": notify_slack_on_failure,
}


# =============================================================================
# BÀI TẬP 1: BRANCHING & DYNAMIC REPORT ORCHESTRATION DAG
# =============================================================================
def decide_report_type(**context):
    """Quyết định chọn nhánh báo cáo theo ngày thực thi."""
    exec_date = context["execution_date"]
    
    # 01/10 hàng năm -> Chạy báo cáo Năm Tài Khóa (Fiscal Year)
    if exec_date.month == 10 and exec_date.day == 1:
        return "run_fiscal_year_report"
    # Ngày 1 hàng tháng -> Chạy báo cáo Tháng
    elif exec_date.day == 1:
        return "run_monthly_report"
    # Thứ Hai hàng tuần -> Chạy báo cáo Tuần
    else:
        return "run_weekly_report"


with DAG(
    dag_id="fleet_dynamic_branching_report_dag",
    default_args=DEFAULT_ARGS,
    description="Dynamic Branching Report Orchestration DAG",
    schedule_interval="0 1 * * *", # 01:00 AM hàng ngày
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["fleet", "branching", "production"],
) as dag1:

    # Task 1: SCD Type 2 Update (Luôn luôn chạy trước)
    t1_scd2_update = BashOperator(
        task_id="update_scd2_customers",
        bash_command="spark-submit --master local[*] /path/to/scd2_customer_dimension.py",
    )

    # Task 2: Branch Decision Operator
    t2_branch_decision = BranchPythonOperator(
        task_id="decide_report_branch",
        python_callable=decide_report_type,
        provide_context=True,
    )

    # Task 3a: Nhánh Báo cáo Tuần
    t3a_weekly = BashOperator(
        task_id="run_weekly_report",
        bash_command="spark-submit --master local[*] /path/to/batch_dwh_aggregation.py --granularity weekly",
    )

    # Task 3b: Nhánh Báo cáo Tháng
    t3b_monthly = BashOperator(
        task_id="run_monthly_report",
        bash_command="spark-submit --master local[*] /path/to/batch_dwh_aggregation.py --granularity monthly",
    )

    # Task 3c: Nhánh Báo cáo Năm Tài Khóa
    t3c_fiscal = BashOperator(
        task_id="run_fiscal_year_report",
        bash_command="spark-submit --master local[*] /path/to/batch_dwh_aggregation.py --granularity fiscal_year",
    )

    # Dependency Pipeline
    t1_scd2_update >> t2_branch_decision
    t2_branch_decision >> [t3a_weekly, t3b_monthly, t3c_fiscal]


# =============================================================================
# BÀI TẬP 2: CROSS-DAG DEPENDENCY ORCHESTRATION (TRIGGER & SENSOR)
# =============================================================================
with DAG(
    dag_id="fleet_master_orchestrator_dag",
    default_args=DEFAULT_ARGS,
    description="Master DAG triggering sub-pipeline DAGs with Sensor verification",
    schedule_interval="0 2 1 * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["fleet", "orchestrator"],
) as dag2:

    # Trigger DAG Báo cáo Tháng
    trigger_monthly_dag = TriggerDagRunOperator(
        task_id="trigger_monthly_report_dag",
        trigger_dag_id="fleet_monthly_report_dag",
        wait_for_completion=True, # Cho tới khi DAG con chạy xong
        poke_interval=30,
    )

    # Sensor chờ tín hiệu hoàn tất từ HDFS Compaction DAG
    wait_hdfs_compaction = ExternalTaskSensor(
        task_id="wait_for_hdfs_compaction",
        external_dag_id="hdfs_nightly_compaction_dag",
        external_task_id="compact_parquet_files",
        mode="reschedule", # Tránh chiếm dụng worker slot
        timeout=3600,
    )

    wait_hdfs_compaction >> trigger_monthly_dag
