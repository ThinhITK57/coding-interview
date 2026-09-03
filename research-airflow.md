# CẨM NANG ÔN LUYỆN PHỎNG VẤN: APACHE AIRFLOW

Tài liệu này tập trung vào kiến thức nền tảng, thiết kế DAG chuẩn production, các Executors, xây dựng ETL pipeline hoàn chỉnh, cài đặt Docker, tối ưu hóa HA 24/7 và hệ thống giám sát cảnh báo cho Apache Airflow.

---

## PHẦN 1: KIẾN THỨC NỀN TẢNG APACHE AIRFLOW

### 1. Kiến trúc Apache Airflow (Airflow Architecture)
Airflow hoạt động theo cơ chế lập lịch hướng sự kiện (event-driven scheduler) với các thành phần cốt lõi sau:

```mermaid
graph TD
    Web[Web Server] -->|Queries status| DB[(Metadata Database)]
    Scheduler[Scheduler] -->|Schedules & triggers| DB
    Scheduler -->|Pushes tasks| Queue[Message Queue / Celery Broker <br> Redis/RabbitMQ]
    Queue -->|Pulls tasks| Worker[Celery/K8s Worker]
    Worker -->|Updates state| DB
    DAGs[DAG Directory] -->|Parsed by| Scheduler
    DAGs -->|Parsed by| Web
    DAGs -->|Parsed by| Worker
```

*   **Scheduler**: Bộ não của Airflow. Nó liên tục quét thư mục chứa các file DAG, phân tích cấu trúc, kiểm tra lịch trình (schedule interval) và đẩy các task cần thực thi vào hàng đợi (Queue) dưới dạng các TaskInstance. Từ Airflow 2.0+, bạn có thể chạy nhiều Scheduler song song để tăng tính dự phòng (HA).
*   **Web Server**: Giao diện người dùng trực quan (Flask App). Cho phép xem danh sách DAGs, trạng thái chạy của các task, xem logs, kích hoạt/tạm dừng DAG, quản lý Connections và Variables. Web Server chỉ đọc dữ liệu từ Metadata DB và không trực tiếp chạy tasks.
*   **Metadata Database**: Nơi lưu trữ trạng thái của tất cả các DAGs, Tasks, Connections, Variables, người dùng, v.v. Hỗ trợ PostgreSQL (khuyên dùng trong production), MySQL, SQLite (chỉ dùng cho phát triển local).
*   **Executor**: Định nghĩa *cách thức* và *nơi* các task sẽ được thực thi. Executor chạy bên trong Scheduler.
*   **Workers**: Các tiến trình thực thi task thực sự (trong CeleryExecutor hoặc KubernetesExecutor).

---

### 2. Các Khái niệm cốt lõi (Core Concepts)
*   **DAG (Directed Acyclic Graph)**: Đồ thị có hướng không chu trình. Tập hợp các Task được sắp xếp theo trình tự phụ thuộc rõ ràng.
*   **Operator**: Bản thiết kế (blueprint) cho một Task. Xác định công việc cụ thể sẽ làm.
    *   *Action Operators*: Thực thi một hành động (ví dụ: `PythonOperator`, `BashOperator`).
    *   *Transfer Operators*: Di chuyển dữ liệu giữa các hệ thống (ví dụ: `S3ToRedshiftOperator`).
    *   *Sensors*: Một loại Operator đặc biệt, liên tục thăm dò (polling) một điều kiện ngoại vi (ví dụ: file xuất hiện trên S3, một partition trong DB được tạo) và chỉ cho phép các task tiếp theo chạy khi điều kiện thỏa mãn.
*   **Task**: Đại diện cho một node trong DAG. Là một thực thể cụ thể của một Operator.
*   **Task Instance**: Trạng thái của một Task tại một thời điểm chạy cụ thể (ví dụ: Task A chạy ngày 2026-07-12). Trạng thái có thể là: *scheduled, queued, running, success, failed, skipped, retrying*.
*   **Connection & Variable**:
    *   *Connection*: Lưu thông tin kết nối tới hệ thống ngoài (DB credentials, API keys) được mã hóa trong Metadata DB.
    *   *Variable*: Lưu trữ cấu hình toàn cục dạng key-value.
*   **XCom (Cross-Communication)**: Cơ chế cho phép các Task trao đổi dữ liệu nhỏ với nhau (mặc định lưu qua Metadata DB dưới dạng serialized JSON). Không dùng XCom để truyền tập dữ liệu lớn (như file CSV 1GB) vì sẽ làm nghẽn Database. Thay vào đó hãy ghi file ra S3/HDFS và truyền đường dẫn file qua XCom.

---

### 3. Phân biệt các loại Executor trong Production

| Executor | Cách thức hoạt động | Ưu điểm | Nhược điểm | Phù hợp cho |
| :--- | :--- | :--- | :--- | :--- |
| **LocalExecutor** | Scheduler khởi tạo các tiến trình con (subprocess) cục bộ để chạy task song song trên cùng một server với Scheduler. | Dễ cài đặt, không cần cài đặt thêm Broker bên ngoài (Redis, RabbitMQ). | Giới hạn bởi cấu hình phần cứng của một server duy nhất. Không thể scale ngang (Scale-out). | Dự án vừa và nhỏ, hạ tầng đơn giản. |
| **CeleryExecutor** | Gửi task vào Message Broker (Redis/RabbitMQ). Các Celery Workers chạy độc lập trên nhiều server khác nhau sẽ pull task về chạy. | Khả năng scale ngang cực tốt bằng cách thêm worker node mới. Chạy ổn định, chịu tải cao. | Cần cài đặt và duy trì các hệ thống ngoài (Redis/RabbitMQ, Celery workers). Khó quản lý môi trường Python đồng nhất trên tất cả worker. | Môi trường production lớn với tần suất chạy task liên tục và ổn định. |
| **KubernetesExecutor** | Mỗi khi có task cần chạy, Airflow sẽ gọi Kubernetes API để tạo một Pod mới (Kubernetes Pod). Khi task chạy xong, Pod tự động bị xóa. | Scale-out vô hạn (chỉ bị giới hạn bởi cluster K8s). Tiết kiệm chi phí vì Pod chỉ sống khi có task. Tự cô lập môi trường (mỗi task có thể có Docker Image riêng với thư viện khác nhau). | Độ trễ khởi động task cao (phải đợi K8s tạo pod và pull docker image - khoảng 10-30s). Cần am hiểu Kubernetes sâu sắc. | Các tác vụ không chạy liên tục nhưng cần tài nguyên cực lớn khi chạy, hoặc đòi hỏi môi trường chạy biệt lập. |

---

### 4. Điểm mới của Airflow 2.x & TaskFlow API
*   **Scheduler HA**: Từ Airflow 2.0, bạn có thể chạy nhiều Scheduler cùng lúc ở chế độ Active-Active để tăng tính dự phòng.
*   **TaskFlow API**: Cách viết DAG mới tối giản bằng Python Decorators (`@dag`, `@task`). Tự động xử lý XCom truyền nhận giá trị giữa các hàm mà không cần gọi `xcom_push()` và `xcom_pull()`.

**Code Ví dụ TaskFlow API:**
```python
from airflow.decorators import dag, task
from datetime import datetime

@dag(start_date=datetime(2026, 1, 1), schedule="@daily", catchup=False)
def simple_taskflow_dag():
    
    @task()
    def get_user_id():
        return 42

    @task()
    def process_user(user_id: int):
        print(f"Processing user: {user_id}")

    # Tự động truyền dữ liệu qua XCom dưới background
    user_id = get_user_id()
    process_user(user_id)

dag_obj = simple_taskflow_dag()
```

---

## PHẦN 2: THIẾT KẾ DAG & PRODUCTION BEST PRACTICES

### 1. Tính bất biến và Khả năng chạy lại (Idempotency)
> **[!IMPORTANT]  
> Nguyên tắc số 1 của Data Pipeline:** Một DAG hoặc Task được gọi là **Idempotent** nếu khi ta chạy nó nhiều lần với cùng một tham số đầu vào (cùng ngày thực thi - execution_date), kết quả đầu ra cuối cùng phải luôn luôn giống nhau và không sinh ra dữ liệu trùng lặp.

**Cách thiết kế Idempotent DAGs:**
*   **Tránh dùng câu lệnh `INSERT` thuần túy**: Thay thế bằng cơ chế **Upsert** (Update if exists, Insert if not) hoặc **Overwrite Partition** (Xóa sạch partition của ngày hôm đó trước khi ghi đè dữ liệu mới).
*   **Không dùng thời gian hiện tại (`datetime.now()`) bên trong code xử lý**: Luôn sử dụng biến môi trường thời gian của Airflow (`ds` hoặc `logical_date` đại diện cho ngày thực thi dữ liệu) để đảm bảo khi ta backfill (chạy lại dữ liệu của 1 tháng trước), code vẫn xử lý chính xác dữ liệu của 1 tháng trước chứ không lấy ngày hôm nay.

---

### 2. Backfilling và Catchup
*   `catchup=True` (mặc định): Nếu bạn định nghĩa một DAG có `start_date` là ngày 2026-07-01 và kích hoạt nó vào ngày 2026-07-12, Airflow sẽ tự động khởi tạo hàng loạt luồng chạy cho tất cả các ngày từ 01 đến 12 để bù lại dữ liệu quá khứ.
*   `catchup=False`: Airflow chỉ chạy phiên dữ liệu gần nhất và bỏ qua toàn bộ lịch sử. Khuyên dùng trong production trừ khi bạn chủ động muốn nạp lại dữ liệu cũ.
*   **Backfill Command**: Nếu muốn chạy lại dữ liệu cũ thủ công:
    ```bash
    airflow dags backfill --start-date 2026-07-01 --end-date 2026-07-10 my_dag_name
    ```

---

### 3. Dynamic Task Generation và Task Groups
*   **Dynamic Task Mapping**: Kể từ Airflow 2.3, bạn có thể định nghĩa số lượng task chạy song song động dựa trên kết quả đầu ra của task trước đó.
*   **TaskGroups**: Thay thế cho SubDAGs (đã lỗi thời và gây rò rỉ tài nguyên). Dùng để gom nhóm các task trực quan trên UI Web Server.

```python
from airflow.utils.task_group import TaskGroup

with TaskGroup("process_db_tables", tooltip="Process DB Tables") as process_group:
    for table in ["users", "orders", "payments"]:
        # Tạo động các task xử lý song song từng bảng trong nhóm
        process_table = PythonOperator(
            task_id=f"clean_{table}",
            python_callable=clean_table_func,
            op_kwargs={"table_name": table}
        )
```

---

## PHẦN 3: XÂY DỰNG ETL PIPELINE VỚI AIRFLOW & SPARK

Dưới đây là một ví dụ thực tế chuẩn phỏng vấn: Dùng Airflow để điều phối (orchestrate) một luồng ETL: Đọc dữ liệu từ DB PostgreSQL → Khởi chạy Spark Job xử lý trên Cloud (AWS EMR/Docker) → Ghi vào Data Warehouse.

### Mã nguồn DAG hoàn chỉnh (`orchestrate_etl.py`)

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.sql import SqlSensor
from airflow.operators.empty import EmptyOperator

# Cấu hình các tham số mặc định cho các Task
default_args = {
    "owner": "data-infra-team",
    "depends_on_past": False,
    "email": ["alert-data@company.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="postgres_to_spark_to_delta_pipeline",
    default_args=default_args,
    description="Orchestrator for financial transactions ETL pipeline",
    schedule_interval="0 2 * * *",  # Chạy vào 02:00 AM mỗi ngày
    start_date=datetime(2026, 7, 1),
    catchup=False,
    tags=["finance", "etl", "spark"],
) as dag:

    start = EmptyOperator(task_id="start_pipeline")

    # 1. SENSOR: Kiểm tra xem database nguồn đã sẵn sàng dữ liệu của ngày hôm nay chưa
    # Nếu chưa có bản ghi nào của ngày {{ ds }}, sensor sẽ đợi và thăm dò lại mỗi 60s
    wait_for_source_data = SqlSensor(
        task_id="wait_for_source_data",
        conn_id="postgres_source",
        sql="SELECT COUNT(1) FROM transactions WHERE DATE(transaction_date) = '{{ ds }}';",
        poke_interval=60,
        timeout=600,
        mode="reschedule" # Giải phóng worker slot trong lúc chờ
    )

    # 2. RUN QUERY: Tạo trước bảng hoặc chuẩn bị schema phía Data Warehouse PostgreSQL
    prepare_dw_tables = PostgresOperator(
        task_id="prepare_dw_tables",
        postgres_conn_id="postgres_dw",
        sql="""
            CREATE TABLE IF NOT EXISTS dw_transactions (
                id INT,
                user_id INT,
                amount DECIMAL(10,2),
                year INT,
                month INT,
                day INT,
                category_level VARCHAR(20),
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """
    )

    # 3. SPARK SUBMIT: Chạy Spark ETL Job thực tế
    # Gửi code PySpark lên Master Spark Node để chạy phân tán xử lý lượng lớn dữ liệu
    run_spark_etl = SparkSubmitOperator(
        task_id="run_spark_etl",
        application="/opt/airflow/dags/scripts/spark_etl_job.py", # Đường dẫn đến file python spark
        conn_id="spark_default",
        java_class=None,
        total_executor_cores=4,
        executor_cores=2,
        executor_memory="2g",
        driver_memory="1g",
        verbose=True,
        application_args=[
            "--execution_date", "{{ ds }}",
            "--db_host", "postgres-source-host",
            "--db_dw_path", "/data/dw/transactions_delta"
        ]
    )

    end = EmptyOperator(
        task_id="end_pipeline",
        on_success_callback=lambda context: print("Pipeline completed successfully! Send notification.")
    )

    # Thiết lập luồng phụ thuộc (DAG Flow)
    start >> wait_for_source_data >> prepare_dw_tables >> run_spark_etl >> end
```

---

## PHẦN 4: CÀI ĐẶT & HẠ TẦNG (DOCKER SETUP)

### `docker-compose.yaml` (Airflow 2.x Local Development)
Dưới đây là phiên bản thu gọn chính thức của Apache Airflow sử dụng LocalExecutor với DB PostgreSQL.

```yaml
version: '3.8'
x-airflow-common:
  &airflow-common
  image: apache/airflow:2.6.2
  environment:
    &airflow-common-env
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
    AIRFLOW__CORE__FERNET_KEY: ''
    AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: 'true'
    AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
    AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL: 10
  volumes:
    - ./dags:/opt/airflow/dags
    - ./logs:/opt/airflow/logs
    - ./plugins:/opt/airflow/plugins
  user: "${AIRFLOW_UID:-50000}:0"
  depends_on:
    postgres:
      condition: service_healthy

services:
  postgres:
    image: postgres:13
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    volumes:
      - postgres-db-volume:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U airflow"]
      interval: 10s
      timeout: 5s
      retries: 5
    restart: always

  airflow-webserver:
    <<: *airflow-common
    command: webserver
    ports:
      - "8080:8080"
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:8080/health"]
      interval: 30s
      timeout: 10s
      retries: 5
    restart: always

  airflow-scheduler:
    <<: *airflow-common
    command: scheduler
    restart: always

volumes:
  postgres-db-volume:
```

*Khởi tạo DB lần đầu trước khi up:* 
`docker-compose run --rm airflow-webserver airflow db init` (chỉ chạy 1 lần).

---

## PHẦN 5: BẢO ĐẢM UPTIME 24/7 & HỆ THỐNG GIÁM SÁT CẢNH BÁO

### 1. Thiết kế Hệ thống High Availability (HA) cho Airflow
Để Airflow không bao giờ sập trong môi trường production:
*   **Scheduler HA**: Chạy ít nhất 2 Scheduler song song trên các server khác nhau. Chúng sẽ tự động chia sẻ tải đọc ghi DB thông qua cơ chế khóa dòng (row-level locking) trong Metadata Database để tránh trigger task trùng lặp.
*   **Database HA**: Điểm yếu nhất của Airflow chính là Metadata DB. Cần thiết lập PostgreSQL dạng **Primary-Standby** với cơ chế Auto-failover (ví dụ dùng Patroni).
*   **Shared DAGs**: Đảm bảo toàn bộ Scheduler và Workers truy cập cùng một thư mục DAG giống hệt nhau. Giải pháp phổ biến:
    *   Sử dụng **git-sync** (Sidecar container trong Kubernetes tự động pull DAG từ git repo về RAM của các worker/scheduler mỗi 10s).
    *   Không dùng ổ đĩa mạng chia sẻ NFS vì NFS có độ trễ I/O cao, dễ làm treo Scheduler.

---

### 2. Alert qua Slack / Microsoft Teams khi Job lỗi
Airflow cho phép đăng ký callback function để kích hoạt khi task bị lỗi.

**Code cấu hình Alert về Slack (`slack_alert.py`):**
```python
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

def on_failure_slack_alert(context):
    """
    Hàm callback tự động chạy khi bất kỳ task nào trong DAG bị thất bại.
    """
    task_instance = context.get('task_instance')
    dag_id = task_instance.dag_id
    task_id = task_instance.task_id
    execution_date = context.get('execution_date')
    exception = context.get('exception')
    
    slack_msg = f"""
    *🚨 Alert: Airflow Task Failed!*
    *DAG:* {dag_id}
    *Task:* {task_id}
    *Execution Date:* {execution_date}
    *Error:* {exception}
    *Logs:* <{task_instance.log_url}|Click here to view logs>
    """
    
    # Gửi tin nhắn về Slack Channel thông qua Webhook Connection
    alert = SlackWebhookOperator(
        task_id='slack_alert_callback',
        http_conn_id='slack_connection',
        message=slack_msg,
        channel='#data-pipeline-alerts'
    )
    return alert.execute(context=context)

# Áp dụng vào DAG default_args
default_args = {
    'on_failure_callback': on_failure_slack_alert,
    'retries': 2
}
```

---

## PHẦN 6: CÁC CÂU HỎI PHỎNG VẤN VỀ AIRFLOW THƯỜNG GẶP

### Câu 1: Làm thế nào để giải quyết vấn đề truyền dữ liệu giữa các Task?
*   *Trả lời*: Sử dụng **XComs** cho dữ liệu nhỏ cấu hình (dưới vài KB). Đối với dữ liệu lớn (Big Data), ghi dữ liệu ra các kho lưu trữ ngoài như HDFS/S3/GCS từ task trước, sau đó truyền đường dẫn URI của file đó cho task tiếp theo thông qua XCom.

### Câu 2: Sự khác biệt giữa CeleryExecutor và KubernetesExecutor?
*   *Trả lời*: Trình bày bảng so sánh ở Phần 1. Nhấn mạnh việc CeleryExecutor luôn có pool worker sẵn sàng chạy nên độ trễ khởi động task cực thấp, phù hợp pipeline thời gian thực/micro-batch. KubernetesExecutor tiết kiệm tài nguyên tối đa và cô lập môi trường tốt nhưng khởi tạo task chậm hơn.

### Câu 3: Làm thế nào để scale hiệu năng của Scheduler khi có quá nhiều DAG?
*   *Trả lời*:
    1.  Tăng tần suất quét file DAG (`AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL` tăng lên khoảng 30s-60s để giảm tải CPU).
    2.  Chạy nhiều bản sao Scheduler song song (Scheduler HA).
    3.  Tối ưu hóa code DAG: Tránh viết các câu lệnh truy vấn DB lớn hoặc gọi API trực tiếp ở mức toàn cục (top-level code) trong file DAG. Code ở mức này bị Scheduler thực thi lặp đi lặp lại sau mỗi vài giây khi parse DAG, gây nghẽn Scheduler. Chỉ viết code nặng bên trong hàm thực thi của Operator (`execute` hoặc `python_callable`).

### Câu 4: Việc gì xảy ra khi một Task bị timeout? Làm sao xử lý?
*   *Trả lời*: Nếu task chạy lâu hơn tham số `execution_timeout`, Airflow tự động kill task đó và chuyển trạng thái sang `FAILED`. Ta xử lý bằng cách cấu hình `retries` đi kèm `retry_delay`, hoặc sử dụng `on_failure_callback` để gửi alert báo cho kỹ sư dữ liệu xử lý trực tiếp.
