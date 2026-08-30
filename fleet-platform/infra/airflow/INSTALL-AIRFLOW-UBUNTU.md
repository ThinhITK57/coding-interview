# Cài đặt Apache Airflow cho Fleet Platform

> **Vai trò**: Điều phối batch jobs — chạy Spark aggregation theo lịch
> (weekly/monthly/quarterly/fiscal-year/year-end).
> Airflow chạy trên node `master`, dùng PostgreSQL làm metadata DB.

---

## 0. Tiền điều kiện

- ✅ Python 3.10+ trên master
- ✅ PostgreSQL đang chạy với database `airflow_metadata` (đã tạo ở bước Postgres)
- ✅ Spark cluster đang chạy

---

## 1. Cài đặt Airflow

```bash
ssh aiguystory@master

# Tạo virtual environment riêng cho Airflow
python3 -m venv ~/airflow-venv
source ~/airflow-venv/bin/activate

# Set AIRFLOW_HOME
export AIRFLOW_HOME=~/airflow
mkdir -p $AIRFLOW_HOME/dags
mkdir -p $AIRFLOW_HOME/logs
mkdir -p $AIRFLOW_HOME/plugins

# Thêm vào .bashrc
cat >> ~/.bashrc << 'EOF'
export AIRFLOW_HOME=~/airflow
EOF

# Cài đặt Airflow với Postgres provider
# Kiểm tra constraint URL mới nhất tại: https://airflow.apache.org/docs/apache-airflow/stable/installation/installing-from-pypi.html
AIRFLOW_VERSION=2.9.3
PYTHON_VERSION=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

pip install "apache-airflow[postgres]==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

# Cài thêm Spark provider (để dùng SparkSubmitOperator)
pip install apache-airflow-providers-apache-spark
```

---

## 2. Cấu hình Airflow

```bash
# Khởi tạo config mặc định
airflow version  # Tạo airflow.cfg lần đầu

nano $AIRFLOW_HOME/airflow.cfg
```

Chỉnh các mục:

```ini
[database]
# Dùng PostgreSQL thay vì SQLite mặc định
sql_alchemy_conn = postgresql+psycopg2://airflow_user:airflow_2024@master:5432/airflow_metadata

[core]
# Executor: LocalExecutor (đủ cho lab, chạy task song song trên 1 máy)
# Production dùng CeleryExecutor hoặc KubernetesExecutor
executor = LocalExecutor

# Timezone
default_timezone = Asia/Ho_Chi_Minh

# Số DAG runs song song
max_active_runs_per_dag = 1

# Không load DAG ví dụ
load_examples = False

# Thư mục DAGs
dags_folder = /home/aiguystory/airflow/dags

[webserver]
web_server_port = 8081
# Port 8080 đã dùng cho Spark UI, dùng 8081 cho Airflow

[scheduler]
# Interval kiểm tra DAG mới (giây)
dag_dir_list_interval = 30
```

---

## 3. Khởi tạo database và tạo admin user

```bash
source ~/airflow-venv/bin/activate
export AIRFLOW_HOME=~/airflow

# Migrate database schema
airflow db migrate

# Tạo admin user
airflow users create \
  --username admin \
  --firstname Fleet \
  --lastname Admin \
  --role Admin \
  --email admin@fleet.local \
  --password admin123
```

---

## 4. Cấu hình Spark Connection trong Airflow

```bash
airflow connections add 'spark_default' \
  --conn-type 'spark' \
  --conn-host 'spark://master' \
  --conn-port 7077 \
  --conn-extra '{"deploy-mode": "client"}'
```

---

## 5. Tạo systemd services

### 5.1 Airflow Webserver

```bash
sudo tee /etc/systemd/system/airflow-webserver.service << 'EOF'
[Unit]
Description=Airflow Webserver
After=postgresql.service
Requires=postgresql.service

[Service]
Type=simple
User=aiguystory
Group=aiguystory
Environment="AIRFLOW_HOME=/home/aiguystory/airflow"
Environment="PATH=/home/aiguystory/airflow-venv/bin:/usr/local/bin:/usr/bin:/bin"
ExecStart=/home/aiguystory/airflow-venv/bin/airflow webserver --port 8081
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF
```

### 5.2 Airflow Scheduler

```bash
sudo tee /etc/systemd/system/airflow-scheduler.service << 'EOF'
[Unit]
Description=Airflow Scheduler
After=postgresql.service
Requires=postgresql.service

[Service]
Type=simple
User=aiguystory
Group=aiguystory
Environment="AIRFLOW_HOME=/home/aiguystory/airflow"
Environment="PATH=/home/aiguystory/airflow-venv/bin:/usr/local/bin:/usr/bin:/bin"
ExecStart=/home/aiguystory/airflow-venv/bin/airflow scheduler
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF
```

```bash
sudo systemctl daemon-reload
sudo systemctl start airflow-webserver
sudo systemctl start airflow-scheduler
sudo systemctl enable airflow-webserver
sudo systemctl enable airflow-scheduler
```

---

## 6. Verify

```bash
# Kiểm tra services
sudo systemctl status airflow-webserver
sudo systemctl status airflow-scheduler

# Truy cập Web UI
# http://master:8081
# Login: admin / admin123

# CLI check
source ~/airflow-venv/bin/activate
airflow dags list
airflow connections get spark_default
```
