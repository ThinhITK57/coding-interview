# Ticket 5 — Data Warehouse Star Schema & SCD Type 2 Batch Pipeline (Spark Batch + Airflow)

> **Mục tiêu**:
> 1. Xây dựng Data Warehouse Star Schema (Parquet trên HDFS `/fleet-datalake/warehouse/`): Bảng Fact (`fact_repair_service_revenue`, `fact_parts_sales`, `fact_inventory_movement`) và Dimension (`dim_customer` SCD Type 2, `dim_head`, `dim_component`, `dim_date`).
> 2. PySpark SCD Type 2 Pipeline: Quản lý lịch sử biến đổi trạng thái khách hàng (`mới` → `cũ` → `thường niên`).
> 3. Airflow DAGs: Tự động hóa lịch trình tổng hợp báo cáo Hàng tuần (Weekly), Hàng tháng (Monthly), Hàng quý (Quarterly), Năm tài khóa (Fiscal Year: 01/10 - 30/09) và Cuối năm (Year-End).
> 4. Redis Aggregates & Pub/Sub: Lưu kết quả báo cáo mới nhất vào Redis String Keys (`report:agg:*`) và phát tín hiệu báo động lên Redis Channel `channel:report-updates` cho WebSocket Backend Layer.

---

## Cấu trúc thư mục Ticket 5

```
fleet-platform/
└── analytics/
    ├── jobs/
    │   ├── scd2_customer_dimension.py    ← Job PySpark xử lý SCD Type 2 Customer
    │   └── batch_dwh_aggregation.py      ← Job PySpark tổng hợp báo cáo & đẩy Redis
    ├── dags/
    │   ├── dag_weekly_report.py          ← Airflow DAG Báo cáo Hàng tuần (T2 01:00 AM)
    │   ├── dag_monthly_report.py         ← Airflow DAG Báo cáo Hàng tháng (Ngày 1 02:00 AM)
    │   └── dag_fiscal_year_report.py     ← Airflow DAG Báo cáo Năm tài khóa (01/10 03:00 AM)
    └── TICKET-5-GUIDE.md                 ← Hướng dẫn chạy & Interview Q&A
```

---

## 1. Yêu cầu môi trường (Pre-requisites)

1. HDFS Multinode đang chạy (`hdfs://master:9000`).
2. Postgres `fleet_oltp` và Redis đang chạy.
3. Airflow 2.x đã cài đặt và cấu hình `dags_folder = /path/to/fleet-platform/analytics/dags`.

---

## 2. Bước 1: Chạy trực tiếp PySpark SCD Type 2 Pipeline

Trên máy `master`:

```bash
cd /path/to/fleet-platform/analytics/jobs

spark-submit --master local[*] scd2_customer_dimension.py
```

**Output mong đợi:**
```
======================================================================
 Fleet Platform — PySpark SCD Type 2 Pipeline
 Target HDFS Path: hdfs://master:9000/fleet-datalake/warehouse/dim_customer
======================================================================

[SCD2] Writing dimension table to hdfs://master:9000/fleet-datalake/warehouse/dim_customer...
✅ SCD Type 2 Customer Dimension Pipeline completed successfully.

+--------------------------------+-----------+-------------------------------+-----------+--------------+---------------+----------+
|customer_key                    |customer_id|company_name                   |status     |effective_date|expiration_date|is_current|
+--------------------------------+-----------+-------------------------------+-----------+--------------+---------------+----------+
|d41d8cd98f00b204e9800998ecf8427e|1          |Công ty TNHH Vận Tải Phương Nam|thường niên|2026-07-29    |9999-12-31     |true      |
+--------------------------------+-----------+-------------------------------+-----------+--------------+---------------+----------+
```

---

## 3. Bước 2: Chạy Job PySpark Batch Aggregation

Thực thi tính toán báo cáo cho từng tần suất:

```bash
# 1. Tổng hợp Hàng tháng (Monthly)
spark-submit --master local[*] batch_dwh_aggregation.py --granularity monthly

# 2. Tổng hợp Năm tài khóa (Fiscal Year)
spark-submit --master local[*] batch_dwh_aggregation.py --granularity fiscal_year
```

**Output mong đợi:**
```
======================================================================
 Fleet Platform — Batch DWH Aggregation (MONTHLY)
======================================================================

📊 Aggregated Business Metrics:
{
  "granularity": "monthly",
  "generated_at": "2026-07-29 22:18:00",
  "total_revenue": 17050000.0,
  "service_revenue": 2300000.0,
  "parts_revenue": 14750000.0,
  "total_invoices": 4,
  "estimated_profit": 11985000.0,
  "profit_margin_pct": 70.29
}
✅ Published aggregated metrics to Redis key 'report:agg:monthly' and channel 'channel:report-updates'.
```

---

## 4. Bước 3: Kiểm tra Redis Key & Redis Pub/Sub Signal

Mở terminal trên `master` lắng nghe Redis Pub/Sub channel:

```bash
# Terminal 1: Lắng nghe Pub/Sub channel
redis-cli -h master SUBSCRIBE channel:report-updates
```

Khi job `batch_dwh_aggregation.py` chạy xong, Terminal 1 sẽ nhận ngay lập tức message:
```redis
1) "message"
2) "channel:report-updates"
3) "{\"event\": \"REPORT_UPDATED\", \"granularity\": \"monthly\", \"redis_key\": \"report:agg:monthly\", \"total_revenue\": 17050000.0}"
```

Kiểm tra nội dung lưu trữ trong Redis String Key:
```bash
redis-cli -h master GET report:agg:monthly
```

---

## 5. Bước 4: Deploy & Verify Airflow DAGs

1. Copy hoặc symlink thư mục DAGs vào Airflow:
   ```bash
   cp /path/to/fleet-platform/analytics/dags/*.py ~/airflow/dags/
   ```
2. Kiểm tra danh sách DAGs trong Airflow CLI:
   ```bash
   airflow dags list | grep fleet
   ```
   Phải thấy 3 DAGs:
   - `fleet_weekly_report_dag`
   - `fleet_monthly_report_dag`
   - `fleet_fiscal_year_report_dag`

3. Test chạy thử DAG thủ công:
   ```bash
   airflow dags trigger fleet_monthly_report_dag
   ```
