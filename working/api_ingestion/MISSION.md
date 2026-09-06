# Mission: Prefect HQ cho API Ingestion Pipeline

## Why
Bạn vừa bắt đầu ở công ty mới, cần dùng Prefect để quản lý, chạy và theo dõi job pull dữ liệu từ API. Có nền Airflow vững, cần chuyển sang Prefect cho pipeline `api_ingestion` thực tế — chạy được, deploy được, theo dõi được trên HQ dashboard.

## Success looks like
- Chạy `prefect_flow.py` locally và thấy kết quả trên Prefect HQ dashboard
- Biết deploy flow thành scheduled job (cron) cho production
- Đọc được logs, artifacts, task retries trên HQ UI khi job chạy/lỗi
- Map được Airflow concepts (DAG, Operator, XCom, Connection) sang Prefect equivalents

## Constraints
- Spark 2.3.2 (enterprise constraint)
- Dùng bộ code `api_ingestion` thực tế, không ví dụ trừu tượng
- Hands-on: muốn chạy thực, không chỉ đọc lý thuyết
- Tiếng Việt preferred

## Out of scope
- Prefect 1.x (chỉ học Prefect 3)
- Airflow migration strategy (chỉ cần mapping để hiểu, không migrate)
- Kubernetes deployment (chưa cần ở bước này)
