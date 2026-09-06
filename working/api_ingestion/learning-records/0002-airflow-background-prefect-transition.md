# User đã có nền Airflow, bắt đầu học Prefect

User có kinh nghiệm thực tế với Apache Airflow (DAGs, Operators, XCom, schedule_interval, Connections, Pools). Chuyển sang Prefect 3 cho công ty mới.

Quan trọng: Khi dạy Prefect, luôn bridge qua Airflow concepts — ví dụ: "giống Airflow Pool nhưng token-bucket based". Không cần giải thích orchestration là gì, chỉ cần mapping sang paradigm mới.

## Implications
- Skip intro-level orchestration concepts (DAG, task dependency, scheduling)
- Focus on Prefect-specific features Airflow không có: Artifacts, Automations, Events, progressive retry delay
- Hands-on trực tiếp trên bộ code `prefect_flow.py` thật, không dùng toy examples
