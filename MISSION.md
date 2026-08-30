# MISSION: Build & Master the Fleet Maintenance & Repair Data Platform

## Objective
Build a complete, production-grade end-to-end Data Platform for Fleet Maintenance & Repair based on the project specification (`de-interview-prep-fleet-platform.md`).

## Key Learning Outcomes for Job Interviews
1. **Infrastructure & Setup**: Configure Docker Compose with Kafka, Debezium CDC, PostgreSQL (Odoo OLTP), Redis, MinIO (Data Lake), Spark, Airflow, and WebSocket Backend.
2. **Stream Processing (Spark Structured Streaming)**: Build real-time ingestion pipelines consuming Kafka telemetry & repair requests, writing raw Parquet to Data Lake, and extracting location/issue payloads.
3. **CDC & Redis Serving Store**: Deploy Debezium log-based CDC to replicate Odoo PostgreSQL changes into Kafka, and build a sync worker updating Redis GEO (Head search) and Redis Hash (inventory/prices). Implement atomic slot reservation (Lua/TTL).
4. **Data Warehousing & Star Schema (Spark Batch + Airflow)**: Model facts (`fact_repair_service_revenue`, `fact_parts_sales`) and dimensions (`dim_customer` with SCD Type 2, `dim_head`, `dim_component`, `dim_date`). Orchestrate DAGs for weekly, monthly, quarterly, fiscal-year, and year-end reports.
5. **Push-Based Serving Layer**: Build a WebSocket backend subscribing to Redis Pub/Sub channels to push newly calculated aggregates down to dashboards instantly.
6. **Interview Mastery**: Be able to defend every architectural choice, explain trade-offs (e.g. why Debezium over DB Polling, why Redis Multi-Role, why SCD Type 2, why Star Schema), and white-board the entire data flow seamlessly.
