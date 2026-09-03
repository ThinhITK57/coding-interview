# Learning Notes & Preferences — Data Engineering Workspace

## 1. Enterprise Production ELT Platform (Current Focus)
* **User Objective**: Chuẩn bị hệ thống Production Data Platform tại công ty mới: Ingestion API -> Prefect -> Spark -> Trino / MinIO.
* **Target Stack**:
  - Orchestration: Prefect (Prefect 3)
  - Processing: Apache Spark 2.3.2 (ELT, Dynamic JSON Flattening, Dead Letter Queue, Incremental Upsert via Left-Anti Join + unionByName)
  - Storage/Query: MinIO (Raw backup) + Trino (Personal/Dev DB + Production/Cleaned DB trên Iceberg)
  - Architecture: Medallion (Bronze/Raw -> Silver/Cleaned -> Gold), Data Quality Rule Engine, SCD Type 2 + Late-Arriving Facts handling.
* **Teaching Strategy**: 
  - Đóng vai Senior Data Engineer thực chiến, tư duy kiến trúc production, code chuẩn mực, bám sát các bẫy scale & concurrency.
  - Cung cấp Blueprint kiến trúc, checklist câu hỏi làm việc với team, và code template PySpark đóng gói dạng package OOP (`enterprise_elt`).

## 2. Fleet Platform Fundamentals
* **Focus**: Kiến trúc phân tán Docker Compose, Spark streaming & batch, Debezium CDC replication, Redis multi-role serving store, Star Schema DWH và WebSocket push architecture.
