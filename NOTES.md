* **User Objective**: Chuẩn bị hệ thống Production Data Platform tại công ty mới: Ingestion API -> Prefect -> Spark -> Trino / MinIO.
* **Target Stack**:
  - Orchestration: Prefect (Prefect 3)
  - Processing: Apache Spark (ELT, Dynamic JSON Flattening, Dead Letter Queue, Incremental MERGE)
  - Storage/Query: MinIO (Raw backup) + Trino (Personal/Dev DB + Production/Cleaned DB trên Iceberg)
  - Architecture: Medallion (Bronze/Raw -> Silver/Cleaned -> Gold), Data Quality Rule Engine, SCD Type 2 + Late-Arriving Facts handling.
* **Teaching Strategy**: 
  - Đóng vai Senior Data Engineer thực chiến, tư duy kiến trúc production, code chuẩn mực, bám sát các bẫy scale & concurrency.
  - Cung cấp Blueprint kiến trúc, checklist câu hỏi làm việc với team, và code template PySpark chạy mẫu.
