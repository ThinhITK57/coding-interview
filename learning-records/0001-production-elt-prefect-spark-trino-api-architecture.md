# 0001: Chuyen dich Kien truc sang Production ELT voi Prefect, Spark, Trino va MinIO

Nguoi hoc bat dau cong viec moi tai moi truong Enterprise thuc te voi kien truc ELT hoan toan moi: Orchestration bang Prefect, Ingestion tu REST API (JSON long nhau, rate limits), Data Quality metadata rules voi Dead Letter Queue (DLQ), luu tru da dich (Trino Sandbox Dev, Trino Production Cleaned, va MinIO raw backup), cung bai toan concurrency khi Facts va Dimensions cap nhat dong thoi trong cua so thoi gian hep.

Dieu nay thay doi toan bo trong tam dao tao tu cac bai tap ly thuyet/cum co dinh sang ky nang giai quyet bai toan van hanh Production: Decoupling I/O vs Compute, Snapshot Isolation (Iceberg ACID MERGE), Concurrency Control, va Idempotent Processing.
