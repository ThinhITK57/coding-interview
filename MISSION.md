# Mission: Production Enterprise ELT Platform (Prefect, Spark, Trino, MinIO)

## Why
Thiết kế, xây dựng và tối ưu hệ thống ELT data pipeline thực chiến tại công ty mới: Kéo dữ liệu API (4 mục tiêu bảo mật), làm phẳng JSON phi cấu trúc, kiểm soát chất lượng dữ liệu với Dead Letter Queue (DLQ), lưu trữ đa mục tiêu (MinIO backup + Trino Dev/Prod) và xử lý đồng thời (concurrency) giữa Fact & Dimension.

## Success looks like
- Thiết kế luồng pull API an toàn, kiểm soát connection pool và rate limiting, không gây nghẽn hệ thống.
- Pipeline Spark xử lý làm phẳng JSON động và thực thi bộ Metadata Quality Rules tách luồng hợp lệ vs DLQ tự động.
- Ghi dữ liệu đồng thời vào 3 đích: MinIO Raw Backup, Trino Database Cá nhân (Sandbox/Dev), và Trino Database Tổng (Cleaned/Prod) với chi phí I/O tối ưu.
- Giải quyết triệt để bài toán đồng thời (Concurrency / Race Condition) khi Fact và Dimension cập nhật cùng lúc trong cửa sổ thời gian hẹp.
- Trả lời và bảo vệ phương án kiến trúc tự tin, sắc sảo trong buổi họp kỹ thuật ngày mai.

## Constraints
- Orchestrator bắt buộc: Prefect.
- Dữ liệu API JSON lồng nhau, trường động phi cấu trúc.
- Tuân thủ bảo mật doanh nghiệp (4 mục tiêu mã hóa nội bộ).
- Trino làm query engine trung tâm, MinIO làm object storage backup.

## Out of scope
- Cài đặt hạ tầng vật lý ban đầu (đã có team DevOps / Cloud cung cấp môi trường).
- Frontend visualization dashboard (để team BI phụ trách).
