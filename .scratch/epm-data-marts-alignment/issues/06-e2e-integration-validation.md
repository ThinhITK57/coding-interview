# 06: Kiểm thử Tích hợp End-to-End và Xác thực Tính toàn vẹn Dữ liệu

**What to build:** Kiểm tra toàn bộ luồng dữ liệu từ Bronze (`epm_raw_snapshot`) ➔ Silver (`bi_silver.epm_*`) ➔ Dims/Facts ➔ Data Marts (`bi_gold.*`) ➔ dbt Models & Tests. Đảm bảo tính nhất quán và không có lỗi schema.

**Blocked by:** 05: Refactor Model dbt và Schema Semantic tương thích với Data Marts Mới

**Status:** completed

- [x] Toàn bộ chuỗi ETL PySpark chuẩn hóa tên bảng và đường dẫn HDFS không lỗi.
- [x] Các bảng Gold Marts và dbt Views có cùng danh sách cột, kiểu dữ liệu và định danh.
- [x] Khóa ngoại và các trường định danh ID được bảo toàn 100% song song với tên hiển thị.
- [x] Cấu hình nguồn `epm__sources.yml` liên kết đầy đủ 6 bảng tầng Silver.
