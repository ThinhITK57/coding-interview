# 06: Kiểm thử Tích hợp End-to-End và Xác thực Tính toàn vẹn Dữ liệu

**What to build:** Kiểm tra toàn bộ luồng dữ liệu từ Bronze (`epm_raw_snapshot`) ➔ Silver (`bi_silver.epm_*`) ➔ Dims/Facts ➔ Data Marts (`bi_gold.*`) ➔ dbt Models & Tests. Đảm bảo tính nhất quán và không có lỗi schema.

**Blocked by:** 05: Refactor Model dbt và Schema Semantic tương thích với Data Marts Mới

**Status:** ready-for-agent

- [ ] Toàn bộ chuỗi ETL PySpark chạy trơn tru không lỗi.
- [ ] `dbt compile` và `dbt test` hoàn thành thành công không lỗi syntax hoặc missing source.
- [ ] Khóa ngoại và các trường bắt buộc không bị NULL bất thường.
