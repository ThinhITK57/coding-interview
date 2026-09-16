# 05: Refactor Model dbt và Schema Semantic tương thích với Data Marts Mới

**What to build:** Đổi tên các file model `.sql` và `__schema.yml` trong `dbt_projects/epm/models/` từ tiền tố `br01_...` sang tên nghiệp vụ chuẩn (`bsc_yearly.sql`, `cvct_execution_report.sql`...). Cập nhật schema YAML với các cột Tên hiển thị mới và cấu hình Semantic Layer.

**Blocked by:** 04: Xây dựng 6 Data Marts PySpark theo Tên Miền Dữ Liệu và Enrich Tên Hiển Thị (Fact JOIN Dim)

**Status:** completed

- [x] Đổi tên 6 file `.sql` và 6 file `__schema.yml` sang tên nghiệp vụ chuẩn không chứa `br01`-`br06`.
- [x] Cập nhật alias view trong `bi_gold`: `vw_bsc_yearly`, `vw_cvct_execution_report`, `vw_task_report`, `vw_project_report`, `vw_user_access_traffic`, `vw_board_objectives`.
- [x] Cập nhật metadata, documentation và metrics trong các file `__schema.yml`.
- [x] Bổ sung model staging `epm_user_access_log.sql` và schema tương ứng.
- [x] Xóa bỏ triệt để các file cũ mang tiền tố `br01` - `br06`.
