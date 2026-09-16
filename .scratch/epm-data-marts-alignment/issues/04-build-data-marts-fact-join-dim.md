# 04: Xây dựng 6 Data Marts PySpark theo Tên Miền Dữ Liệu và Enrich Tên Hiển Thị (Fact JOIN Dim)

**What to build:** Triển khai 6 script PySpark Data Mart tại `working/code-need-done/etl-zeppline-jobs/etls/bi_silver/epm/` tương ứng với 6 tên miền nghiệp vụ: `mart_bsc_yearly.py`, `mart_cvct_execution.py`, `mart_task_report.py`, `mart_project_report.py`, `mart_user_access_traffic.py`, `mart_board_objectives.py`. Mỗi mart JOIN Fact với các Dim để cung cấp song hành Khóa ID và Tên hiển thị làm giàu.

**Blocked by:** 03: Chuẩn hóa tầng Star Schema (Conformed Dimensions & Snapshot Facts)

**Status:** ready-for-agent

- [ ] 6 file script Data Mart được đặt tên chuẩn theo nghiệp vụ (không chứa tiền tố `br01` - `br06`).
- [ ] Mỗi Data Mart thực hiện JOIN Fact với Dim tương ứng, giữ nguyên 100% các cột Khóa ID và bổ sung các cột Tên hiển thị.
- [ ] Có đầy đủ docstring và comment giải thích logic nghiệp vụ cho từng cột.
