# 02: Đồng bộ tên bảng tầng Silver PySpark với dbt Source (bi_silver.epm_*)

**What to build:** Cập nhật các script PySpark Bronze-to-Silver trong `working/code-need-done/etl-zeppline-jobs/etls/epm_silver/` để lưu bảng với tiền tố chuẩn `bi_silver.epm_*` (`epm_tasks`, `epm_projects`, `epm_assignments`, `epm_objectives`, `epm_targets`), khớp hoàn hảo với `epm__sources.yml` của dbt.

**Blocked by:** 01: Chuẩn hóa quy ước đặt tên Data-Driven và Đặc tả Hợp đồng Dữ liệu

**Status:** ready-for-agent

- [ ] Cập nhật `tasks.py`, `projects.py`, `assignments.py`, `objectives.py`, `targets.py` lưu vào bảng `bi_silver.epm_<entity>`.
- [ ] Đường dẫn HDFS/Parquet đồng bộ về `/opt/datasets/crawlers/vcs_silver/bi_silver/data/epm_<entity>`.
- [ ] Khớp 100% với danh mục bảng trong `dbt_projects/epm/models/epm__sources.yml`.
