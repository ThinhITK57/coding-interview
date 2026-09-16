# 03: Chuẩn hóa tầng Star Schema (Conformed Dimensions & Snapshot Facts)

**What to build:** Nâng cấp `epm_dims.py` và `epm_facts.py` trong `working/code-need-done/etl-zeppline-jobs/etls/bi_silver/epm/`. Bảng Fact lưu trữ đầy đủ các Surrogate / Foreign Keys trỏ sang bảng Dim tương ứng (`department_key`, `assignee_key`, `assignor_key`, `project_key`, `objective_key`). Sửa chính xác logic fallback của `c_update_description`, `c_epm_default` và kiểu dữ liệu của `resources`.

**Blocked by:** 02: Đồng bộ tên bảng tầng Silver PySpark với dbt Source (bi_silver.epm_*)

**Status:** ready-for-agent

- [ ] `epm_dims.py` tạo đủ 6 Conformed Dimensions: `dim_epm_department`, `dim_epm_resource`, `dim_epm_project`, `dim_epm_task`, `dim_epm_objective`, `dim_epm_assignment`.
- [ ] `epm_facts.py` tạo 4 Snapshot Facts có đầy đủ Foreign Keys trỏ sang Dim.
- [ ] Sửa fallback `update_description` thành `COALESCE(c_update_description, overview)` và `epm_default` thành `COALESCE(c_epm_default, default_integration_path)`.
- [ ] Xử lý ép kiểu `STRING` cho `resources` khi lấy từ `resources_and_placeholders_count`.
