-- Override thân model production_management_1_n_5_vvip_incident_cause_id (dbtgen.load_overrides đọc file này).
--
-- Thân SQL nằm ở VIEW `hive.pbi_silver.production_management_1_n_5_vvip_incident_cause_id_calc`, xem
--     powerbi/Quản trị sản xuất (1)-parquet/create_views_trino.sql
-- Chạy file đó TRƯỚC `dbt run`. Model này chỉ còn `select *` để dimension/metric
-- trong __schema.yml có chỗ bám, đúng khuôn các model sinh tự động.
--
-- Bảng '5VVIP_Nguyên nhân sự cố ID' có 185 dòng thật, thiếu duy nhất cột
-- [Customer] = LOOKUPVALUE('5VVIP_PRM_Companies'[VVIP], ... [ISSUE_ID]).
-- 1 chart dùng cột này làm dimension.

select *
from {{ source('pbi_silver_views__production_management_1',
               'production_management_1_n_5_vvip_incident_cause_id_calc') }}
