-- Override thân model production_management_1_kpikqi_tkcg_report (dbtgen.load_overrides đọc file này).
--
-- Thân SQL nằm ở VIEW `hive.pbi_silver.production_management_1_kpikqi_tkcg_report_calc`, xem
--     powerbi/Quản trị sản xuất (1)-parquet/create_views_trino.sql
-- Chạy file đó TRƯỚC `dbt run`. Model này chỉ còn `select *` để dimension/metric
-- trong __schema.yml có chỗ bám, đúng khuôn các model sinh tự động.
--
-- KPIKQI_TKCG_Report = UNION(_Week, _Month, _Quater) trên 'KPIKQI Dịch vụ TKCG'
-- (15.537 dòng, CÓ dữ liệu). Silver: 0 dòng. Nhánh _Week tính ngay trong view.

select *
from {{ source('pbi_silver_views__production_management_1',
               'production_management_1_kpikqi_tkcg_report_calc') }}
