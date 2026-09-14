-- Override thân model qtsx_v2_kpikqi_tkcg_report (dbtgen.load_overrides đọc file này).
--
-- Thân SQL nằm ở VIEW `hive.pbi_silver.qtsx_v2_kpikqi_tkcg_report_calc`, xem
--     powerbi/QTSX V2-parquet/create_views_trino.sql
-- Chạy file đó TRƯỚC `dbt run`. Model này chỉ còn `select *` để dimension/metric
-- trong __schema.yml có chỗ bám, đúng khuôn các model sinh tự động.
--
-- KPIKQI_TKCG_Report = UNION(_Week, _Month, _Quater) trên 'KPIKQI Dịch vụ TKCG'
-- (16.771 dòng, CÓ dữ liệu). Silver: 0 dòng. Đây cũng là nhánh thứ 3 mà
-- qtsx_v2_kpikqi_all trước đây còn thiếu.

select *
from {{ source('pbi_silver_views__qtsx_v2',
               'qtsx_v2_kpikqi_tkcg_report_calc') }}
