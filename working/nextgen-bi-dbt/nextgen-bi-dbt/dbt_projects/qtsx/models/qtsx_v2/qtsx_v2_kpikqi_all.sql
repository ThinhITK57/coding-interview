-- Override thân model qtsx_v2_kpikqi_all (dbtgen.load_overrides đọc file này).
--
-- Thân SQL nằm ở VIEW `hive.pbi_silver.qtsx_v2_kpikqi_all_calc`, xem
--     powerbi/QTSX V2-parquet/create_views_trino.sql
-- Chạy file đó TRƯỚC `dbt run`. Model này chỉ còn `select *` để dimension/metric
-- trong __schema.yml có chỗ bám, đúng khuôn các model sinh tự động.
--
-- KPIKQI_ALL = UNION('KPIKQI tuần Jira', FILTER('KPIKQI_cloudrity', ...),
-- KPIKQI_TKCG_Report) + 7 calculated column. 9 chart dùng bảng này.
-- Silver: 8.122 dòng nhưng 12/19 cột RỖNG (do UNION/SELECTCOLUMNS).
-- View dựng lại đủ CẢ BA nhánh (bản sql_overrides trước đây thiếu nhánh TKCG =
-- 2.187 dòng, toàn bộ Trung tâm 'TT.SI').

select *
from {{ source('pbi_silver_views__qtsx_v2',
               'qtsx_v2_kpikqi_all_calc') }}
