-- Override thân model qtsx_v2_dim_date (dbtgen.load_overrides đọc file này).
--
-- Thân SQL nằm ở VIEW `hive.pbi_silver.qtsx_v2_dim_date_calc`, xem
--     powerbi/QTSX V2-parquet/create_views_trino.sql
-- Chạy file đó TRƯỚC `dbt run`. Model này chỉ còn `select *` để dimension/metric
-- trong __schema.yml có chỗ bám, đúng khuôn các model sinh tự động.
--
-- Dim_Date là DAX CALENDAR + calculated column -> bảng silver 2.222 dòng nhưng
-- 12/15 cột RỖNG, mà 50 chart dùng bảng này. View dựng lại toàn bộ lịch.

select *
from {{ source('pbi_silver_views__qtsx_v2',
               'qtsx_v2_dim_date_calc') }}
