-- Override thân model production_management_1_dim_date (dbtgen.load_overrides đọc file này).
--
-- Thân SQL nằm ở VIEW `hive.pbi_silver.production_management_1_dim_date_calc`, xem
--     powerbi/Quản trị sản xuất (1)-parquet/create_views_trino.sql
-- Chạy file đó TRƯỚC `dbt run`. Model này chỉ còn `select *` để dimension/metric
-- trong __schema.yml có chỗ bám, đúng khuôn các model sinh tự động.
--
-- Dim_Date là DAX CALENDAR(2024-12-01, 2030-12-31) + 16 calculated column ->
-- bảng silver có 2.222 dòng nhưng 11/17 cột RỖNG, mà 120 chart dùng bảng này.
-- View dựng lại toàn bộ lịch bằng SQL.

select *
from {{ source('pbi_silver_views__production_management_1',
               'production_management_1_dim_date_calc') }}
