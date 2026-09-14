-- Override thân model production_management_1_period_selector (dbtgen.load_overrides đọc file này).
--
-- Thân SQL nằm ở VIEW `hive.pbi_silver.production_management_1_period_selector_calc`, xem
--     powerbi/Quản trị sản xuất (1)-parquet/create_views_trino.sql
-- Chạy file đó TRƯỚC `dbt run`. Model này chỉ còn `select *` để dimension/metric
-- trong __schema.yml có chỗ bám, đúng khuôn các model sinh tự động.
--
-- PeriodSelector = UNION(DISTINCT(Dim_Date[Tuần]), DISTINCT(Dim_Date[MonthYear])),
-- bảng slicer chọn kỳ. Silver: 0 dòng.

select *
from {{ source('pbi_silver_views__production_management_1',
               'production_management_1_period_selector_calc') }}
