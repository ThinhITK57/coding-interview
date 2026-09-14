-- Override thân model qtsx_v2_dim_customer (dbtgen.load_overrides đọc file này).
--
-- Thân SQL nằm ở VIEW `hive.pbi_silver.qtsx_v2_dim_customer_calc`, xem
--     powerbi/QTSX V2-parquet/create_views_trino.sql
-- Chạy file đó TRƯỚC `dbt run`. Model này chỉ còn `select *` để dimension/metric
-- trong __schema.yml có chỗ bám, đúng khuôn các model sinh tự động.
--
-- Dim_Customer = FILTER(DISTINCT(KPIKQI_nocodb[CustomerName]), NOT ISBLANK(...)).
-- Silver: 0 dòng.

select *
from {{ source('pbi_silver_views__qtsx_v2',
               'qtsx_v2_dim_customer_calc') }}
