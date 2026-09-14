-- Override thân model production_management_1_customer_summary (dbtgen.load_overrides đọc file này).
--
-- Thân SQL nằm ở VIEW `hive.pbi_silver.production_management_1_customer_summary_calc`, xem
--     powerbi/Quản trị sản xuất (1)-parquet/create_views_trino.sql
-- Chạy file đó TRƯỚC `dbt run`. Model này chỉ còn `select *` để dimension/metric
-- trong __schema.yml có chỗ bám, đúng khuôn các model sinh tự động.
--
-- Customer_Summary = SUMMARIZE('KPIKQI_New', 6 cột khoá, 4 cột tính toán) trong đó
-- có CONCATENATEX. Silver: 0 dòng.

select *
from {{ source('pbi_silver_views__production_management_1',
               'production_management_1_customer_summary_calc') }}
