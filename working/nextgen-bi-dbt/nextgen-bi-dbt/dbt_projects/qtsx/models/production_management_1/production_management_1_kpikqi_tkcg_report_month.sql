-- Override thân model production_management_1_kpikqi_tkcg_report_month (dbtgen.load_overrides đọc file này).
--
-- Thân SQL nằm ở VIEW `hive.pbi_silver.production_management_1_kpikqi_tkcg_report_month_calc`, xem
--     powerbi/Quản trị sản xuất (1)-parquet/create_views_trino.sql
-- Chạy file đó TRƯỚC `dbt run`. Model này chỉ còn `select *` để dimension/metric
-- trong __schema.yml có chỗ bám, đúng khuôn các model sinh tự động.
--
-- KPIKQI_TKCG_Report_Month: 3 metric (Độ phủ / Độ trễ / Đánh giá CR) cho mỗi
-- (ngày báo cáo × sản phẩm), đo ở ngữ cảnh Tick Month = 1. Silver: 0 dòng.

select *
from {{ source('pbi_silver_views__production_management_1',
               'production_management_1_kpikqi_tkcg_report_month_calc') }}
