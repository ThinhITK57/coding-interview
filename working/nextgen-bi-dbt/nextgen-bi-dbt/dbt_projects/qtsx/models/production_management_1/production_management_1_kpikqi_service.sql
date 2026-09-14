-- Override thân model production_management_1_kpikqi_service (dbtgen.load_overrides đọc file này).
--
-- Thân SQL nằm ở VIEW `hive.pbi_silver.production_management_1_kpikqi_service_calc`, xem
--     powerbi/Quản trị sản xuất (1)-parquet/create_views_trino.sql
-- Chạy file đó TRƯỚC `dbt run`. Model này chỉ còn `select *` để dimension/metric
-- trong __schema.yml có chỗ bám, đúng khuôn các model sinh tự động.
--
-- Bảng 'KPIKQI dịch vụ' CÓ 6.119 dòng dữ liệu thật, nhưng 8 calculated column
-- RỖNG, trong đó `evaluation` và `color` bị 2 metric của Lightdash dùng (2 chart).
-- View đọc lại chính bảng này và tính bù cả 8 cột theo chuỗi DAX
-- [So với target] -> [Đánh giá] / [Màu sắc] -> [Cảnh báo KPI].

select *
from {{ source('pbi_silver_views__production_management_1',
               'production_management_1_kpikqi_service_calc') }}
