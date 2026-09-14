-- Model viết tay (không có trong mapping.yml nên run.py không ghi đè).
--
-- Thân SQL nằm ở VIEW `hive.pbi_silver.production_management_1_mm_ratio_by_project_month`, xem
--     powerbi/Quản trị sản xuất (1)-parquet/create_views_trino.sql
-- Chạy file đó TRƯỚC `dbt run`. Model này chỉ còn `select *` để dimension/metric
-- trong __schema.yml có chỗ bám, đúng khuôn các model sinh tự động.
--
-- 10 bảng gộp về grain (tháng × khối × sản phẩm) rồi ghép ngang, cho các measure
-- "chia khối lượng cho man-month" mà khai join thẳng thì bị fan-out.
--
-- GRAIN: (tháng × khối × sản phẩm) — 1 dòng duy nhất mỗi tổ hợp.

select *
from {{ source('pbi_silver_views__production_management_1',
               'production_management_1_mm_ratio_by_project_month') }}
