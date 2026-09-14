-- Model viết tay (không có trong mapping.yml nên run.py không ghi đè).
--
-- Thân SQL nằm ở VIEW `hive.pbi_silver.production_management_1_defect_rate_by_month`, xem
--     powerbi/Quản trị sản xuất (1)-parquet/create_views_trino.sql
-- Chạy file đó TRƯỚC `dbt run`. Model này chỉ còn `select *` để dimension/metric
-- trong __schema.yml có chỗ bám, đúng khuôn các model sinh tự động.
--
-- Điểm defect và man-month gộp về grain THÁNG (KHÔNG giữ khối), phục vụ measure
-- '% Tỉ lệ defect' vốn dùng REMOVEFILTERS('Khối') ở cả tử lẫn mẫu.

select *
from {{ source('pbi_silver_views__production_management_1',
               'production_management_1_defect_rate_by_month') }}
