-- Model viết tay (không có trong mapping.yml nên run.py không ghi đè).
--
-- Thân SQL nằm ở VIEW `hive.pbi_silver.qtsx_v2_mm_ratio_by_project_month`, xem
--     powerbi/QTSX V2-parquet/create_views_trino.sql
-- Chạy file đó TRƯỚC `dbt run`. Model này chỉ còn `select *` để dimension/metric
-- trong __schema.yml có chỗ bám, đúng khuôn các model sinh tự động.
--
-- 10 bảng gộp về grain (YearMonth × sản phẩm) rồi ghép ngang, cho toàn bộ measure
-- "chia cho man-month" và các thẻ đếm "n SP Passed / n SP Failed".
--
-- GRAIN: (YearMonth × sản phẩm) — 1 dòng mỗi tổ hợp, không fan-out được.

select *
from {{ source('pbi_silver_views__qtsx_v2',
               'qtsx_v2_mm_ratio_by_project_month') }}
