-- Model viết tay (không có trong mapping.yml nên run.py không ghi đè).
--
-- Thân SQL nằm ở VIEW `hive.pbi_silver.production_management_1_pyc_snapshot_events`, xem
--     powerbi/Quản trị sản xuất (1)-parquet/create_views_trino.sql
-- Chạy file đó TRƯỚC `dbt run`. Model này chỉ còn `select *` để dimension/metric
-- trong __schema.yml có chỗ bám, đúng khuôn các model sinh tự động.
--
-- Chỗ đặt toàn bộ measure của 'PYC-Snapshot': USERELATIONSHIP hai cột ngày
-- commit, SUMX theo MonthYear, SUMX(VALUES(ISSUE_KEY), MAX(...)).
--
-- GRAIN: (PYC × date_role × tháng) — COUNT(*) chính là
-- SUMX(VALUES(MonthYear), DISTINCTCOUNT(ISSUE_KEY)) của DAX. Mọi metric lọc date_role.

select *
from {{ source('pbi_silver_views__production_management_1',
               'production_management_1_pyc_snapshot_events') }}
