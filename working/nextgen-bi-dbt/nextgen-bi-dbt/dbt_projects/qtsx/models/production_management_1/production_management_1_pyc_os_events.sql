-- Model viết tay (không có trong mapping.yml nên run.py không ghi đè).
--
-- Thân SQL nằm ở VIEW `hive.pbi_silver.production_management_1_pyc_os_events`, xem
--     powerbi/Quản trị sản xuất (1)-parquet/create_views_trino.sql
-- Chạy file đó TRƯỚC `dbt run`. Model này chỉ còn `select *` để dimension/metric
-- trong __schema.yml có chỗ bám, đúng khuôn các model sinh tự động.
--
-- Chỗ đặt TẤT CẢ measure USERELATIONSHIP của bảng 'PYC Jira': 26 measure đều là
-- CALCULATE(<agg>, USERELATIONSHIP('Dim_Date'[Date], <một trong SÁU cột ngày>)),
-- mà Lightdash chỉ có 1 join active tới dim_date. View fan-out mỗi PYC thành 1
-- dòng cho mỗi mốc ngày, mỗi metric tự lọc `date_role`.
--
-- GRAIN: (PYC × mốc ngày) — 536 PYC -> 2.237 dòng. Mọi metric trong __schema.yml
-- đều lọc date_role; ĐỪNG thêm metric đếm/cộng không lọc role.

select *
from {{ source('pbi_silver_views__production_management_1',
               'production_management_1_pyc_os_events') }}
