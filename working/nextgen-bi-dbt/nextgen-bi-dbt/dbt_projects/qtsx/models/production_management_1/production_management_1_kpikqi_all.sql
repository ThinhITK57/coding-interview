-- Override thân model production_management_1_kpikqi_all (dbtgen.load_overrides đọc file này).
--
-- Thân SQL nằm ở VIEW `hive.pbi_silver.production_management_1_kpikqi_all_calc`, xem
--     powerbi/Quản trị sản xuất (1)-parquet/create_views_trino.sql
-- Chạy file đó TRƯỚC `dbt run`. Model này chỉ còn `select *` để dimension/metric
-- trong __schema.yml có chỗ bám, đúng khuôn các model sinh tự động.
--
-- KPIKQI_ALL = UNION(KPIKQI_TKCG_Report, FILTER(KPIKQI_NEW, TickKPI=1),
-- KPI_BoLotTanCong). 8 chart dùng bảng này.
--
-- Silver: 0 DÒNG — và bản gốc Power BI cũng vậy: DAX nhánh 1 trỏ
-- KPIKQI_TKCG_Report[SPDV] nhưng bảng đó sinh ra cột tên "SP", không có "SPDV",
-- nên calculated table lỗi và rỗng ngay trong .pbix (riêng nhánh 2 đã phải có
-- 39.984 dòng). View sửa chỗ đó: nhánh 1 lấy SPDV = [SP] (= 'TKCG').

select *
from {{ source('pbi_silver_views__production_management_1',
               'production_management_1_kpikqi_all_calc') }}
