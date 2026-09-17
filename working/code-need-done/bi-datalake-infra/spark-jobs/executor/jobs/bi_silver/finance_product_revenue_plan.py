# %livy.pyspark


# OLD: biz_product_revenue_plan
# NEW: finance_product_revenue_plan

# spark.sql("DROP TABLE IF EXISTS bi_silver.biz_product_revenue_plan")
# spark.sql("DROP TABLE IF EXISTS bi_silver.finance_product_revenue_plan")
spark.sql("REFRESH TABLE finance_raw.product_revenue_plan")
spark.sql("REFRESH TABLE finance_raw.product_revenue_plan_2026")

# =========================
#  PLAN_SPDV_KD - Finally
# =========================
src_spdv_table = "finance_raw.product_revenue_plan"
tgt_spdv_path  = "s3a://bi-silver/finance_product_revenue_plan"
tgt_spdv_table = "bi_silver.finance_product_revenue_plan"

# 1. LOAD VÀ IN SCHEMA NGAY LẬP TỨC
print("--- [FORCED DEBUG] PRINTING SCHEMA FOR: " + src_spdv_table + " ---")
df_src = spark.sql("""
SELECT        
CAST(LAST_DAY(TO_DATE(period, 'yyyy-MM')) as TIMESTAMP) as plan_date,
product_group as business_group_name,
'N/A' as product_group,
product_code,
product_name,
`year` as plan_year,
`month` as plan_month,
currency_code,
CAST(must AS DECIMAL(18,2)) as plan_must_amount,
CAST(0 AS DECIMAL(18,2)) as plan_should_amount,
CAST(nice AS DECIMAL(18,2)) as plan_nice_amount

FROM finance_raw.product_revenue_plan

UNION ALL

SELECT 
CAST(report_date as DATE) as plan_date,
group_name as business_group_name,
subgroup_name as product_group,
service_code as product_code,
service_name as product_name,
CAST(report_year as INT) as plan_year,
CAST(report_month as INT) as plan_month,
amount_unit as currency_code,
CAST(priority_must AS DECIMAL(18,2)) as plan_must_amount,
CAST(priority_should AS DECIMAL(18,2)) as plan_should_amount,
CAST(priority_nice AS DECIMAL(18,2)) as plan_nice_amount

FROM finance_raw.product_revenue_plan_2026

""")

# BỔ SUNG: Dòng này phải chạy trước khi check missing_cols
print("List of columns found: ", df_src.columns)


# 3. Đưa vào TempView để xử lý bằng SQL
df_src.createOrReplaceTempView("raw_spdv")

# 4. TRANSFORM DỮ LIỆU
df_transformed = spark.sql("""
    WITH mapping_cate AS (
        SELECT DISTINCT 
            category_code, 
            product_category, 
            map_group_name, 
            sub_group_name 
        FROM (VALUES 
            ('VCS001', 'MSS', 'Nhóm MSS', 'Hệ sinh thái SOC'),
            ('VCS002', 'VCS-CyM', 'Nhóm MSS', 'Hệ sinh thái SOC'),
            ('VCS003', 'VCS-CyCir', 'Nhóm MSS', 'Hệ sinh thái SOC'),
            ('VCS004', 'VCS-aJiant', 'Nhóm MSS', 'Hệ sinh thái SOC'),
            ('VCS005', 'VCS-NDR', 'Nhóm MSS', 'Hệ sinh thái SOC'),
            ('VCS006', 'VCS-NSM', 'Nhóm MSS', 'Hệ sinh thái SOC'),
            ('VCS007', 'VCS-KIAN', 'Nhóm MSS', 'Hệ sinh thái SOC'),
            ('VCS008', 'VCS-AMA', 'Nhóm MSS', 'Hệ sinh thái SOC'),
            ('VCS009', 'VCS-ESG', 'Nhóm MSS', 'Hệ sinh thái SOC'),
            ('VCS010', 'VCS-WSG', 'Nhóm MSS', 'Hệ sinh thái SOC'),
            ('VCS011', 'VCS-NAC', 'Nhóm MSS', 'Hệ sinh thái SOC'),
            ('VCS012', 'SOC Platform', 'Nhóm MSS', 'Hệ sinh thái SOC'),
            ('VCS013', 'Cloudrity', 'Nhóm MSS', NULL),
            ('VCS014', 'AntiDDos', 'Nhóm Mass', NULL),
            ('VCS015', 'M-Suite', 'Nhóm SP Standalone', NULL),
            ('VCS016', 'VCS-F2DR', 'Nhóm SP đặc thù', NULL),
            ('VCS017', 'VCS-TI', 'Nhóm Mass', NULL),
            ('VCS018', 'VCS-InT', 'Nhóm SP Standalone', NULL),
            ('VCS019', 'Viettel Safe platform', 'Nhóm Mass', NULL),
            ('VCS020', 'LIG', 'Nhóm SP đặc thù', NULL),
            ('VCS021', 'TML', 'Nhóm SP đặc thù', NULL),
            ('VCS022', 'BRG', 'Nhóm SP đặc thù', NULL),
            ('VCS023', 'TAD', 'Nhóm SP đặc thù', NULL),
            ('VCS024', 'VAPT (Pentest)', 'Nhóm Professional Services', NULL),
            ('VCS025', 'Redteam', 'Nhóm Professional Services', NULL),
            ('VCS026', 'Infrastructure Audit', 'Nhóm Professional Services', NULL),
            ('VCS027', 'Tư vấn', 'Nhóm Professional Services', NULL),
            ('VCS028', 'Threat Hunting', 'Nhóm Professional Services', NULL),
            ('VCS029', 'Purple Team Service', 'Nhóm Professional Services', NULL),
            ('VCS029', 'Diễn tập', 'Nhóm Professional Services', NULL),
            ('VCS030', 'CA', 'Nhóm MSS', NULL),
            ('VCS031', 'WAAP Solution', 'Nhóm Mass', NULL),
            ('VCS032', 'TAS (Telco Authen Service)', 'Khác', NULL),
            ('VCS033', 'MDR Service', 'Nhóm MSS', NULL),
            ('VCS034', 'Dịch vụ chuyên nghiệp khác', 'Nhóm Professional Services', NULL),
            ('VCS035', 'VCS-V2S', 'Nhóm Mass', NULL),
            ('VCS036', 'Software', 'Khác', NULL),
            ('VCS037', 'Hardware', 'Nhóm kinh doanh SI', NULL),
            ('VCS038', 'Khác', 'Khác (PC, PM phát sinh)', NULL),
            ('VCS038', 'SI', 'Nhóm kinh doanh SI', NULL),
            ('VCS039', 'Content Security', 'Nhóm MSS', 'Hệ sinh thái SOC'),
            ('VCS040', 'VCS-TI', 'Nhóm Mass', NULL),
            ('VCS041', 'AntiDDos', 'Nhóm Mass', NULL),
            ('VCS042', 'VCS-CyM', 'Nhóm MSS', 'Hệ sinh thái SOC'),
            ('VCS043', 'Lá chắn số (TNXH)', 'Nhóm Mass', NULL),
            ('VCS044', 'Sản phẩm AI', 'Khác', NULL)
        ) AS t (category_code, product_category, map_group_name, sub_group_name)
    )   
    SELECT 
    TO_TIMESTAMP(TO_DATE(r.plan_date)) as plan_date,
    r.plan_year,
    r.plan_month,
    
    r.plan_must_amount,
    r.plan_should_amount,
    r.plan_nice_amount,
    
    r.product_group,
    r.product_code as product_category_code,
    m.product_category,
    r.product_name,
    
    m.map_group_name as business_group_name,
    m.sub_group_name ,
    r.currency_code

    FROM raw_spdv r
    LEFT JOIN mapping_cate m 
        ON TRIM(CAST(r.product_code AS STRING)) = TRIM(m.category_code)
""")

# 5. REPARTITION + OVERWRITE
(df_transformed
    .repartition(1)
    .write
    .mode("overwrite")
    .format("parquet")
    .option("path", tgt_spdv_path)
    .saveAsTable(tgt_spdv_table))

# =========================
# CHECK DỮ LIỆU SAU KHI GHI
# =========================
spark.sql("REFRESH TABLE " + tgt_spdv_table)
print(tgt_spdv_table)