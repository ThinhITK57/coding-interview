# %livy.pyspark

# Thành phần: cm_catalog, cm_pricebook
spark.catalog.clearCache()

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
spark.sql("REFRESH TABLE crm_raw.cm_catalog")
spark.sql("REFRESH TABLE crm_raw.cm_pricebook")

tgt_table = "bi_silver.crm_product_category"
tgt_path  = "s3a://bi-silver/crm_product_category"

# spark.sql("DROP TABLE IF EXISTS bi_silver.crm_product_category")

query = """
WITH mapping_cate AS (
    SELECT * FROM (VALUES 
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
        ('VCS029', 'Diễn tập', 'Nhóm Professional Services', NULL),
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
    ) AS t (category_code, product_name, group_name, sub_group_name)
),
final_mapping AS (
    SELECT 
        product_name,
        MIN(category_code) as category_code,
        MIN(group_name) as group_name,
        MIN(sub_group_name) as sub_group_name
    FROM mapping_cate
    GROUP BY product_name
),
rawProductTree AS (
    SELECT DISTINCT
        m.category_code                 AS category_code,
        b.custom_field.cf_category      AS product_category
    FROM crm_raw.cm_pricebook s
    LEFT JOIN crm_raw.cm_catalog b 
        ON s.custom_field.cf_catalog = b.id
    LEFT JOIN final_mapping m 
        ON TRIM(b.custom_field.cf_category) = m.product_name
)
SELECT 
    distinct
    pd.category_code,
    pd.product_category
FROM rawProductTree pd
"""

df = spark.sql(query)

# 1) Drop table (KHÔNG dùng f-string để tránh lỗi Python2)
# spark.sql("DROP TABLE IF EXISTS " + tgt_table)

# 2) Write
df.repartition(1).write \
  .mode("overwrite") \
  .format("parquet") \
  .option("path", tgt_path) \
  .saveAsTable(tgt_table)

spark.catalog.refreshTable(tgt_table)

print(tgt_table)
print("Rows:", df.count())

