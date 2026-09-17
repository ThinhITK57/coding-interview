%livy.pyspark
spark.catalog.clearCache()
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

spark.sql("REFRESH TABLE crm_raw.cm_pricebook")
spark.sql("REFRESH TABLE crm_raw.deleted_cm_pricebook")
spark.sql("REFRESH TABLE crm_raw.cm_catalog")

tgt_table = "bi_silver.crm_pricebook"
tgt_path  = "/opt/datasets/crawlers/vcs_silver/bi_silver/data/crm_pricebook"

sql_query = """
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
        s.id                    AS product_id,
        b.name                    AS product_name,
        s.name                    AS product_description,
        m.group_name                    AS product_group,
        COALESCE(m.sub_group_name, 'Others')  AS product_sub_group,
        m.category_code                 AS category_code,
        b.custom_field.cf_category      AS product_category,
        COALESCE(s.custom_field.cf_sku, '-')  AS sku,
        b.custom_field.cf_version       AS product_version,
        b.custom_field.cf_item_type     AS item_type,
        b.custom_field.cf_active     AS is_active,
        COALESCE(s.custom_field.cf_type,'-')       AS product_type,
        COALESCE(s.custom_field.cf_sub_type,'-')       AS sub_type, 
        COALESCE(s.custom_field.cf_license, 'N/A')       AS product_license,
        COALESCE(s.custom_field.cf_price_type, 'N/A' )   AS price_type,
        COALESCE(s.custom_field.cf_package, 'N/A')       AS package,
        CAST(s.custom_field.cf_price AS DECIMAL(18,2)) AS price,
        COALESCE(s.custom_field.cf_currency, 'VND')      AS currency_code,
        CAST(COALESCE(s.custom_field.cf_min, 0) AS DECIMAL(18,2)) AS price_min,
        CAST(COALESCE(s.custom_field.cf_max, 0) AS DECIMAL(18,2)) AS price_max,
        s.custom_field.cf_unit          AS price_unit,
        CAST(s.custom_field.cf_is_quantity_based AS BOOLEAN) AS is_quantity_based,
        CAST(s.custom_field.cf_is_related_csmp   AS BOOLEAN) AS is_related_csmp,
        CAST(COALESCE(s.custom_field.cf_csmp_discount, 0)        AS DECIMAL(18,2)) AS csmp_discount,
        CAST(COALESCE(s.custom_field.cf_csmp_discount_silver, 0) AS DECIMAL(18,2)) AS csmp_discount_silver,
        CAST(COALESCE(s.custom_field.cf_csmp_discount_gold, 0)   AS DECIMAL(18,2)) AS csmp_discount_gold,
        CAST(COALESCE(s.custom_field.cf_csmp_discount_diamond, 0) AS DECIMAL(18,2)) AS csmp_discount_diamond,
        TO_TIMESTAMP(TO_DATE(s.created_at)) AS created_at,
        TO_TIMESTAMP(TO_DATE(s.updated_at)) AS updated_at
    FROM crm_raw.cm_pricebook s
    LEFT JOIN crm_raw.cm_catalog b 
        ON s.custom_field.cf_catalog = b.id
    LEFT JOIN final_mapping m 
        ON TRIM(b.custom_field.cf_category) = m.product_name
)
SELECT 
   pd.*,
   pdc.item_code,
   -- Tạo idd từ các cột của pd (rawProductTree)
   CONCAT(
        COALESCE(CAST(pd.product_group AS STRING), ''), '__',
        COALESCE(CAST(pd.product_sub_group AS STRING), ''), '__',
        COALESCE(CAST(pd.category_code AS STRING), ''), '__',
        COALESCE(CAST(pd.product_category AS STRING), ''), '__',
        COALESCE(CAST(pd.sku AS STRING), ''), '__',
        COALESCE(CAST(pd.product_version AS STRING), ''), '__',
        COALESCE(CAST(pd.item_type AS STRING), ''), '__',
        COALESCE(CAST(pd.product_license AS STRING), ''), '__',
        COALESCE(CAST(pd.price_type AS STRING), ''), '__',
        COALESCE(CAST(pd.price_unit AS STRING), '')
    ) AS idd
FROM rawProductTree pd
LEFT JOIN bi_silver.crm_product_tree_item_code pdc 
    ON pdc.idd = CONCAT(
        COALESCE(CAST(pd.product_group AS STRING), ''), '__',
        COALESCE(CAST(pd.product_sub_group AS STRING), ''), '__',
        COALESCE(CAST(pd.category_code AS STRING), ''), '__',
        COALESCE(CAST(pd.product_category AS STRING), ''), '__',
        COALESCE(CAST(pd.sku AS STRING), ''), '__',
        COALESCE(CAST(pd.product_version AS STRING), ''), '__',
        COALESCE(CAST(pd.item_type AS STRING), ''), '__',
        COALESCE(CAST(pd.product_license AS STRING), ''), '__',
        COALESCE(CAST(pd.price_type AS STRING), ''), '__',
        COALESCE(CAST(pd.price_unit AS STRING), '')
    )
ORDER BY pd.product_group, pd.product_category ASC
"""

df = spark.sql(sql_query)

# # DROP TABLE + hard delete path để chống double file
# spark.sql("DROP TABLE IF EXISTS " + tgt_table)


# Write
df.repartition(1).write \
  .mode("overwrite") \
  .format("parquet") \
  .save(tgt_path)
#   .option("path", tgt_path) \
#   .saveAsTable(tgt_table)

spark.catalog.refreshTable(tgt_table)

print(tgt_table)
print("Rows:", df.count())
