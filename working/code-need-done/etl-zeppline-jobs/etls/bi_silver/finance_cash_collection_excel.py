%livy.pyspark


# OLD: biz_business_results
# NEW: finance_cash_collection
# NEW: finance_allocated_revenue

# spark.sql("DROP TABLE IF EXISTS bi_silver.biz_business_results")
# spark.sql("DROP TABLE IF EXISTS bi_silver.finance_allocated_revenue")


from pyspark.sql import functions as F

# 1) Spark config
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

src_actual = "finance_raw.actual_revenue"
src_provisional = "finance_raw.provisional_revenue"
tgt_path = "/opt/datasets/crawlers/vcs_silver/bi_silver/data/finance_cash_collection_excel"
tgt_table = "bi_silver.finance_cash_collection_excel"

# 2) Hàm bổ trợ để Union các bảng lệch cột (Dùng cho Spark phiên bản cũ < 3.1)
def union_diff_schema(df1, df2):
    cols1 = set(df1.columns)
    cols2 = set(df2.columns)
    
    # Thêm các cột thiếu vào df1
    for c in cols2 - cols1:
        df1 = df1.withColumn(c, F.lit(None))
    # Thêm các cột thiếu vào df2
    for c in cols1 - cols2:
        df2 = df2.withColumn(c, F.lit(None))
        
    return df1.unionByName(df2)

# Đọc dữ liệu từ 2 nguồn
df_actual = spark.table(src_actual)
df_provisional = spark.table(src_provisional)

# Gộp bảng
df_union = union_diff_schema(df_actual, df_provisional)
df_union.createOrReplaceTempView("raw_data")

# 3) SQL Query đầy đủ logic mapping
sql_query = """
SELECT
  CAST(TO_DATE(report_date) AS timestamp) as report_date,
  YEAR(report_date) as report_year,
  MONTH(report_date) as report_month,

  category_code as product_category_code,
  product_category,
  product_growth_type,
  product_service_group,
  COALESCE(revenue_amount, 0) as revenue_amount ,
  company_alias,

  revenue_type,
  channel_name,
  is_soc,
  department_alias,
  segment_l1,
  segment_l1 as customer_segment_l1,
  segment_l3,
  segment_l3 as customer_segment_l3,
  revenue_group,
  vat,
  payment_stage,
  am_username,
  presale_name,
  currency_code,
  unit_level_1,
  unit_level_1 as business_unit_level_1,
  territory_name

FROM (
  SELECT
    -- Mapping từ Schema nguồn sang Alias trung gian
    CAST(TO_DATE(report_date) as TIMESTAMP)     AS report_date,
    category_code,
    old_category          AS product_category,
    -- Fixed me. nguon bi sai ten
    product_type          AS product_growth_type, 
    product_service_group AS product_service_group,
    CAST(revenue AS DECIMAL(18,2)) AS revenue_amount,
    customer_name         AS company_alias,
    -- product_type          AS revenue_type,
    sales_channel         AS channel_name,
    CASE WHEN is_soc='SOC' THEN true ELSE false END as is_soc,
    department            AS department_alias,
    CASE 
      WHEN revenue_group = 'Quốc tế' THEN 'International'
      WHEN revenue_group = 'Thị trường' THEN 'International'
      WHEN revenue_group = 'Ngoài' THEN 'Khách hàng ngoài'
      WHEN revenue_group = 'Thị trường' THEN 'International'
      WHEN revenue_group = 'SI-Ngoài' THEN 'Khách hàng ngoài'
      WHEN revenue_group = 'Khác' THEN 'Khách hàng ngoài'
      ELSE revenue_group
    END as segment_l1,
    revenue_group       AS revenue_group,
    segment_level_3       AS segment_l3,
    CAST(vat_amount AS DECIMAL(18,2)) AS vat,
    -- payment_stage,
    CASE WHEN account_manager is null or account_manager = '' THEN 'N/A' ELSE account_manager END as am_username,
    CASE WHEN presale_owner is null or presale_owner = '' THEN 'N/A' ELSE presale_owner END as presale_name,
    'VND'                 AS currency_code,
    revenue_allocated_unit as unit_level_1,
    CASE WHEN territory is null or territory = '' THEN 'Others' ELSE territory END as territory_name,
    
    -- Logic Doanh nghiệp/Khách hàng
    CASE
      WHEN (segment_level_3 IS NULL OR segment_level_3 NOT IN ('GOV', 'Province', 'Energy'))
      THEN 1 ELSE 0
    END AS is_private_enterprise,

    CASE WHEN segment_level_3 IN ('BFSI') THEN 1 ELSE 0 END AS is_banking_group,
    CASE WHEN segment_level_3 IN ('Quốc tế') THEN 1 ELSE 0 END AS is_international_client,
    CASE WHEN segment_level_3 IN ('Nội bộ') THEN 1 ELSE 0 END AS is_internal_client,

    CASE
      WHEN sales_channel IN ('Khách hàng - chia sẻ VTS', 'Khách hàng - chia sẻ Innet', 'Khách hàng - chia sẻ IDC',
                             'Khách hàng - chia sẻ CBO', 'Khách hàng - chia sẻ Siêu Việt')
      THEN 1 ELSE 0
    END AS is_partner,

    -- Mapping Revenue Type
    CASE
      WHEN product_type IN ('DT cũ') THEN 'Doanh thu hiện hữu'
      WHEN product_type IN ('DT renew') THEN 'Doanh thu gia hạn'
      WHEN product_type IN ('DT mới', 'DT  mới') THEN 'Doanh thu mới'
      WHEN product_type IN ('Doanh thu upsell', 'DT upsell') THEN 'Doanh thu bán thêm'
      WHEN product_type IN ('DT Upsales') THEN 'Doanh thu bán gia tăng'
      WHEN product_type IN ('SPDV mới') THEN 'Sản phẩm / Dịch vụ mới'
      ELSE 'Khác'
    END AS revenue_type,
    
    -- Mapping Department
    CASE
      WHEN revenue_allocated_unit = 'TTSP TELCO' THEN 'Trung tâm Sản phẩm TELCO'
      WHEN revenue_allocated_unit = 'TTGS' THEN 'Trung tâm Giám sát'
      WHEN revenue_allocated_unit = 'TTDVATTT' THEN 'Trung tâm Dịch vụ An toàn Thông tin'
      WHEN revenue_allocated_unit = 'TTSX &CNTT' THEN 'Trung tâm Sản xuất và Công nghệ Thông tin'
      WHEN revenue_allocated_unit = 'TTSP ENTERPRISE' THEN 'Trung tâm Sản phẩm ENTERPRISE'
      WHEN revenue_allocated_unit = 'TTDVMN' THEN 'Trung tâm Dịch vụ Miền Nam'
      WHEN revenue_allocated_unit = 'TT TI' THEN 'Trung tâm TI'
      WHEN revenue_allocated_unit = 'PKD' THEN 'Phòng Kinh doanh'
      WHEN revenue_allocated_unit = 'KDQT' THEN 'Kinh doanh Quốc tế'
      WHEN revenue_allocated_unit = 'PTTT' THEN 'Phát triển Thị trường'
      ELSE 'Khác'
    END AS unit_level_1_vi,

    -- Mapping Payment Stage
    CASE
      WHEN payment_stage = 'GT' THEN 'Giảm trừ'
      WHEN payment_stage = 'TT' THEN 'Tạm tính'
      WHEN payment_stage = 'PB' THEN 'Phân bổ'
      WHEN payment_stage = 'GTĐS_VTS' THEN 'Giá trị đối soát với đơn vị VTS'
      WHEN payment_stage = 'HD' THEN 'Hóa đơn'
      WHEN payment_stage = 'HĐĐS_VTS' THEN 'Hóa đơn đối soát với đơn vị VTS'
      WHEN payment_stage = 'HD_Phụ thuộc' THEN 'Hóa đơn thuộc diện phụ thuộc'
      WHEN payment_stage IN ('HD_GT', 'GT_HĐ') THEN 'Giảm trừ hóa đơn'
      WHEN payment_stage = 'HD_cũ' THEN 'Hóa đơn cũ'
      WHEN payment_stage = 'Chênh lệch hóa đơn' THEN 'Chênh lệch hóa đơn'
      WHEN payment_stage = 'TT_VCS_Mới' THEN 'Tạm tính giai đoạn thành lập pháp nhân công ty độc lập (từ 04/2025)'
      WHEN payment_stage IN ('HD_VCS_Cũ', 'HD_VCS Cũ') THEN 'Hóa đơn của đơn vị VCS (trước 04/2025)'
      WHEN payment_stage = 'HD_VCS_Mới' THEN 'Hóa đơn của đơn vị VCS (từ 04/2025)'
      WHEN payment_stage IN ('GT_VCS_Mới', 'GT_VCS mới') THEN 'Giá trị giảm trừ của đơn vị VCS (từ 04/2025)'
      WHEN payment_stage = 'GT_VCS_Cũ' THEN 'Giá trị giảm trừ của đơn vị VCS (trước 04/2025)'
      WHEN payment_stage = 'GT_VCS_Mới_Cũ' THEN 'Giá trị giảm trừ của đơn vị VCS'
      ELSE 'Khác'
    END AS payment_stage

  FROM raw_data
) t
"""

# 4) Thực thi SQL
df_final = spark.sql(sql_query)

# 5) Ghi dữ liệu với Repartition(1) để gom file (Optimize cho bảng Silver nhỏ/vừa)
df_final.repartition(1).write \
    .mode("overwrite") \
    .format("parquet") \
    .option("path", tgt_path) \
    .saveAsTable(tgt_table)

# 6) Refresh Metadata
spark.sql("REFRESH TABLE " + tgt_table)
print(tgt_table)
# 7) In kết quả kiểm tra
print("--- SUCCESS: Data Processed and Saved ---")
# df_final.select("report_date", "company_alias", "revenue_amount", "payment_stage").show(10, truncate=False)