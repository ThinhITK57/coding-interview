# %livy.pyspark
tgt_path  = "s3a://bi-silver/crm_product_service_group_mapping"
tgt_table = "bi_silver.crm_product_service_group_mapping"

df = spark.sql("""
SELECT
    product_category,
    product_category_code,
    product_service_group_vcs,
    product_service_group,
    business_unit_n1,
    business_unit_n1_full_name
FROM VALUES
('VCS-TI','VCS017','Nhóm Mass','SPDV MỚI','TT PT&CSNC','Trung tâm Phân tích chia sẻ nguy cơ An ninh mạng'),
('VCS-Threat Intelligence','VCS017','Nhóm Mass','SPDV MỚI','TT PT&CSNC','Trung tâm Phân tích chia sẻ nguy cơ An ninh mạng'),
('Consolidated Platform','VCS042','Nhóm Sản phẩm mới','SPDV MỚI','TT SP Enterprise','Trung tâm Sản phẩm Enterprise'),
('Telco Authen Service','VCS032','Nhóm Sản phẩm mới','SP An ninh Viễn thông','TT SP Telco','Trung tâm Sản phẩm Telco'),
('TAS','VCS032','Nhóm Sản phẩm mới','SP An ninh Viễn thông','TT SP Telco','Trung tâm Sản phẩm Telco'),
('Sản phẩm AI','VCS043','Nhóm Sản phẩm mới','SPDV MỚI','TTGS','Trung tâm Giám sát & Phản ứng trên Không gian mạng'),
('MSS','VCS001','Nhóm MSS-Hệ sinh thái SOC','Hệ sinh thái SOC','TTGS','Trung tâm Giám sát & Phản ứng trên Không gian mạng'),
('VCS-CyM','VCS002','Nhóm MSS-Hệ sinh thái SOC','Hệ sinh thái SOC','TT SP Enterprise','Trung tâm Sản phẩm Enterprise'),
('VCS-CyCir','VCS003','Nhóm MSS-Hệ sinh thái SOC','Hệ sinh thái SOC','TT SP Enterprise','Trung tâm Sản phẩm Enterprise'),
('VCS-aJiant','VCS004','Nhóm MSS-Hệ sinh thái SOC','Hệ sinh thái SOC','TT SP Enterprise','Trung tâm Sản phẩm Enterprise'),
('VCS-NDR','VCS005','Nhóm MSS-Hệ sinh thái SOC','Hệ sinh thái SOC','TT SP Enterprise','Trung tâm Sản phẩm Enterprise'),
('VCS-NSM','VCS006','Nhóm MSS-Hệ sinh thái SOC','Hệ sinh thái SOC','TT SP Enterprise','Trung tâm Sản phẩm Enterprise'),
('VCS-KIAN','VCS007','Nhóm MSS-Hệ sinh thái SOC','Hệ sinh thái SOC','TT SX&CNTT','Trung tâm Sản xuất & Công nghệ thông tin'),
('VCS-AMA','VCS008','Nhóm MSS-Hệ sinh thái SOC','Hệ sinh thái SOC','TT SX&CNTT','Trung tâm Sản xuất & Công nghệ thông tin'),
('VCS-ESG','VCS009','Nhóm MSS-Hệ sinh thái SOC','Hệ sinh thái SOC','TT SX&CNTT','Trung tâm Sản xuất & Công nghệ thông tin'),
('VCS-WSG','VCS010','Nhóm MSS-Hệ sinh thái SOC','Hệ sinh thái SOC','TT SX&CNTT','Trung tâm Sản xuất & Công nghệ thông tin'),
('VCS-NAC','VCS011','Nhóm MSS-Hệ sinh thái SOC','Hệ sinh thái SOC','TT SX&CNTT','Trung tâm Sản xuất & Công nghệ thông tin'),
('SOC platform','VCS012','Nhóm MSS-Hệ sinh thái SOC','Hệ sinh thái SOC','TT SP Enterprise','Trung tâm Sản phẩm Enterprise'),
('VCS M-Suite','VCS015','Nhóm SP Standalone','SPDV MỚI','TT SX&CNTT','Trung tâm Sản xuất & Công nghệ thông tin'),
('M-Suite','VCS015','Nhóm SP Standalone','SPDV MỚI','TT SX&CNTT','Trung tâm Sản xuất & Công nghệ thông tin'),
('VCS-F2DR','VCS016','Nhóm Telco','SPDV MỚI','TT SP Telco','Trung tâm Sản phẩm Telco'),
('VCS-InT','VCS018','Nhóm SP Standalone','SPDV MỚI','TT SP Enterprise','Trung tâm Sản phẩm Enterprise'),
('Lá chắn số (TNXH)','VCS019','Nhóm Mass','SPDV MỚI','TT SP Telco','Trung tâm Sản phẩm Telco'),
('LI','VCS020','Nhóm Telco','SP An ninh Viễn thông','TT SP Telco','Trung tâm Sản phẩm Telco'),
('LIG','VCS020','Nhóm Telco','SP An ninh Viễn thông','TT SP Telco','Trung tâm Sản phẩm Telco'),
('TML','VCS021','Nhóm Telco','SP An ninh Viễn thông','TT SP Telco','Trung tâm Sản phẩm Telco'),
('BRG','VCS022','Nhóm Telco','SP An ninh Viễn thông','TT SP Telco','Trung tâm Sản phẩm Telco'),
('TAD','VCS023','Nhóm Telco','SP An ninh Viễn thông','TT SP Telco','Trung tâm Sản phẩm Telco'),
('VCS-TAD','VCS023','Nhóm Telco','SP An ninh Viễn thông','TT SP Telco','Trung tâm Sản phẩm Telco'),
('VAPT','VCS024','Nhóm Professional Services','Dịch vụ ATTT chuyên sâu','TT DV ATTT','Trung tâm Dịch vụ An Toàn Thông Tin'),
('VAPT (Pentest)','VCS024','Nhóm Professional Services','Dịch vụ ATTT chuyên sâu','TT DV ATTT','Trung tâm Dịch vụ An Toàn Thông Tin'),
('Red Team','VCS025','Nhóm Professional Services','Dịch vụ ATTT chuyên sâu','TT DV ATTT','Trung tâm Dịch vụ An Toàn Thông Tin'),
('Redteam','VCS025','Nhóm Professional Services','Dịch vụ ATTT chuyên sâu','TT DV ATTT','Trung tâm Dịch vụ An Toàn Thông Tin'),
('Infrastructure Audit','VCS026','Nhóm Professional Services','Dịch vụ ATTT chuyên sâu','TT DV ATTT','Trung tâm Dịch vụ An Toàn Thông Tin'),
('Tư vấn','VCS027','Nhóm Professional Services','Dịch vụ ATTT chuyên sâu','SI','Phòng Hợp tác SI'),
('Threat Hunting','VCS028','Nhóm Professional Services','Dịch vụ ATTT chuyên sâu','TTGS','Trung tâm Giám sát & Phản ứng trên Không gian mạng'),
('Diễn tập','VCS029','Nhóm Professional Services','Dịch vụ ATTT chuyên sâu','TT DV ATTT','Trung tâm Dịch vụ An Toàn Thông Tin'),
('Diễn tập','VCS029','Nhóm Professional Services','Dịch vụ ATTT chuyên sâu','TT DV ATTT','Trung tâm Dịch vụ An Toàn Thông Tin'),
('Purple Team Service','VCS029','Nhóm Professional Services','Dịch vụ ATTT chuyên sâu','TT DV ATTT','Trung tâm Dịch vụ An Toàn Thông Tin'),
('CA','VCS030','Nhóm MSS-CA','Dịch vụ ATTT chuyên sâu','TTGS','Trung tâm Giám sát & Phản ứng trên Không gian mạng'),
('WAAP Solution','VCS031','Nhóm Mass','SPDV MỚI','TT SP Telco','Trung tâm Sản phẩm Telco'),
('MDR Service','VCS033','Nhóm MSS-MDR Service','SPDV MỚI','TT SP Enterprise','Trung tâm Sản phẩm Enterprise'),
('Dịch vụ chuyên nghiệp khác','VCS034','Nhóm Professional Services','Dịch vụ ATTT chuyên sâu','TT DV ATTT','Trung tâm Dịch vụ An Toàn Thông Tin'),
('Dịch vụ khác','VCS034','Nhóm Professional Services','Dịch vụ ATTT chuyên sâu','TT DV ATTT','Trung tâm Dịch vụ An Toàn Thông Tin'),
('Kinh doanh SI','VCS040','Nhóm kinh doanh SI','KHÁC (PC, PM phát sinh)','SI','Phòng Hợp tác SI'),
('Content Security','VCS041','Nhóm MSS-Hệ sinh thái SOC','Hệ sinh thái SOC','TTGS','Trung tâm Giám sát & Phản ứng trên Không gian mạng'),
('Cloudrity','VCS013','Nhóm SP Standalone-SPDV MỚI','SPDV MỚI','TT SP Telco','Trung tâm Sản phẩm Telco'),
('AntiDDos','VCS014','Nhóm SP Standalone-SPDV MỚI','SPDV MỚI','TT SP Telco','Trung tâm Sản phẩm Telco'),
('Khác','VCS038','Nhóm kinh doanh SI','KHÁC (PC, PM phát sinh)','SI','Phòng Hợp tác SI'),
('','VCS038','Nhóm kinh doanh SI','KHÁC (PC, PM phát sinh)','SI','Phòng Hợp tác SI'),
('SI','VCS038','Nhóm kinh doanh SI','KHÁC (PC, PM phát sinh)','SI','Phòng Hợp tác SI')
AS t(
    product_category,
    product_category_code,
    product_service_group_vcs,
    product_service_group,
    business_unit_n1,
    business_unit_n1_full_name
)
""")

# 2) Write
df.repartition(1).write \
  .mode("overwrite") \
  .format("parquet") \
  .option("path", tgt_path) \
  .saveAsTable(tgt_table)
