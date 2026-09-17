%livy.pyspark
spark.sql("""
CREATE OR REPLACE VIEW bi_gold.cx_cso_ticket_tags AS
select
	created_at,
    UPPER(tag) as tag,
    ticket_id
FROM bi_silver.cx_cso_ticket_tags
""")

# -- Truy vấn kiểm tra kết quả
print("DONE")