%livy.pyspark

tgt_table = "bi_silver.crm_am_activity_agg_daily"
tgt_path  = "/opt/datasets/crawlers/vcs_silver/bi_silver/data/crm_am_activity_agg_daily"

# Làm mới danh mục file nguồn
spark.catalog.clearCache()

sql_query = """
SELECT
    CAST(report_date AS DATE) AS report_date,
    am_user_id,
    am_name,

    COUNT(
        DISTINCT CASE
            WHEN activity_type = 'CUSTOMER_CREATED'
            THEN customer_id
        END
    ) AS new_customer_count,

    COUNT(
        DISTINCT CASE
            WHEN activity_type = 'CUSTOMER_UPDATED'
            THEN customer_id
        END
    ) AS updated_customer_count,

    COUNT(
        DISTINCT CASE
            WHEN activity_type = 'OPPORTUNITY_CREATED'
            THEN opp_id
        END
    ) AS new_opportunity_count,

    COUNT(
        DISTINCT CASE
            WHEN activity_type = 'OPPORTUNITY_UPDATED'
            THEN opp_id
        END
    ) AS updated_opportunity_count,

    COUNT(
        DISTINCT CASE
            WHEN activity_type = 'CONTRACT_CREATED'
            THEN contract_id
        END
    ) AS new_contract_count,

    COUNT(
        DISTINCT CASE
            WHEN activity_type = 'CONTRACT_UPDATED'
            THEN contract_id
        END
    ) AS updated_contract_count,

    am_status,
    am_group

FROM bi_silver.crm_am_activity_daily

WHERE am_group <> 'OTHER'

GROUP BY
    CAST(report_date AS DATE),
    am_user_id,
    am_name,
    am_status,
    am_group
"""

df = spark.sql(sql_query)

# 4) Save
df.repartition(1).write  \
  .mode("overwrite") \
  .format("parquet") \
  .option("path", tgt_path) \
  .saveAsTable(tgt_table)
  
# .option("path", tgt_path) \
#   .saveAsTable(tgt_table)

spark.catalog.refreshTable(tgt_table)

print(tgt_table)
# 5) Quick check
spark.sql("SELECT * FROM " + tgt_table + " LIMIT 1").show(truncate=False)
