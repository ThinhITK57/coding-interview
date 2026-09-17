# %livy.pyspark

tgt_table = "bi_silver.crm_am_activity_daily"
tgt_path  = "s3a://bi-silver/crm_am_activity_daily"

# Làm mới danh mục file nguồn
spark.catalog.clearCache()

sql_query = """
SELECT
    CAST(activity_date AS DATE) AS activity_date,

    -- AM
    am_user_id,
    am_user_name,
    am_group,
    am_status,

    -- Activity classification
    activity_category,
    activity_type,
    activity_action,
    entity_type,

    -- Statistics
    COUNT(*) AS activity_count,

    COUNT(DISTINCT entity_id) AS distinct_object_count,

    COUNT(DISTINCT company_id) AS distinct_company_count,

    COUNT(DISTINCT contact_id) AS distinct_contact_count,

    COUNT(DISTINCT deal_id) AS distinct_deal_count,

    COUNT(DISTINCT quotation_id) AS distinct_quotation_count,

    COUNT(DISTINCT contract_id) AS distinct_contract_count,

    -- Create / Update / Delete
    COUNT_IF(
        activity_action = 'CREATE'
        AND is_countable = TRUE
    ) AS create_activity_count,

    COUNT_IF(
        activity_action = 'UPDATE'
        AND is_countable = TRUE
    ) AS update_activity_count,

    COUNT_IF(
        activity_action = 'DELETE'
        AND is_countable = TRUE
    ) AS delete_activity_count,

    -- Business activity groups
    COUNT_IF(
        activity_category = 'CRM_COMPLIANCE'
        AND is_countable = TRUE
    ) AS crm_compliance_count,

    COUNT_IF(
        activity_category = 'SALES_ACTIVITY'
        AND is_countable = TRUE
    ) AS sales_activity_count,

    COUNT_IF(
        activity_category = 'SALES_PROGRESS'
        AND is_countable = TRUE
    ) AS sales_progress_count,

    COUNT_IF(
        activity_category = 'BUSINESS_OUTCOME'
        AND is_countable = TRUE
    ) AS business_outcome_count

FROM bi_silver.crm_activity_history

WHERE is_countable = TRUE

GROUP BY
    CAST(activity_date AS DATE),
    am_user_id,
    am_user_name,
    am_group,
    am_status,
    activity_category,
    activity_type,
    activity_action,
    entity_type;
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
