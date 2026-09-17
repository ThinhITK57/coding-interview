# %livy.pyspark

from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

spark.sql("REFRESH TABLE crm_raw.deleted_sales_accounts ")
spark.sql("REFRESH TABLE crm_raw.sales_accounts ")
spark.sql("REFRESH TABLE crm_raw.industry_types ")
spark.sql("REFRESH TABLE crm_raw.business_types ")

# ------------------------------------------------------------------
# 1. LẤY SALES ACCOUNT MỚI NHẤT (ANTI-JOIN DELETED)
# ------------------------------------------------------------------
df_partners = spark.sql("""
SELECT
    parent_sales_account_id as parent_id,
    true as is_partner
FROM crm_raw.sales_accounts ss
WHERE parent_sales_account_id is not null and NOT EXISTS (
    SELECT 1
    FROM crm_raw.deleted_sales_accounts d
    WHERE CAST(d.id AS BIGINT) = ss.id
)
""")

df_partners.createOrReplaceTempView("sales_partners_view")

df_sales_accounts = spark.sql("""
SELECT
    ss.*,
    coalesce(pn.is_partner, false) as is_partner,
    ROW_NUMBER() OVER (
        PARTITION BY id
        ORDER BY updated_at_ts DESC, id DESC
    ) AS rn
FROM crm_raw.sales_accounts ss
LEFT JOIN sales_partners_view pn ON pn.parent_id = ss.id
WHERE NOT EXISTS (
    SELECT 1
    FROM crm_raw.deleted_sales_accounts d
    WHERE CAST(d.id AS BIGINT) = ss.id
)

""").filter(col("rn") == 1)

# ------------------------------------------------------------------
# 2. LOAD DIM TABLES
# ------------------------------------------------------------------

df_business_types = spark.table("crm_raw.business_types") \
    .select(
        col("id").alias("business_type_id"),
        col("name").alias("business_type_name")
    )

df_industry_types = spark.table("crm_raw.industry_types") \
    .select(
        col("id").alias("industry_type_id"),
        col("name").alias("industry_type_name")
    )

# ------------------------------------------------------------------
# 3. JOIN + SELECT FINAL COLUMNS
# ------------------------------------------------------------------

df_sales_account_final = (
    df_sales_accounts
        .join(df_business_types, on="business_type_id", how="left")
        .join(df_industry_types, on="industry_type_id", how="left")
        .filter(col("is_deleted") == False)
        .select(
            col("id").cast("string").alias("sale_id"),
            col("is_partner"),
            col("name"),
            col("address"),
            col("city"),
            col("state"),
            col("zipcode"),
            col("country"),
            col("number_of_employees"),
            col("annual_revenue"),
            col("website"),
            col("owner_id").alias("user_am_id"),
            col("phone"),
            col("open_deals_amount"),
            col("open_deals_count"),
            col("won_deals_amount"),
            col("won_deals_count"),
            col("last_contacted"),
            col("last_contacted_mode"),
            col("facebook"),
            col("twitter"),

            # -------- custom_field --------
            col("custom_field.cf_alias").alias("company_alias"),
            col("custom_field.cf_tax_code").alias("tax_code"),
            col("custom_field.cf_using_soc").alias("is_using_soc"),
            col("custom_field.cf_vcs_socothers").alias("current_soc_provider"),
            col("custom_field.cf_soc_brand").alias("current_soc_brand"),
            col("custom_field.cf_date_using_soc").alias("first_date_using_soc"),
            col("custom_field.cf_country").alias("country_custom"),
            col("custom_field.cf_province").alias("province"),
            col("custom_field.cf_group").alias("customer_group"),
            col("custom_field.cf_segment").alias("segment_l1"),
            col("custom_field.cf_segment2").alias("segment_l2"),
            col("custom_field.cf_segment3").alias("segment_l3"),
            col("custom_field.cf_initial_source").alias("lead_initial_source"),
            col("custom_field.cf_warm_up_source").alias("lead_warm_up_source"),
            col("custom_field.cf__am").alias("user_am_fullname"),
            col("custom_field.cf__domain").alias("company_domain"),

            col("parent_sales_account_id").alias("parent_id"),
            col("last_assigned_at"),
            col("renewal_date"),

            col("business_type_id"),
            col("business_type_name"),
            col("industry_type_id"),
            col("industry_type_name")
        )
)

# ------------------------------------------------------------------
# 4. WRITE SILVER TABLE
# ------------------------------------------------------------------

df_sales_account_final \
    .repartition(1) \
    .write \
    .mode("overwrite") \
    .format("parquet") \
    .option(
        "path",
        "s3a://vcs-silver/crm-silver/sales_accounts"
    ) \
    .saveAsTable("crm_silver.sales_accounts")


print("DONE: crm_silver.sales_accounts")
spark.catalog.dropTempView("sales_partners_view")
