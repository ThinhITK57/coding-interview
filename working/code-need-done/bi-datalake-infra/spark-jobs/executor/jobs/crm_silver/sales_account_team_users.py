# %livy.pyspark
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# =========================
# 1. Load source tables
# =========================
sales_accounts = spark.table("crm_raw.sales_accounts")
deleted_sales_accounts = spark.table("crm_raw.deleted_sales_accounts")

# =========================
# 2. NOT EXISTS deleted records
# =========================
sales_accounts_filtered = (
    sales_accounts.alias("ss")
    .join(
        deleted_sales_accounts.alias("d"),
        F.col("ss.id") == F.col("d.id").cast("bigint"),
        "left_anti"
    )
)

# =========================
# 3. Keep latest record per sale_id
# =========================
window_spec = Window.partitionBy("id").orderBy(
    F.col("updated_at_ts").desc(),
    F.col("id").desc()
)

last_sales_accounts = (
    sales_accounts_filtered
    .withColumn("rn", F.row_number().over(window_spec))
    .filter(F.col("rn") == 1)
)

# =========================
# 4. CROSS JOIN UNNEST(team_user_ids)
# =========================
result_df = (
    last_sales_accounts
    .filter(F.col("is_deleted") == False)
    .withColumn(
        "team_user_ids_safe",
        F.when(F.col("team_user_ids").isNull(), F.array())
         .otherwise(F.col("team_user_ids"))
    )
    .withColumn("user_id", F.explode("team_user_ids_safe"))
    .select(
        F.col("id").cast("string").alias("sale_id"),
        F.col("user_id")
    )
)

# =========================
# 5. Save to Hive table + path
# =========================
(
    result_df
    .repartition(5)
    .write
    .mode("overwrite")
    .format("parquet")
    .option(
        "path",
        "s3a://vcs-silver/crm-silver/sales_account_team_users"
    )
    .saveAsTable("crm_silver.sales_account_team_users")
)

# result_df.show(truncate=False)

