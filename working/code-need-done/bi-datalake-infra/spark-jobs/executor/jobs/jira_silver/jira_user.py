# %livy.pyspark

spark.sql("drop  table IF EXISTS jira_silver.jira_user")
 
sql_query = """

SELECT
    -- ========= IDENTIFIER =========
    user_id       AS user_id,
    user_key      AS user_key,

    -- ========= USER INFO =========
    username      AS username,
    display_name  AS display_name,
    email         AS email,

    -- ========= STATUS =========
    active        AS is_active,
    directory_id  AS directory_id

    -- ========= META =========
--    source_file   AS source_file
FROM jira_raw.jira_user

"""

df = spark.sql(sql_query)

df.write \
    .mode("overwrite") \
    .format("parquet") \
    .option(
        "path",
        "s3a://vcs-silver/jira-silver/jira_user"
    ) \
    .saveAsTable("jira_silver.jira_user")
