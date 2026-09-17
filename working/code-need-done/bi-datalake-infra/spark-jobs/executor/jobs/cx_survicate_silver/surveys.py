# %livy.pyspark

spark.sql("drop  table IF EXISTS cx_survicate_silver.surveys")
 
sql_query = """

WITH last_items_cte AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY id
                   ORDER BY created_at_ts DESC, id DESC
               ) rn
        FROM cx_survicate_raw.dim_survicate_surveys ss
    ) t
    WHERE rn = 1
)

SELECT
    id,
    type,
    name,
--    created_at,
    enabled,
    responses as num_responses,
--    launch.start_at        AS launch_start_at,
--    launch.end_at          AS launch_end_at,
--    launch.responses_limit AS launch_responses_limit,
    from_unixtime(created_at_ts / 1000) AS created_at
FROM last_items_cte




"""

df = spark.sql(sql_query)

df.write \
    .mode("overwrite") \
    .format("parquet") \
    .option(
        "path",
        "s3a://vcs-silver/cx-survicate-silver/surveys"
    ) \
    .saveAsTable("cx_survicate_silver.surveys")
