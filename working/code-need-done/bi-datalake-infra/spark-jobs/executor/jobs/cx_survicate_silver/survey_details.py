# %livy.pyspark

# spark.sql("DROP TABLE IF EXISTS cx_survicate_silver.survey_details")

sql_query = """
WITH last_items_cte AS (
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY id
                ORDER BY created_at_ts DESC
            ) AS rn
        FROM cx_survicate_raw.dim_survicate_survey_details
    ) t
    WHERE rn = 1
)

SELECT
    s.id                      AS survey_detail_id,
    s.survey_id,
    s.type,
    s.name,
    s.enabled,
    s.responses,

    -- launch struct
    s.launch.start_at         AS launch_start_at,
    s.launch.end_at           AS launch_end_at,
    s.launch.responses_limit AS launch_responses_limit,

    -- author struct
    s.author.name             AS author_name,
    s.author.email            AS author_email,

    s.folder,
    s.first_response_at,
    s.last_response_at,

    attr                      AS attribute,

    s.created_at_ts,
    s.first_response_at_ts,
    s.last_response_at_ts
FROM last_items_cte s
LATERAL VIEW OUTER explode(s.attributes) attr_tbl AS attr
"""

df = spark.sql(sql_query)

df.write \
    .mode("overwrite") \
    .format("parquet") \
    .option(
        "path",
        "s3a://vcs-silver/cx-survicate-silver/survey_details"
    ) \
    .saveAsTable("cx_survicate_silver.survey_details")
