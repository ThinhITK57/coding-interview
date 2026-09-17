# %livy.pyspark

# spark.sql("drop  table IF EXISTS cx_cso_silver.ticket_tags")
 
sql_query = """

WITH last_fact_cso_tickets AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY id
                   ORDER BY updated_at_ts DESC, id DESC
               ) rn
        FROM cx_cso_raw.fact_cso_tickets
        WHERE deleted IS NULL
    ) t
    WHERE rn = 1
)

SELECT
    t.id AS ticket_id,

    trim(regexp_replace(tag, '✨', '')) AS tag,

    from_unixtime(t.created_at_ts) AS created_at,
    from_unixtime(t.updated_at_ts) AS updated_at

FROM last_fact_cso_tickets t
LATERAL VIEW explode(t.tags) u AS tag


"""

df = spark.sql(sql_query)

df.write \
    .mode("overwrite") \
    .format("parquet") \
    .option(
        "path",
        "s3a://vcs-silver/cx-cso-silver/ticket_tags"
    ) \
    .saveAsTable("cx_cso_silver.ticket_tags")
