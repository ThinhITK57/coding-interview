# %livy.pyspark
spark.catalog.clearCache()

tgt_table = "bi_silver.cx_cso_ticket_tags"
tgt_path  = "s3a://bi-silver/cx_cso_ticket_tags"

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
),
exploded_tags AS (
    SELECT
        CAST(t.id AS STRING) AS ticket_id,
        TO_TIMESTAMP(CAST(t.created_at as DATE)) AS created_at,
        TO_TIMESTAMP(CAST(t.updated_at as DATE)) AS updated_at,
        tag_raw
    FROM last_fact_cso_tickets t
    LATERAL VIEW explode(t.tags) exploded AS tag_raw
)
SELECT DISTINCT
    ticket_id,
    trim(regexp_replace(regexp_replace(tag_raw, '✨', ''), '\\\\s+', ' ')) AS tag,
    CAST( CAST(created_at as DATE) as TIMESTAMP) as created_at,
    CAST( CAST(updated_at as DATE) as TIMESTAMP) as updated_at
FROM exploded_tags
WHERE tag_raw IS NOT NULL AND trim(tag_raw) <> ''
"""


df = spark.sql(sql_query)


df.repartition(1).write \
  .mode("overwrite") \
  .format("parquet") \
  .save(tgt_path)
#   .option("path", tgt_path) \
#   .saveAsTable(tgt_table)

spark.catalog.refreshTable(tgt_table)

print(tgt_table)
spark.sql("SELECT count(*) cnt FROM " + tgt_table).show()

