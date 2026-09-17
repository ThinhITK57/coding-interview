%livy.pyspark
spark.catalog.clearCache()

spark.sql("REFRESH TABLE crm_raw.deal_reasons")

tgt_table = "bi_silver.crm_deal_reasons"
tgt_path  = "/opt/datasets/crawlers/vcs_silver/bi_silver/data/crm_deal_reasons"

sql_query = """
WITH ranked AS (
  SELECT
    r.*,
    ROW_NUMBER() OVER (
      PARTITION BY CAST(r.id AS STRING)
      ORDER BY
        CAST(r.position AS BIGINT) DESC NULLS LAST,
        upper(regexp_replace(trim(r.name), '\\\\s+', ' ')) DESC
    ) AS rn
  FROM crm_raw.deal_reasons r
)
SELECT
  CAST(id AS STRING) AS id,
  upper(regexp_replace(trim(name), '\\\\s+', ' ')) AS reason_name,
  CAST(position AS BIGINT) AS position,
  COALESCE(CAST(partial AS BOOLEAN), false) AS is_partial
FROM ranked
WHERE rn = 1
"""

df = spark.sql(sql_query)

# # Drop table + hard delete path để CHỐNG DOUBLE
# spark.sql("DROP TABLE IF EXISTS " + tgt_table)

# hconf = spark._jsc.hadoopConfiguration()
# fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hconf)
# path = spark._jvm.org.apache.hadoop.fs.Path(tgt_path)
# if fs.exists(path):
#     fs.delete(path, True)

# Write
df.repartition(1).write \
  .mode("overwrite") \
  .format("parquet") \
  .save(tgt_path)
  # .option("path", tgt_path) \
  # .saveAsTable(tgt_table)

spark.catalog.refreshTable(tgt_table)

print(tgt_table)
print("Rows:", df.count())
spark.sql("SELECT * FROM bi_silver.crm_deal_reasons ORDER BY position DESC LIMIT 50").show(truncate=False)
