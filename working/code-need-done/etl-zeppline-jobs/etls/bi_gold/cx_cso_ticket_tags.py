%livy.pyspark

sql_query = """
SELECT
  md5(concat_ws('|',
    coalesce(CAST(ticket_id AS STRING),'__NULL__'),
    coalesce(CAST(tag AS STRING),'__NULL__')
  )) AS id,
  ticket_id,
  tag
FROM bi_silver.cx_cso_ticket_tags
"""

df = spark.sql(sql_query)

target_table = "bi_gold.cx_cso_ticket_tags"
target_path  = "/opt/datasets/crawlers/vcs_gold/bi_gold/data/cx_cso_ticket_tags"

# spark.sql("DROP TABLE IF EXISTS {}".format(target_table))

# hard delete path
# hconf = spark._jsc.hadoopConfiguration()
# fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hconf)
# path = spark._jvm.org.apache.hadoop.fs.Path(target_path)
# if fs.exists(path):
#     fs.delete(path, True)

(df.repartition(1).write
  .mode("overwrite")
  .format("parquet")
  .option("path", target_path)
  .saveAsTable(target_table)
)

print("DONE")
