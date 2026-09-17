%livy.pyspark
from datetime import datetime
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col, desc

input_path = "/opt/datasets/crawlers/vcs/freshdesk/data/fact_cso_tickets"

backup_path = "/opt/datasets/crawlers/vcs_raw_backup/freshdesk/data/fact_cso_tickets/" + \
              datetime.now().strftime("%Y%m%d_%H%M%S")

tmp_path = input_path + "_tmp"
old_path = input_path + "_old"

# Read
df = spark.read.parquet(input_path)

# Backup raw
(df.write
    .mode("overwrite")
    .option("compression", "snappy")
    .parquet(backup_path))

# Deduplicate
window = Window.partitionBy("id") \
               .orderBy(desc("updated_at_ts"))

df_dedup = (
    df.withColumn("rn", row_number().over(window))
      .filter(col("rn") == 1)
      .drop("rn")
)

# Write temp
(df_dedup
    .repartition(5, "id")
    .write
    .mode("overwrite")
    .option("compression", "snappy")
    .parquet(tmp_path))

# # ---------------- Rename ----------------

jvm = spark._jvm
conf = spark._jsc.hadoopConfiguration()
fs = jvm.org.apache.hadoop.fs.FileSystem.get(conf)

src = jvm.org.apache.hadoop.fs.Path(tmp_path)
dst = jvm.org.apache.hadoop.fs.Path(input_path)
old = jvm.org.apache.hadoop.fs.Path(old_path)

if fs.exists(old):
    fs.delete(old, True)

if fs.exists(dst):
    fs.rename(dst, old)

fs.rename(src, dst)

if fs.exists(old):
    fs.delete(old, True)

print("Backup:", backup_path)
print("Overwrite:", input_path)