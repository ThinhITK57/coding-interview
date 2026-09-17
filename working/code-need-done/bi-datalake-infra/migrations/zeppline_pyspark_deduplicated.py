# %livy.pyspark

# ==============================
# INPUT
# ==============================
TABLE = "crm_raw.sales_accounts"


# ==============================
# HELPERS
# ==============================
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col, desc

def get_columns(df):
    return [f.name.lower() for f in df.schema.fields]


def deduplicate_by_key(df, keys, order_col):
    window = Window.partitionBy(*keys).orderBy(desc(order_col))

    return df.withColumn("rn", row_number().over(window)) \
             .filter(col("rn") == 1) \
             .drop("rn")


def auto_deduplicate(df):
    cols = get_columns(df)

    id_candidates = ["id", "uuid", "pk"]
    ts_candidates = ["updated_at_ts", "updated_at", "last_updated", "crawled_at_ts"]

    found_id = next((c for c in id_candidates if c in cols), None)
    found_ts = next((c for c in ts_candidates if c in cols), None)

    if found_id and found_ts:
        print("🔑 Dedup by ({}, {})".format(found_id, found_ts))
        return deduplicate_by_key(df, [found_id], found_ts)

    print("🧹 Dedup full row")
    return df.dropDuplicates()


# ==============================
# GET TABLE LOCATION (Hadoop)
# ==============================
def get_table_location(table_name):
    desc = spark.sql("DESCRIBE FORMATTED {}".format(table_name)).collect()

    for row in desc:
        if row.col_name.strip().lower() == "location":
            return row.data_type

    raise Exception("❌ Không tìm thấy location của table {}".format(table_name))


# ==============================
# MAIN
# ==============================
print("\n🚀 Processing {}".format(TABLE))

spark.catalog.refreshTable(TABLE)

# 1. đọc data
df = spark.sql("SELECT * FROM {}".format(TABLE))

# 2. dedup
df_clean = auto_deduplicate(df)

# 3. lấy location
tgt_path = get_table_location(TABLE)

print("📦 Location: {}".format(tgt_path))

# 4. tránh small files
df_final = df_clean.repartition(1)


tmp_table = TABLE + "_tmp"

spark.sql("DROP TABLE IF EXISTS {}".format(tmp_table))

# 5. overwrite bằng parquet
df_final.write \
    .mode("overwrite") \
    .format("parquet") \
    .option("path", tgt_path) \
    .saveAsTable(tmp_table)
    
# swap
spark.sql("DROP TABLE {}".format(TABLE))
spark.sql("ALTER TABLE {} RENAME TO {}".format(tmp_table,TABLE ))

spark.catalog.refreshTable(TABLE)

print("✅ Done {}".format(TABLE))