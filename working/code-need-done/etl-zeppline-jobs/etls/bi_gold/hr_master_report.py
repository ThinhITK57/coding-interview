%livy.pyspark
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# =========================
# 1) Read Silver tables
# =========================
onboard  = spark.table("bi_silver.hr_employee_onboard")
resigned = spark.table("bi_silver.hr_employee_resigned")

onboard  = onboard.withColumn("employee_code", F.col("employee_code").cast("string"))
resigned = resigned.withColumn("employee_code", F.col("employee_code").cast("string"))

# =========================
# 2) Align schema
# =========================
all_cols = sorted(list(set(onboard.columns).union(set(resigned.columns))))

def align(df, all_columns):
    for c in all_columns:
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None))
    return df.select(*all_columns)

full_hr = align(onboard, all_cols).unionByName(align(resigned, all_cols))

# =========================
# 3) Pick 1 record per employee_code (deterministic as much as possible)
# =========================
term_key = F.coalesce(F.col("termination_date").cast("date"), F.lit("0001-01-01").cast("date"))
hire_key = F.coalesce(F.col("hire_date_vcs").cast("date"), F.lit("0001-01-01").cast("date"))

# tie-breaker if exists
if "updated_at" in full_hr.columns:
    upd_key = F.coalesce(F.col("updated_at").cast("timestamp"), F.lit("1900-01-01").cast("timestamp"))
    order_cols = [term_key.desc(), hire_key.desc(), upd_key.desc()]
elif "created_at" in full_hr.columns:
    cre_key = F.coalesce(F.col("created_at").cast("timestamp"), F.lit("1900-01-01").cast("timestamp"))
    order_cols = [term_key.desc(), hire_key.desc(), cre_key.desc()]
else:
    order_cols = [term_key.desc(), hire_key.desc()]

w = Window.partitionBy("employee_code").orderBy(*order_cols)

full_hr = (full_hr
           .withColumn("__rn", F.row_number().over(w))
           .where(F.col("__rn") == 1)
           .drop("__rn"))

# =========================
# 4) Cast score columns if exist
# =========================
score_cols = [
    "performance_score_q1_2024","performance_score_q2_2024","performance_score_q3_2024","performance_score_q4_2024",
    "avg_performance_score_2024",
    "performance_score_q1_2025","performance_score_q2_2025","performance_score_q3_2025",
    "num_cnxs_awards"
]
for c in score_cols:
    if c in full_hr.columns:
        full_hr = full_hr.withColumn(c, F.col(c).cast("double"))

# =========================
# 5) Helper
# =========================
def col_or_null(df, name):
    return F.col(name) if name in df.columns else F.lit(None)

# compute age if possible
if "date_of_birth" in full_hr.columns:
    age_col = F.floor(F.months_between(F.current_date(), F.col("date_of_birth"))/12)
else:
    age_col = col_or_null(full_hr, "age")

cols = [
    col_or_null(full_hr, "employee_code").alias("employee_code"),
    col_or_null(full_hr, "full_name").alias("full_name"),
    col_or_null(full_hr, "current_status").alias("current_status"),
    col_or_null(full_hr, "division").alias("division"),
    col_or_null(full_hr, "unit_level_1").alias("unit_level_1"),
    col_or_null(full_hr, "department_level_2").alias("department_level_2"),
    col_or_null(full_hr, "job_level").alias("job_level"),
    col_or_null(full_hr, "management_level").alias("management_level"),
    col_or_null(full_hr, "hire_date_vcs").cast("timestamp").alias("hire_date"),
    col_or_null(full_hr, "termination_date").cast("timestamp").alias("termination_date"),
    col_or_null(full_hr, "gender").alias("gender"),
    age_col.alias("age"),
    F.col("employment_status"),
    F.when(F.col("employment_status").isNull(), "ACTIVE").otherwise(F.col("employment_status")).alias("employment_status"),
    F.col("termination_reason"),
]

for c in score_cols:
    cols.append(col_or_null(full_hr, c).cast("double").alias(c))

cols += [
    col_or_null(full_hr, "talent_9box_label").alias("talent_9box_label"),
    col_or_null(full_hr, "performance_level").alias("performance_level"),
    col_or_null(full_hr, "potential_level").alias("potential_level"),
    col_or_null(full_hr, "resignation_reason_group_1").alias("resignation_reason_group_1"),
]

is_key = col_or_null(full_hr, "is_key_employee")
cols.append(F.when(is_key == 1, F.lit(1)).otherwise(F.lit(0)).alias("is_key_employee_flag"))

gold_df = full_hr.select(*cols)

out_path = "/opt/datasets/crawlers/vcs_gold/bi_gold/data/hr_master_report"
tgt_table = "bi_gold.hr_master_report"

gold_df.repartition(1).write \
    .mode("overwrite") \
    .format("parquet") \
    .option("path", out_path) \
    .saveAsTable(tgt_table)

spark.catalog.refreshTable(tgt_table)

print("DONE")