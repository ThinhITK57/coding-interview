%livy.pyspark

sql_query = """
WITH base AS (
    SELECT
        ticket_id,
        subject,
        customer_type,
        status_name,
        priority_name,

        CAST(created_at AS TIMESTAMP)         AS created_at,
        CAST(updated_at AS TIMESTAMP)         AS updated_at,
        CAST(first_responded_at AS TIMESTAMP) AS first_responded_at,
        CAST(resolved_at AS TIMESTAMP)        AS resolved_at,
        CAST(closed_at AS TIMESTAMP)          AS closed_at,

        resolution_time_hours,
        first_response_time_hours,
        l1_time_actual_minutes,
        l1_time_allowed_minutes,
        l1_violated,
        l2_time_actual_minutes,
        l2_time_allowed_minutes,
        l2_violated,
        l3_time_actual_minutes,
        l3_time_allowed_minutes,
        l3_violated,
        l4_time_actual_minutes,
        l4_time_allowed_minutes,
        l4_violated,
        ttr_minutes,
        requester_name,
        company_name,
        company_name as customer_name
    FROM bi_silver.cx_cso_tickets
),
norm AS (
    SELECT
        *,
        -- normalize alias để join ổn định
        TRIM(REGEXP_REPLACE(
            REGEXP_REPLACE(
                LOWER(company_name),
                '[^0-9a-zA-Z\\u00C0-\\u1EF9]+', ' '
            ),
            '\\\\s+', ' '
        )) AS company_name_norm
    FROM base
)
SELECT * FROM norm
"""

df = spark.sql(sql_query)

# =========================
# Target config
# =========================
target_table = "bi_gold.cx_cso_tickets"
target_path  = "/opt/datasets/crawlers/vcs_gold/bi_gold/data/cx_cso_tickets"
# Drop table (FIX: f-string)
# Drop table
# spark.sql("DROP TABLE IF EXISTS {}".format(target_table))

# Hard delete path
# hconf = spark._jsc.hadoopConfiguration()
# fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hconf)
# path = spark._jvm.org.apache.hadoop.fs.Path(target_path)

# if fs.exists(path):
#     fs.delete(path, True)

# Write
(df.repartition(1).write
  .mode("overwrite")
  .format("parquet")
  .option("path", target_path)
  .saveAsTable(target_table)
)

print("DONE")
