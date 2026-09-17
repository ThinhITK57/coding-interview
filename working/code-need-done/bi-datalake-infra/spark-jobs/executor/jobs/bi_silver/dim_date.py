# %livy.pyspark

from pyspark.sql import functions as F
from datetime import date

spark.catalog.clearCache()
location = "s3a://bi-silver/dim_date"

start = date(2020, 1, 1)
end = date(2040, 12, 31)
days = (end - start).days + 1

start_date = "2020-01-01"

week_start_expr = """
date_sub(
    d,
    case
        when dayofweek(d) = 1 then 6
        else dayofweek(d) - 2
    end
)
"""

df = (
    spark.range(0, days)
    .select(
        F.expr("date_add('{}', cast(id as int))".format(start_date)).alias("d")
    )
    .select(
        F.col("d").alias("date_value"),
        F.col("d").cast("timestamp").alias("date_ts"),

        F.date_format("d", "yyyyMMdd").cast("int").alias("date_key"),

        F.year("d").alias("date_year"),
        F.quarter("d").alias("date_quarter"),
        F.month("d").alias("date_month"),
        F.weekofyear("d").alias("date_week"),
        F.dayofmonth("d").alias("date_day"),

        F.expr("""
            case
                when dayofweek(d) = 1 then 7
                else dayofweek(d) - 1
            end
        """).alias("date_day_of_week"),

        F.date_format("d", "EEEE").alias("date_day_name"),
        F.date_format("d", "MMMM").alias("date_month_name"),

        F.expr(week_start_expr).alias("date_week_start"),
        F.expr("date_add({}, 6)".format(week_start_expr)).alias("date_week_end"),

        F.trunc("d", "MM").alias("date_month_start"),
        F.last_day("d").alias("date_month_end"),

        F.date_format("d", "yyyyMM").cast("int").alias("date_year_month"),

        F.date_format(F.expr(week_start_expr), "yyyyMMdd")
            .cast("int")
            .alias("date_year_week"),

        F.expr("""
            case
                when dayofweek(d) in (1, 7) then true
                else false
            end
        """).alias("date_is_weekend"),

        F.expr("""
            case
                when d = last_day(d) then true
                else false
            end
        """).alias("date_is_month_end"),

        F.expr("""
            case
                when month(d) in (3, 6, 9, 12)
                 and d = last_day(d)
                then true
                else false
            end
        """).alias("date_is_quarter_end")
    )
)

df = df.orderBy(F.col("date_value").asc())
(
    df.repartition(1).write
    .mode("overwrite")
    .format("parquet")
    .option("path", location)
    .saveAsTable("bi_silver.dim_date")
)