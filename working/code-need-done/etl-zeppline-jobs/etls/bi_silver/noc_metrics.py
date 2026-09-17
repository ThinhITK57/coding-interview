%livy.pyspark

# spark.sql("drop  table IF EXISTS bi_silver.noc_metrics")
 
sql_query = """

WITH unioned AS (

    SELECT
        'cpu_usage_average' AS metric_name,
        'max'               AS agg_type,
        customer,
        project,
        host,
        max                 AS metric_value,
        time_ts             AS metric_ts,
        from_unixtime(time_ts/1000) AS metric_time,
        TO_TIMESTAMP(date(from_unixtime(time_ts/1000))) AS dt
    FROM noc_metrics_raw.cpu_usage_average__max
    WHERE max IS NOT NULL

    UNION ALL
    SELECT
        'cpu_usage_average',
        'mean',
        customer,
        project,
        host,
        mean,
        time_ts,
        from_unixtime(time_ts/1000),
        date(from_unixtime(time_ts/1000))
    FROM noc_metrics_raw.cpu_usage_average__mean
    WHERE mean IS NOT NULL

    UNION ALL
    SELECT
        'cpu_usage_average',
        'min',
        customer,
        project,
        host,
        min,
        time_ts,
        from_unixtime(time_ts/1000),
        date(from_unixtime(time_ts/1000))
    FROM noc_metrics_raw.cpu_usage_average__min
    WHERE min IS NOT NULL


    UNION ALL
    SELECT
        'disk_project_io_util',
        'max',
        customer,
        project,
        host,
        max,
        time_ts,
        from_unixtime(time_ts/1000),
        date(from_unixtime(time_ts/1000))
    FROM noc_metrics_raw.disk_project__io_util__max
    WHERE max IS NOT NULL

    UNION ALL
    SELECT
        'disk_project_io_util',
        'mean',
        customer,
        project,
        host,
        mean,
        time_ts,
        from_unixtime(time_ts/1000),
        date(from_unixtime(time_ts/1000))
    FROM noc_metrics_raw.disk_project__io_util__mean
    WHERE mean IS NOT NULL

    UNION ALL
    SELECT
        'disk_project_io_util',
        'min',
        customer,
        project,
        host,
        min,
        time_ts,
        from_unixtime(time_ts/1000),
        date(from_unixtime(time_ts/1000))
    FROM noc_metrics_raw.disk_project__io_util__min
    WHERE min IS NOT NULL


    UNION ALL
    SELECT
        'disk_used_percent',
        'max',
        customer,
        project,
        host,
        max,
        time_ts,
        from_unixtime(time_ts/1000),
        date(from_unixtime(time_ts/1000))
    FROM noc_metrics_raw.disk_used_percent__max
    WHERE max IS NOT NULL

    UNION ALL
    SELECT
        'disk_used_percent',
        'mean',
        customer,
        project,
        host,
        mean,
        time_ts,
        from_unixtime(time_ts/1000),
        date(from_unixtime(time_ts/1000))
    FROM noc_metrics_raw.disk_used_percent__mean
    WHERE mean IS NOT NULL

    UNION ALL
    SELECT
        'disk_used_percent',
        'min',
        customer,
        project,
        host,
        min,
        time_ts,
        from_unixtime(time_ts/1000),
        date(from_unixtime(time_ts/1000))
    FROM noc_metrics_raw.disk_used_percent__min
    WHERE min IS NOT NULL


    UNION ALL
    SELECT
        'mem_usage_average',
        'max',
        customer,
        project,
        host,
        max,
        time_ts,
        from_unixtime(time_ts/1000),
        date(from_unixtime(time_ts/1000))
    FROM noc_metrics_raw.mem_usage_average__max
    WHERE max IS NOT NULL

    UNION ALL
    SELECT
        'mem_usage_average',
        'mean',
        customer,
        project,
        host,
        mean,
        time_ts,
        from_unixtime(time_ts/1000),
        date(from_unixtime(time_ts/1000))
    FROM noc_metrics_raw.mem_usage_average__mean
    WHERE mean IS NOT NULL

    UNION ALL
    SELECT
        'mem_usage_average',
        'min',
        customer,
        project,
        host,
        min,
        time_ts,
        from_unixtime(time_ts/1000),
        date(from_unixtime(time_ts/1000))
    FROM noc_metrics_raw.mem_usage_average__min
    WHERE min IS NOT NULL


    UNION ALL
    SELECT
        'net_customer_util',
        'max',
        customer,
        project,
        host,
        max,
        time_ts,
        from_unixtime(time_ts/1000),
        date(from_unixtime(time_ts/1000))
    FROM noc_metrics_raw.net_customer__util__max
    WHERE max IS NOT NULL

    UNION ALL
    SELECT
        'net_customer_util',
        'mean',
        customer,
        project,
        host,
        mean,
        time_ts,
        from_unixtime(time_ts/1000),
        date(from_unixtime(time_ts/1000))
    FROM noc_metrics_raw.net_customer__util__mean
    WHERE mean IS NOT NULL

    UNION ALL
    SELECT
        'net_customer_util',
        'min',
        customer,
        project,
        host,
        min,
        time_ts,
        from_unixtime(time_ts/1000),
        date(from_unixtime(time_ts/1000))
    FROM noc_metrics_raw.net_customer__util__min
    WHERE min IS NOT NULL


    UNION ALL
    SELECT
        'free_space_capacity',
        'last',
        customer,
        project,
        host,
        last_last,
        time_ts,
        from_unixtime(time_ts/1000),
        date(from_unixtime(time_ts/1000))
    FROM noc_metrics_raw.free_space__capacity
    WHERE last_last IS NOT NULL
)

SELECT DISTINCT
    metric_name,
    agg_type,
    customer,
    project,
    host,
    metric_value,
    metric_ts,
    metric_time,
    dt
FROM unioned



"""

df = spark.sql(sql_query)

df.repartition(10).write \
    .mode("overwrite") \
    .format("parquet") \
    .option(
        "path",
        "/opt/datasets/crawlers/vcs_silver/bi_silver/data/noc_metrics"
    ) \
    .saveAsTable("bi_silver.noc_metrics")
