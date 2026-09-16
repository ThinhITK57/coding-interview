CREATE TABLE hive.noc_metrics_gold.dim_time AS
SELECT DISTINCT
    metric_ts,
    from_unixtime(metric_ts)            AS metric_datetime,
    hour(from_unixtime(metric_ts))      AS hour,
    minute(from_unixtime(metric_ts))    AS minute
FROM hive.noc_metrics_silver.metrics;
