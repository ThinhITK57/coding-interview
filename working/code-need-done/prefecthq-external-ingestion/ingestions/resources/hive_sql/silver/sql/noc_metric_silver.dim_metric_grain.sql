CREATE TABLE hive.noc_metrics_gold.dim_metric_grain AS
SELECT DISTINCT
    metric_name,
    agg_type,
    CASE
        WHEN metric_ts % 3600 = 0 THEN 'hourly'
        WHEN metric_ts % 300  = 0 THEN '5min'
        ELSE 'raw'
    END AS grain
FROM hive.noc_metrics_silver.metrics;
