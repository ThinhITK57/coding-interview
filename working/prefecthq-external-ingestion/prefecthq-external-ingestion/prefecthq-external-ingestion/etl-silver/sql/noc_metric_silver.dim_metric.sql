SELECT DISTINCT
    metric_name,
    agg_type
FROM hive.noc_metrics_silver.metrics
WHERE metric_name IS NOT NULL;