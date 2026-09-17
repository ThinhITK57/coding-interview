CREATE TABLE hive.noc_metrics_gold.dim_entity AS
SELECT DISTINCT
    customer,
    project,
    host
FROM hive.noc_metrics_silver.metrics
WHERE customer IS NOT NULL
   OR project IS NOT NULL
   OR host IS NOT NULL;