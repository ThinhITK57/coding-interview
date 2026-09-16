CREATE TABLE hive.noc_metrics_gold.dim_date AS
SELECT DISTINCT
    dt,
    year(dt)  AS year,
    month(dt) AS month,
    day(dt)   AS day
FROM hive.noc_metrics_silver.metrics
WHERE dt IS NOT NULL;
