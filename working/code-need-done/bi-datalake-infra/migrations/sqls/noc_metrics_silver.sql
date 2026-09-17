CREATE SCHEMA hive.noc_metrics_silver 
with (location='s3a://vcs-silver/noc-metrics-silver/');

-- hive.noc_metrics_silver.dim_date definition

CREATE TABLE hive.noc_metrics_silver.dim_date (
   dt date,
   year integer,
   month integer,
   day integer
)
WITH (
   format = 'PARQUET'
);


-- hive.noc_metrics_silver.dim_entity definition

CREATE TABLE hive.noc_metrics_silver.dim_entity (
   customer varchar,
   project varchar,
   host varchar
)
WITH (
   format = 'PARQUET'
);


-- hive.noc_metrics_silver.dim_metric definition

CREATE TABLE hive.noc_metrics_silver.dim_metric (
   metric_name varchar,
   agg_type varchar
)
WITH (
   format = 'PARQUET'
);


-- hive.noc_metrics_silver.dim_metric_grain definition

CREATE TABLE hive.noc_metrics_silver.dim_metric_grain (
   metric_name varchar,
   agg_type varchar,
   grain varchar
)
WITH (
   format = 'PARQUET'
);


-- hive.noc_metrics_silver.dim_time definition

CREATE TABLE hive.noc_metrics_silver.dim_time (
   metric_ts bigint,
   metric_datetime timestamp(3),
   hour integer,
   minute integer
)
WITH (
   format = 'PARQUET'
);


-- hive.noc_metrics_silver.metrics definition

CREATE TABLE hive.noc_metrics_silver.metrics (
   metric_name varchar,
   agg_type varchar,
   customer varchar,
   project varchar,
   host varchar,
   metric_value double,
   metric_ts bigint,
   metric_time varchar,
   dt date
)
WITH (
   format = 'PARQUET'
);