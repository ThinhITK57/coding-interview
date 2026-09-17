CREATE SCHEMA hive.noc_metrics_raw 
with (location='s3a://vcs-raw/noc-metrics-raw/');

-- hive.noc_metrics_raw.cpu_usage_average__max definition

CREATE TABLE hive.noc_metrics_raw.cpu_usage_average__max (
   name varchar,
   customer varchar,
   host varchar,
   project varchar,
   tags ROW(name varchar),
   time varchar,
   max double,
   time_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.noc_metrics_raw.cpu_usage_average__mean definition

CREATE TABLE hive.noc_metrics_raw.cpu_usage_average__mean (
   name varchar,
   customer varchar,
   host varchar,
   project varchar,
   tags ROW(name varchar),
   time varchar,
   mean double,
   time_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.noc_metrics_raw.cpu_usage_average__min definition

CREATE TABLE hive.noc_metrics_raw.cpu_usage_average__min (
   name varchar,
   customer varchar,
   host varchar,
   project varchar,
   tags ROW(name varchar),
   time varchar,
   min double,
   time_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.noc_metrics_raw.disk_project__io_util__max definition

CREATE TABLE hive.noc_metrics_raw.disk_project__io_util__max (
   name varchar,
   customer varchar,
   host varchar,
   project varchar,
   tags ROW(name varchar),
   time varchar,
   max double,
   time_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.noc_metrics_raw.disk_project__io_util__mean definition

CREATE TABLE hive.noc_metrics_raw.disk_project__io_util__mean (
   name varchar,
   customer varchar,
   host varchar,
   project varchar,
   tags ROW(name varchar),
   time varchar,
   mean double,
   time_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.noc_metrics_raw.disk_project__io_util__min definition

CREATE TABLE hive.noc_metrics_raw.disk_project__io_util__min (
   name varchar,
   customer varchar,
   host varchar,
   project varchar,
   tags ROW(name varchar),
   time varchar,
   min double,
   time_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.noc_metrics_raw.disk_used_percent__max definition

CREATE TABLE hive.noc_metrics_raw.disk_used_percent__max (
   name varchar,
   customer varchar,
   host varchar,
   project varchar,
   tags ROW(path varchar),
   time varchar,
   max double,
   time_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.noc_metrics_raw.disk_used_percent__mean definition

CREATE TABLE hive.noc_metrics_raw.disk_used_percent__mean (
   name varchar,
   customer varchar,
   host varchar,
   project varchar,
   tags ROW(path varchar),
   time varchar,
   mean double,
   time_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.noc_metrics_raw.disk_used_percent__min definition

CREATE TABLE hive.noc_metrics_raw.disk_used_percent__min (
   name varchar,
   customer varchar,
   host varchar,
   project varchar,
   tags ROW(path varchar),
   time varchar,
   min double,
   time_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.noc_metrics_raw.free_space__capacity definition

CREATE TABLE hive.noc_metrics_raw.free_space__capacity (
   name varchar,
   customer varchar,
   host varchar,
   project varchar,
   tags ROW(ds_name varchar),
   time varchar,
   last_last double,
   time_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.noc_metrics_raw.la_per_cpu__max definition

CREATE TABLE hive.noc_metrics_raw.la_per_cpu__max (
   name varchar,
   customer varchar,
   host varchar,
   project varchar,
   tags varchar,
   time varchar,
   max double,
   time_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.noc_metrics_raw.la_per_cpu__mean definition

CREATE TABLE hive.noc_metrics_raw.la_per_cpu__mean (
   name varchar,
   customer varchar,
   host varchar,
   project varchar,
   tags varchar,
   time varchar,
   mean double,
   time_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.noc_metrics_raw.la_per_cpu__min definition

CREATE TABLE hive.noc_metrics_raw.la_per_cpu__min (
   name varchar,
   customer varchar,
   host varchar,
   project varchar,
   tags varchar,
   time varchar,
   min double,
   time_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.noc_metrics_raw.mem_project__util__max definition

CREATE TABLE hive.noc_metrics_raw.mem_project__util__max (
   name varchar,
   customer varchar,
   host varchar,
   project varchar,
   tags varchar,
   time varchar,
   max double,
   time_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.noc_metrics_raw.mem_project__util__mean definition

CREATE TABLE hive.noc_metrics_raw.mem_project__util__mean (
   name varchar,
   customer varchar,
   host varchar,
   project varchar,
   tags varchar,
   time varchar,
   mean double,
   time_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.noc_metrics_raw.mem_project__util__min definition

CREATE TABLE hive.noc_metrics_raw.mem_project__util__min (
   name varchar,
   customer varchar,
   host varchar,
   project varchar,
   tags varchar,
   time varchar,
   min double,
   time_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.noc_metrics_raw.mem_usage_average__max definition

CREATE TABLE hive.noc_metrics_raw.mem_usage_average__max (
   name varchar,
   customer varchar,
   host varchar,
   project varchar,
   tags ROW(name varchar),
   time varchar,
   max double,
   time_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.noc_metrics_raw.mem_usage_average__mean definition

CREATE TABLE hive.noc_metrics_raw.mem_usage_average__mean (
   name varchar,
   customer varchar,
   host varchar,
   project varchar,
   tags ROW(name varchar),
   time varchar,
   mean double,
   time_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.noc_metrics_raw.mem_usage_average__min definition

CREATE TABLE hive.noc_metrics_raw.mem_usage_average__min (
   name varchar,
   customer varchar,
   host varchar,
   project varchar,
   tags ROW(name varchar),
   time varchar,
   min double,
   time_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.noc_metrics_raw.net_customer__util__max definition

CREATE TABLE hive.noc_metrics_raw.net_customer__util__max (
   name varchar,
   customer varchar,
   host varchar,
   project varchar,
   tags ROW(speed varchar),
   time varchar,
   max double,
   time_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.noc_metrics_raw.net_customer__util__mean definition

CREATE TABLE hive.noc_metrics_raw.net_customer__util__mean (
   name varchar,
   customer varchar,
   host varchar,
   project varchar,
   tags ROW(speed varchar),
   time varchar,
   mean double,
   time_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.noc_metrics_raw.net_customer__util__min definition

CREATE TABLE hive.noc_metrics_raw.net_customer__util__min (
   name varchar,
   customer varchar,
   host varchar,
   project varchar,
   tags ROW(speed varchar),
   time varchar,
   min double,
   time_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.noc_metrics_raw.swap_used_percent__max definition

CREATE TABLE hive.noc_metrics_raw.swap_used_percent__max (
   name varchar,
   customer varchar,
   host varchar,
   project varchar,
   tags varchar,
   time varchar,
   max double,
   time_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.noc_metrics_raw.swap_used_percent__mean definition

CREATE TABLE hive.noc_metrics_raw.swap_used_percent__mean (
   name varchar,
   customer varchar,
   host varchar,
   project varchar,
   tags varchar,
   time varchar,
   mean double,
   time_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.noc_metrics_raw.swap_used_percent__min definition

CREATE TABLE hive.noc_metrics_raw.swap_used_percent__min (
   name varchar,
   customer varchar,
   host varchar,
   project varchar,
   tags varchar,
   time varchar,
   min double,
   time_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.noc_metrics_raw.usage__max definition

CREATE TABLE hive.noc_metrics_raw.usage__max (
   name varchar,
   customer varchar,
   host varchar,
   project varchar,
   tags varchar,
   time varchar,
   max double,
   time_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.noc_metrics_raw.usage__mean definition

CREATE TABLE hive.noc_metrics_raw.usage__mean (
   name varchar,
   customer varchar,
   host varchar,
   project varchar,
   tags varchar,
   time varchar,
   mean double,
   time_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.noc_metrics_raw.usage__min definition

CREATE TABLE hive.noc_metrics_raw.usage__min (
   name varchar,
   customer varchar,
   host varchar,
   project varchar,
   tags varchar,
   time varchar,
   min double,
   time_ts bigint
)
WITH (
   format = 'PARQUET'
);