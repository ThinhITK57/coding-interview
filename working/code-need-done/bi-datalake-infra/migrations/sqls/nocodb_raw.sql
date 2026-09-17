CREATE SCHEMA hive.nocodb_raw 
with (location='s3a://vcs-raw/nocodb-raw/');

-- hive.nocodb_raw.kpi_systems definition

CREATE TABLE hive.nocodb_raw.kpi_systems (
   id bigint,
   title varchar,
   createdat varchar,
   updatedat varchar,
   starttime varchar,
   endtime varchar,
   value double,
   before1periodvalue double,
   before2periodsvalue double,
   targetdifference double,
   before2periodsdifference double,
   before1perioddifference double,
   result boolean,
   trend varchar,
   criteriatarget double,
   criteriaperiodtype varchar,
   criteriaoperator varchar,
   criteriaunit varchar,
   dws_kpi_systems_id double,
   dws_kpi_criterias_id double,
   criteriacode varchar,
   influxdbquery varchar,
   influxdbresult varchar,
   calculatetime varchar,
   error varchar,
   isempty boolean,
   dws_kpi_systems varchar,
   customername varchar,
   customertype varchar,
   dws_kpi_criterias ROW(id bigint, name varchar),
   productname varchar,
   dws_kpi_criterias_name varchar,
   dws_kpi_systems_name varchar,
   starttime_ts bigint,
   endtime_ts bigint
)
WITH (
   format = 'PARQUET'
);