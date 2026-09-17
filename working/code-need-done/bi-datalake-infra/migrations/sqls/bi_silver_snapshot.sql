SHOW SCHEMAS FROM hive;

CREATE SCHEMA  IF NOT EXISTS hive.bi_silver_snapshot 
with (location='s3a://vcs-silver/bi-silver-snapshot/');


CREATE SCHEMA IF NOT EXISTS hive.bi_silver_snapshot;