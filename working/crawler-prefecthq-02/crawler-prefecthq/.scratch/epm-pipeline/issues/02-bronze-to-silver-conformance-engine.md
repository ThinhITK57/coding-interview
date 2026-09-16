# 02: Bronze to Silver Automated Conformance & Deduplication Engine

**What to build:**
A robust, schema-contract-enforced Spark ETL pipeline (`spark/bronze_to_silver.py`) that reads raw multi-entity Parquet files from HDFS Bronze (`/user/lakehouse/bronze/clarizen/{endpoint}`), validates and conforms columns against `data_type/*_dataType.sql` via `SchemaContract`, executes primary-key deduplication (`sysid` + `last_updated_on`) via `DedupEngine`, handles corrupt records and schema evolution gracefully, and writes clean partitioned Parquet to HDFS Silver (`/user/lakehouse/silver/epm/epm_{table}`).

**Blocked by:**
- 01: Ambari HDFS & YARN Spark Infrastructure Adapter

**Status:** ready-for-agent

- [ ] Connect `spark/bronze_to_silver.py` to HDFS storage via the Ambari adapter.
- [ ] Conforming all 5 core entities (`tasks`, `projects`, `targets`, `objectives`, `c_assignments`) plus `user_access_log`.
- [ ] Implement deduplication audit logging (record counts before/after dedup, dropped duplicate counts).
- [ ] Partition Silver Parquet on HDFS by `ingest_date` using Snappy compression.
- [ ] Register/refresh Hive external tables in `bi_silver` metastore over the HDFS paths.
