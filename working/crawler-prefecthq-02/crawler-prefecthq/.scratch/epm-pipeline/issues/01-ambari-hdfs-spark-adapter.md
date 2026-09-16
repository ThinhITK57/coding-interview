# 01: Ambari HDFS & YARN Spark Infrastructure Adapter

**What to build:**
Provide a production-grade Spark session adapter that connects seamlessly to the on-premise Ambari Hadoop cluster (HDFS + YARN), decoupling batch ETL execution from the shared Apache Zeppelin notebook environment. The adapter must dynamically resolve `HADOOP_CONF_DIR` (`core-site.xml`, `hdfs-site.xml`, `yarn-site.xml`), target a dedicated YARN queue (`--queue etl_production`), enforce dynamic resource allocation, and support both `hdfs://namenode:8020/` and WebHDFS protocols.

**Blocked by:** None (can start immediately)

**Status:** ready-for-agent

- [ ] Implement `AmbariSparkConfig` in `transform/spark_session.py` supporting Ambari client configs (`HADOOP_CONF_DIR`, Kerberos/Simple auth, HDFS NameNode HA).
- [ ] Provide standalone `spark-submit` runner script with dedicated YARN queue parameters (`--master yarn`, `--deploy-mode cluster/client`, `--queue etl_production`, `--driver-memory 4G`, `--executor-memory 8G`).
- [ ] Verify connectivity and read/write capabilities on HDFS directories: `/user/lakehouse/bronze`, `/user/lakehouse/silver`, `/user/lakehouse/gold`.
- [ ] Ensure full backward compatibility with local fallback and S3/MinIO for developer machines.
