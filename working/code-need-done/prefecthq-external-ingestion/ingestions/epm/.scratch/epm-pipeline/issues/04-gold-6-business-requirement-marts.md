# 04: Gold 6 Business Requirement Data Marts (Batch Tables & Views)

**What to build:**
Transform clean Silver data into the 6 Business Requirement Gold tables and views (`br01_bsc_yearly`, `br02_dieu_hanh_cvct_klcd`, `br03_task_report`, `br04_project_report`, `br05_user_access_traffic`, `br06_board_objectives`). Strictly enforce the Data Engineering boundary: contain ONLY the exact conformed columns requested by the DA/Business specs, with ZERO derived calculations in physical tables. Support both physical Parquet persistence on HDFS and lightweight SQL views on Spark/Trino.

**Blocked by:**
- 02: Bronze to Silver Automated Conformance & Deduplication Engine

**Status:** ready-for-agent

- [ ] Execute `spark/silver_to_gold.py` to write Snappy Parquet to HDFS `/user/lakehouse/gold/epm/` or Hive `bi_gold`.
- [ ] Create and verify `CREATE OR REPLACE VIEW` DDLs in Spark SQL (Zeppelin/Metastore) and Trino.
- [ ] Verify column contracts for all 6 tables against business specifications (BR-01 to BR-06).
- [ ] Verify that no derived calculation columns (achievement_rate, gap, is_overdue) exist in physical tables.
