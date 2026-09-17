# 06: Prefect HQ & YARN Decoupled Pipeline Orchestrator

**What to build:**
Configure and deploy Prefect HQ flows that orchestrate the end-to-end pipeline on scheduled intervals (API extraction -> Bronze HDFS -> Spark Silver -> Spark Gold / dbt run), replacing fragile Zeppelin notebook scheduled jobs. All Spark batch jobs must run directly on YARN (`--queue etl_production`) or via headless Livy Batch API (`POST /batches`), with checkpoint recovery, failure notifications, and complete isolation from interactive Zeppelin notebook sessions.

**Blocked by:**
- 02: Bronze to Silver Automated Conformance & Deduplication Engine
- 04: Gold 6 Business Requirement Data Marts (Batch Tables & Views)
- 05: NextGen-BI dbt Semantic Layer & Lightdash Metrics Integration

**Status:** ready-for-agent

- [ ] Configure `prefect.yaml` with the 8 scheduled deployments matching the business crawl strategy.
- [ ] Connect Prefect flow tasks to trigger headless Spark batch jobs on YARN rather than Zeppelin notebooks.
- [ ] Implement checkpoint persistence to HDFS/local to resume interrupted extractions without data loss.
- [ ] Implement alerting and audit artifact generation in Prefect HQ UI.
