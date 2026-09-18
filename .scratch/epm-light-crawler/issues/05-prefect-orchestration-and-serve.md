# 05: Prefect Orchestration & Multi-Schedule Serve

**What to build:** An end-to-end orchestration suite built with Prefect that packages extraction, sinking, audit logging, and the background backfill worker into production flows, served via `prefect.serve()` with 9 distinct deployment schedules adhering strictly to the enterprise crawl strategy defined in `CRAWL-DATA-PLAN.md`.

**Blocked by:** 03: Decoupled Background Backfill Worker, 04: Enterprise Audit Tracking & Observability Engine

**Status:** closed

- [x] Implements main Prefect flow `crawl_epm_endpoint(endpoint_name, mode)` orchestrating Extract, Sink, and Audit Tracking.
- [x] Implements `master_dag_flow` sequencing the 5 EPM endpoints according to dependency order.
- [x] Implements `background_backfill_flow` scheduled to periodically drain any buffered fallback files.
- [x] Provides `main_serve.py` utilizing `prefect.serve()` to configure and register all 9 enterprise deployments:
  - Tasks (2-Hourly incremental, Mon-Fri 08:00-18:00)
  - Tasks (Weekly full sync, Sun 23:00 / Sun 02:00)
  - Projects (Midday 12:00, Evening 18:30)
  - Targets (Midday 12:30, Evening 19:00)
  - Objectives (Daily full sync, 03:30)
  - Assignments (Daily full sync, 03:00)
  - Master DAG (Daily consolidated, 04:00)
  - Background Backfill (Periodic drain every 30 mins)
  - Ad-hoc Manual Run
- [x] Provides clean Dockerfile and environment setup instructions with zero Spark/JVM footprint.
