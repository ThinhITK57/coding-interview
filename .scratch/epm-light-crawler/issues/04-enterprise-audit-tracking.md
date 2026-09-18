# 04: Enterprise Audit Tracking & Observability Engine

**What to build:** An enterprise-grade observability and audit tracking layer that records operational telemetry for every crawl execution (records extracted, total runtime seconds, request counts, retry events, error traces, and watermark intervals) into a queryable local SQLite tracking table and mirrors structured daily audit summaries to MinIO for centralized monitoring and post-mortem incident analysis.

**Blocked by:** 02: Atomic MinIO Sink & Iceberg-style Buffer Manifest

**Status:** closed

- [x] Captures detailed run metrics per endpoint: `batch_id`, `endpoint_name`, `mode`, `start_time`, `end_time`, `duration_seconds`, `records_extracted`, `requests_sent`, `retry_count`, `watermark_start`, `watermark_end`, and `status`.
- [x] Records granular error events (HTTP status codes, timeout incidents, retry backoff times).
- [x] Persists execution metadata into a local operational SQLite table `crawler_audit_runs`.
- [x] Provides an exporter that packages daily audit summaries into `.jsonl` objects and syncs them to `lakehouse/audit/epm_crawler_audit_YYYY-MM-DD.jsonl` on MinIO.
- [x] Exposes a CLI/API method to query recent crawl runs and system health status.
