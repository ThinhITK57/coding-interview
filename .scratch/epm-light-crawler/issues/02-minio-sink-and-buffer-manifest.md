# 02: Atomic MinIO Sink & Iceberg-style Buffer Manifest

**What to build:** An immutable, partition-aware storage sink that packages extracted JSON batches into compressed gzip objects and streams them directly to MinIO. When MinIO is unreachable, it atomically diverts files to a local buffer directory and logs a manifest commit entry into a local pending queue (mirroring Iceberg's decoupled data-file commit pattern) so that extraction never halts due to network blips.

**Blocked by:** 01: Core Lightweight Extractor & Persistent Daily Quota Manager

**Status:** closed

- [x] Formats raw records into JSON lines and compresses them into `.json.gz` files.
- [x] Streams compressed files directly to MinIO under standard Lakehouse Bronze paths: `lakehouse/bronze/clarizen/{endpoint}/year=YYYY/month=MM/day=DD/{batch_id}.json.gz`.
- [x] Automatically detects MinIO connection failures and falls back to a structured local buffer `./data/buffer/{endpoint}/...`.
- [x] Writes a persistent manifest entry (`batch_id`, `endpoint`, `local_path`, `intended_s3_uri`, `record_count`, `created_at`, `status='PENDING'`) into an operational SQLite database or manifest file.
- [x] Does not perform any transformation, schema conversion, or DDL generation—stores pure immutable Bronze payload.
