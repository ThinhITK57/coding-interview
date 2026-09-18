# 03: Decoupled Background Backfill Worker

**What to build:** An asynchronous, non-blocking backfill background worker that continuously or periodically checks MinIO health, scans the local manifest queue for pending fallback files, streams them to MinIO under their intended partition paths, verifies content integrity, and atomically transitions manifest records to `COMMITTED` before cleaning up local buffer files, ensuring regular crawl freshness is never compromised.

**Blocked by:** 02: Atomic MinIO Sink & Iceberg-style Buffer Manifest

**Status:** closed

- [x] Runs independently in the background (as an async task or standalone daemon) without blocking active crawl executions.
- [x] Probes MinIO connectivity before initiating any transfer.
- [x] Reads pending items from the manifest commit log in FIFO order.
- [x] Uploads buffered `.json.gz` files to their original intended MinIO keys and verifies file size and checksum.
- [x] Atomically updates the manifest record status to `COMMITTED` and safely deletes the local buffer file.
- [x] Handles partial failures gracefully without duplicating objects or deleting unverified local files.
