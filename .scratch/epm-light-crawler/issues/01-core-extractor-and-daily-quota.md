# 01: Core Lightweight Extractor & Persistent Daily Quota Manager

**What to build:** A lightweight, pure-Python API extraction engine for Clarizen that loads endpoint configurations, authenticates via API tokens, executes paginated HTTP requests, enforces rate limiting per minute, and critically manages a persistent daily request quota (persisted across independent process runs) so that repeated runs within the same calendar day never exceed the API provider limit.

**Blocked by:** None (can start immediately)

**Status:** closed

- [x] Can load endpoint definitions and query payloads from `config.json` without any Spark or JVM dependencies.
- [x] Executes paginated queries (`offset`/`limit`) against Clarizen API with automatic token authentication and exponential backoff retry.
- [x] Implements a stateful daily request quota counter stored in a persistent local store (SQLite/atomic file) that accumulates requests across independent runs and raises a clean quota exhaustion alert when reaching the daily ceiling.
- [x] Resets the daily request counter atomically when the UTC date rolls over.
- [x] Emits structured JSON log events for each request, batch, and pagination state.
