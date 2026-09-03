# ADR 0002: Multi-Role Redis Architecture (Search/Serving Store, Concurrency Control, and Pub/Sub Cache)

* **Status**: Accepted
* **Date**: 2026-07-28

## Context & Problem Statement

The platform requires specialized memory-speed capabilities across three distinct functional layers:
1. **Head Matching**: Sub-second geospatial distance query + inventory lookup for drivers finding nearby repair centers.
2. **Slot Reservation**: Preventing multiple drivers from simultaneously booking the same Head time slot (overbooking / concurrency race condition).
3. **Dashboard Real-time Notification**: Notifying the backend layer immediately when new Airflow/Spark batch reporting aggregates are published.

## Decision Drivers

* **Sub-millisecond Read Latency**: Searching Heads must be instantaneous.
* **Geospatial & Complex Data Structures**: Support for GEO indexing (`GEOADD`, `GEORADIUS`/`GEOSEARCH`) and Hash structures (`HSET`, `HGETALL`).
* **Atomic Primitives**: Atomic locks and conditional key setting (`SET key value NX PX ttl`) to handle booking concurrency.
* **Pub/Sub Capability**: Lightweight messaging to trigger push events.

## Decision Outcome

Chosen Option: **Single Multi-Role Redis Deployment** (segmented by key namespaces and databases).

### Redis Roles Allocated

1. **Role #1 — Search & Serving Store**:
   - `geo:heads` (Redis GEO Set for latitude/longitude location of all Heads).
   - `head:parts:{head_id}` (Redis Hash for live parts inventory and pricing, synced from Debezium CDC).
2. **Role #2 — Concurrency Control (Atomic Reservation)**:
   - `reservation:{head_id}:{timeslot}` (Key with TTL using atomic `SET NX PX` or Lua script to lock slots).
3. **Role #3 — Cache & Pub/Sub Layer**:
   - `report:agg:{report_type}` (Redis Key storing latest pre-aggregated metrics from Spark).
   - Channel `channel:report-updates` (Redis Pub/Sub channel emitting event notifications when Airflow finishes a report batch).

### Positive Consequences

* Single high-performance infrastructure component handles geospatial search, atomic booking, and event notification.
* Completely decouples driver searches from OLTP PostgreSQL.

### Negative Consequences / Trade-offs

* Memory usage must be monitored; TTLs must be enforced on booking keys.
* Redis Pub/Sub is not persistent (fire-and-forget); backend layer must handle reconnection by reading latest Redis keys.
