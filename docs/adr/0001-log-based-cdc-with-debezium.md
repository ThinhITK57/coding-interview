# ADR 0001: Log-Based Change Data Capture (CDC) using Debezium & PostgreSQL WAL

* **Status**: Accepted
* **Date**: 2026-07-28

## Context & Problem Statement

The Fleet Maintenance & Repair Platform relies on Odoo (running on PostgreSQL) as its core OLTP system for managing Head locations, inventory, repair work orders, and billing. We need to extract data continuously for two distinct purposes:
1. Low-latency synchronization of Head master data & inventory into Redis for real-time driver matching.
2. Historical batch ETL into the Data Warehouse for financial and operational reporting.

Traditional database polling (`SELECT * FROM table WHERE updated_at > last_poll`) incurs high CPU/IO load on the OLTP database, cannot reliably capture deleted records, and introduces polling latency.

## Decision Drivers

* **Zero Performance Impact on OLTP**: Searching drivers and batch jobs must not slow down active Odoo repair operations.
* **Complete Change Audit**: Must capture all INSERT, UPDATE, and DELETE mutations in exact order.
* **Low Latency**: Head inventory changes must reflect in matching within seconds.

## Considered Options

1. **Periodic Query Polling (`SELECT ... WHERE updated_at > t`)**: High DB load, misses hard deletes, polling delay.
2. **Application-Level Dual Writes**: Odoo code writes to DB and emits Kafka events. Risk of split-brain/inconsistency if DB write succeeds but Kafka publish fails.
3. **Log-Based CDC via Debezium + Postgres WAL**: Non-intrusive log reading via PostgreSQL Write-Ahead Logging (WAL) and `pgoutput` logical decoding plugin.

## Decision Outcome

Chosen Option: **Option 3 (Log-Based CDC via Debezium)**.

### Positive Consequences

* **Zero Query Load on OLTP**: Debezium reads raw WAL logs directly from disk/replication slot.
* **Exact Capture**: Captures hard DELETEs and intermediate UPDATEs in transaction commit order.
* **Multi-Consumer Pattern**: Single Kafka CDC topic `head-master-data-cdc` can be consumed independently by Redis Sync Job (low latency) and HDFS Data Lake Sink (batch).

### Negative Consequences / Trade-offs

* **WAL Storage Retention**: PostgreSQL WAL replication slots must be monitored. If Debezium is offline for extended periods, WAL logs can fill disk space unless `max_slot_wal_keep_size` is configured.
* **At-Least-Once Delivery Handling**: Downstream consumers must implement idempotent processing using transaction LSN and primary keys.
