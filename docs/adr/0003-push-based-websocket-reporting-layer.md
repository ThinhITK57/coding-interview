# ADR 0003: Push-Based WebSocket Serving Layer for PowerBI & Dashboards

* **Status**: Accepted
* **Date**: 2026-07-28

## Context & Problem Statement

Executive leadership dashboards require up-to-date profit margin and revenue reports across multiple timeframes (weekly, monthly, quarterly, fiscal year, year-end). PowerBI and standard dashboards traditionally use a **Pull Model** (polling database every N minutes), which has two major flaws:
1. Data latency between refresh cycles.
2. High redundant query load on storage when multiple dashboards poll simultaneously.

Furthermore, PowerBI lacks a native Redis connector.

## Decision Outcome

Chosen Option: **Push-Based Architecture via Intermediary Backend Layer & WebSockets**.

```
[ Spark Batch Job (Airflow) ] ──► [ Writes Aggregates to Redis ]
                                          │
                                          ▼ (Pub/Sub Event)
                                [ WebSocket Backend Server ]
                                          │
                                          ▼ (WebSocket Push)
                                 [ PowerBI / Dashboard ]
```

### Positive Consequences

* **Near-Zero Latency**: Dashboard displays updated numbers the second Spark writes the batch result to Redis.
* **Zero Storage Load**: Eliminates polling queries from multiple browser tabs/dashboards.
* **Decoupled Architecture**: Redis serves as data storage; WebSocket backend handles distribution logic.

### Edge Case Handling (Reconnections)

If a dashboard loses WebSocket connection, upon reconnecting, the backend immediately queries the latest state keys from Redis (`report:agg:*`) to resynchronize the dashboard state before listening to new Pub/Sub messages.
