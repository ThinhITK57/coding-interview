select
    metric_code,
    metric_category,
    metric_name,
    is_prometheus_ticked,
    measurement_source,
    measurement_frequency,
    owner_note
from (
    values
        ('A1', 'A', 'API Availability', true, 'Prometheus', '5m / daily', 'App /metrics scrape'),
        ('A2', 'A', 'API Latency P95', true, 'Prometheus', '5m / daily', 'APM histogram scrape'),
        ('A3', 'A', 'Scheduler Success Rate', false, 'Scheduler/job store', 'hourly / daily', 'Keep source semantics in job logs'),
        ('A4', 'A', 'Queue Backlog Health', true, 'Prometheus', '5m', 'Queue depth from app/system metrics'),
        ('A5', 'A', 'Incident MTTR', false, 'Incident tracker', 'weekly / monthly', 'Incident store, not app metrics'),
        ('A6', 'A', 'Throughput Headroom', false, 'Load test / APM', 'weekly / monthly', 'Benchmark artifact'),
        ('A7', 'A', 'Saturation Compliance', true, 'Prometheus', '5m / test window', 'Infra metrics scrape'),
        ('C1', 'C', 'TTFT P95', true, 'Prometheus', '1h / daily', 'AI service histogram scrape'),
        ('C2', 'C', 'End-to-End Response P95', true, 'Prometheus', '1h / daily', 'AI service histogram scrape'),
        ('C3', 'C', 'Tool Success Rate', false, 'DB/log/event store', 'daily', 'Tool calls can be computed offline'),
        ('C4', 'C', 'Hallucination Proxy Rate', false, 'DB/log/event store', 'weekly', 'Feedback / dislike store'),
        ('C5', 'C', 'Step-Cap Reached Rate', true, 'Prometheus', 'daily', 'Step counters from AI service'),
        ('C6', 'C', 'Concurrent Session Stability', false, 'Benchmark run', 'release gate', 'Offline benchmark artifact'),
        ('C7', 'C', 'AI Request Availability', false, 'LLM availability test', 'daily', 'Synthetic request checks'),
        ('C8', 'C', 'Policy Denial Correctness', false, 'Negative test suite', 'release gate', 'Governance test set')
) as t(metric_code, metric_category, metric_name, is_prometheus_ticked, measurement_source, measurement_frequency, owner_note)
