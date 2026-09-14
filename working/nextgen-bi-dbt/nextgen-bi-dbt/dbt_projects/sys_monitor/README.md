# Lightdash System KPI dbt project

This dbt project is a system-observability companion to `financial_lightdash_dbt_project`.

It is designed to monitor the Lightdash system itself:

- runtime KPIs from the audit doc, especially the Prometheus-backed A/C items
- core inventory counts such as users, projects, charts, dashboards, and AI agents
- operational activity from jobs, scheduler logs, AI prompts, and AI tool calls

## Prometheus-backed KPI catalog

The audit doc marks these KPI groups as Prometheus-facing:

- `A1`, `A2`, `A4`, `A7`
- `C1`, `C2`, `C5`

In this project, those rows are marked with `is_prometheus_ticked = true` in `dim_system_kpi_catalog`.
The rest of the A/C items stay in the catalog with `false` so you can track the full control surface.

## Source tables

The project expects the system data to already exist in your warehouse.
Update `models/sources.yml` with the right `database` and `schema` for your environment.

The source set covers:

- `users`
- `projects`
- `saved_queries`
- `dashboards`
- `ai_agent`
- `ai_thread`
- `ai_prompt`
- `ai_agent_tool_call`
- `managed_agent_settings`
- `managed_agent_actions`
- `scheduler_log`
- `jobs`
- `job_steps`
- `analytics_chart_views`
- `analytics_dashboard_views`
- `analytics_app_views`

## Useful formulas included

The models include formula-style metrics for:

- Prometheus KPI coverage rate
- latest captured KPI coverage rate
- active user rate
- active chart rate
- AI data-access enablement rate
- job success rate
- scheduler failure rate
- AI prompt error rate
- inventory share by group

These are defined with Lightdash/dbt YAML metrics and reference the official Lightdash metric syntax.

## Run

```bash
dbt debug
dbt compile
dbt run
dbt test
```

If you want to use this project in Lightdash, connect the dbt repo and the warehouse schema that contains these tables.

This project is pinned to the local pgvector Postgres export at `localhost:5433`.
If you want to use a different warehouse later, update `profiles.yml`.
