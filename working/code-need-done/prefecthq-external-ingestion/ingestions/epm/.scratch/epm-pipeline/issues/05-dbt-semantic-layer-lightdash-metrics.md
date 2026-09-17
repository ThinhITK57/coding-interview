# 05: NextGen-BI dbt Semantic Layer & Lightdash Metrics Integration

**What to build:**
Integrate the 6 Business Requirement models into the active dbt repository (`working/nextgen-bi-dbt/nextgen-bi-dbt/dbt_projects/epm`), defining all derived business metrics (`achievement_rate`, `gap`, `is_overdue`, `remaining_work`, `dau`, etc.) in the exact Lightdash/dbt `meta.metrics` specification matching the CRM template (`crm_activity_history__schema.yml`). Verify that `dbt compile` and `dbt test` pass cleanly.

**Blocked by:**
- 04: Gold 6 Business Requirement Data Marts (Batch Tables & Views)

**Status:** ready-for-agent

- [ ] Place models (`br01` to `br06`) and corresponding `__schema.yml` in `dbt_projects/epm/models/`.
- [ ] Define `meta.semantic_guidance` with `metric_rules` and `restrictions` for each model.
- [ ] Define `meta.metrics` using `${column}` syntax, typed aggregates (`count`, `count_distinct`, `sum`, `average`, `number`), and conditional logic (`CASE WHEN ...`).
- [ ] Run `dbt compile` and `dbt test` against Trino / Hive metastore without errors.
- [ ] Ensure GenBI compatibility for dynamic natural language querying of all defined metrics.
