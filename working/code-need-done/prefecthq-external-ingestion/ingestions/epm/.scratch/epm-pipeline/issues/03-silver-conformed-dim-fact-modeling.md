# 03: Silver Conformed Dimensions & Facts Modeling

**What to build:**
Implement the Kimball Star/Constellation modeling layer on Silver as outlined in `phan-tich-toan-ven.md`. Build the 7 Conformed Dimensions (`dim_date`, `dim_department`, `dim_resource`, `dim_project` with SCD Type 2 tracking, `dim_task`, `dim_objective`, `dim_assignment`) and the core Fact snapshot tables (`fact_target_bsc_snapshot`, `fact_task_execution_snapshot`, etc.) to serve both the 6 Business Requirements and general ad-hoc queries, without polluting physical tables with derived DA business formulas.

**Blocked by:**
- 02: Bronze to Silver Automated Conformance & Deduplication Engine

**Status:** ready-for-agent

- [ ] Implement `dim_date` generator covering 2020-2030 with fiscal and calendar periods.
- [ ] Implement cross-table union dimension builders (`dim_department`, `dim_resource`).
- [ ] Implement `dim_project` SCD Type 2 builder tracking PM, Department, and status changes over time.
- [ ] Implement hierarchy flattening for `dim_objective` and `dim_assignment`.
- [ ] Build daily snapshot fact tables linking to conformed dimensions.
- [ ] Expose views in Hive/Trino schema `bi_silver`.
