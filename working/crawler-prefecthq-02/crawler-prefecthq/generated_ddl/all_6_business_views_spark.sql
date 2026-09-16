-- =============================================================================
-- SPARK SQL DDL: 6 BUSINESS REQUIREMENT VIEWS (DATA WAREHOUSE / ZEPPELIN)
-- Target: Spark SQL 2.3.2 | Catalog / Metastore: Hive bi_silver / bi_gold
--
-- Strict Data Engineering Boundary:
--   - Contains ONLY and STRICTLY the columns requested by the 6 Business Requirements.
--   - Zero DE-derived columns/metrics in this view layer.
--   - All derived measures (achievement_rate, gap, is_overdue, etc.) are computed
--     dynamically via dbt Semantic Layer and GenBI metrics.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- BR-01: Báo cáo BSC trong năm
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW bi_gold.vw_br01_bsc_yearly AS
SELECT
    t.associated_objective,
    t.associated_item,
    t.target_type,
    t.parent_target,
    t.c_department,
    t.name,
    t.c_assignee,
    t.unit,
    CAST(COALESCE(t.c_target_date_m, t.target_date) AS DATE) AS target_date_m,
    CAST(COALESCE(t.c_target_value_m, t.target_value) AS DOUBLE) AS target_value_m,
    CAST(t.c_target_date_n AS DATE) AS target_date_n,
    CAST(t.c_target_value_n AS DOUBLE) AS target_value_n,
    t.state,
    t.status,
    CAST(COALESCE(t.c_target_result_value_m, t.c_target_result_m) AS DOUBLE) AS target_result_m,
    CAST(COALESCE(t.c_target_result_value_n, t.c_target_result_n) AS DOUBLE) AS target_result_n,
    COALESCE(a.c_assignor, t.c_assignor, t.created_by) AS assignor
FROM bi_silver.epm_targets t
LEFT JOIN bi_silver.epm_c_assignments a
    ON t.c_associated_assignment = a.sysid;


-- -----------------------------------------------------------------------------
-- BR-02: Báo cáo điều hành CVCT/KLCĐ
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW bi_gold.vw_br02_dieu_hanh_cvct_klcd AS
SELECT
    COALESCE(t.c_assignee, t.entity_owner) AS assignee,
    CAST(t.resources_and_placeholders_count AS STRING) AS resources,
    t.name,
    t.status,
    CAST(COALESCE(t.c_target_date_m, t.target_date) AS DATE) AS due_date,
    CAST(t.percent_completed AS DOUBLE) AS percent_completed,
    CAST(COALESCE(t.c_target_value_m, t.target_value) AS DOUBLE) AS target_value_m,
    CAST(COALESCE(t.c_target_date_m, t.target_date) AS DATE) AS target_date_m
FROM bi_silver.epm_targets t;


-- -----------------------------------------------------------------------------
-- BR-03: Báo cáo Task
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW bi_gold.vw_br03_task_report AS
SELECT
    t.task_type,
    COALESCE(t.manager, t.c_assignee) AS assignee,
    t.c_department AS department,
    CAST(COALESCE(t.all_user_resources_count, t.resources_and_placeholders_count) AS STRING) AS resources,
    t.parent_project,
    COALESCE(t.track_status, t.status) AS jira_status,
    t.name,
    t.description,
    CAST(t.work AS DOUBLE) AS work,
    CAST(t.duration AS DOUBLE) AS duration,
    COALESCE(t.c_epm_default, t.default_integration_path) AS epm_default,
    CAST(t.start_date AS DATE) AS start_date,
    CAST(t.due_date AS DATE) AS due_date,
    CAST(t.percent_completed AS DOUBLE) AS percent_completed,
    COALESCE(t.c_update_description, t.overview) AS update_description
FROM bi_silver.epm_tasks t;


-- -----------------------------------------------------------------------------
-- BR-04: Báo cáo Dự án
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW bi_gold.vw_br04_project_report AS
SELECT
    p.name,
    COALESCE(p.c_internal_type, p.project_type) AS project_type,
    CAST(p.due_date AS DATE) AS due_date,
    p.state,
    COALESCE(p.track_status, p.status) AS status,
    CAST(p.percent_completed AS DOUBLE) AS percent_completed,
    p.c_department AS department,
    COALESCE(p.c_assignor, p.created_by) AS assignor,
    p.c_assignee AS assignee,
    COALESCE(p.project_manager, p.manager) AS project_manager,
    COALESCE(p.c_action_resources, CAST(p.resources_and_placeholders_count AS STRING)) AS resources
FROM bi_silver.epm_projects p;


-- -----------------------------------------------------------------------------
-- BR-05: Báo cáo Lưu lượng truy cập (User Access Log)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW bi_gold.vw_br05_user_access_traffic AS
SELECT
    CAST(u.login_date AS DATE) AS login_date,
    u.name,
    u.first_name,
    u.last_name,
    u.groups,
    u.direct_manager,
    u.job_title
FROM bi_silver.epm_user_access_log u;


-- -----------------------------------------------------------------------------
-- BR-06: Báo cáo Mục tiêu Ban Giám đốc (Mục tiêu BG)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW bi_gold.vw_br06_board_objectives AS
SELECT
    t.associated_objective,
    t.associated_item,
    t.target_type,
    t.parent_target,
    t.c_department,
    t.name,
    t.c_assignee,
    t.unit,
    CAST(t.c_target_date_m AS DATE) AS c_target_date_m,
    CAST(t.c_target_date_n AS DATE) AS c_target_date_n,
    CAST(t.c_target_value_m AS DOUBLE) AS c_target_value_m,
    CAST(t.c_target_value_n AS DOUBLE) AS c_target_value_n,
    CAST(COALESCE(t.c_target_result_value_m, t.c_target_result_m) AS DOUBLE) AS c_target_result_m,
    CAST(COALESCE(t.c_target_result_value_n, t.c_target_result_n) AS DOUBLE) AS c_target_result_n,
    t.state,
    t.status,
    COALESCE(a.c_assignor, t.c_assignor, t.created_by) AS c_assignor
FROM bi_silver.epm_targets t
LEFT JOIN bi_silver.epm_c_assignments a
    ON t.c_associated_assignment = a.sysid;
