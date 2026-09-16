CREATE VIEW hive.jira_silver.jira_issue_task_fct_vw AS
SELECT
    'BUG'              AS source_type,
    issue_key,
    project_key,
    project_name,
    summary,
    issue_type,
    status,
    priority,
    assignee,
    CAST(NULL AS STRING) AS task_type,
    CAST(NULL AS DOUBLE) AS msn_type,
    CAST(NULL AS STRING) AS assignor,
    CAST(NULL AS STRING) AS work_group,
    CAST(NULL AS STRING) AS department_center,
    created_time,
    updated_time,
    resolved_time,
    due_date_time,
    CAST(NULL AS TIMESTAMP) AS start_date_time
FROM jira_silver.bug_prod

UNION ALL
SELECT
    'DEFECT',
    issue_key,
    project_key,
    project_name,
    summary,
    CAST(NULL AS STRING) AS issue_type,
    status,
    priority,
    CAST(NULL AS STRING) AS assignee,
    CAST(NULL AS STRING) AS task_type,
    CAST(NULL AS DOUBLE) AS msn_type,
    CAST(NULL AS STRING) AS assignor,
    CAST(NULL AS STRING) AS work_group,
    CAST(NULL AS STRING) AS department_center,
    created_time,
    updated_time,
    resolved_time,
    CAST(NULL AS TIMESTAMP) AS due_date_time,
    CAST(NULL AS TIMESTAMP) AS start_date_time
FROM jira_silver.defect_task

UNION ALL
SELECT
    'MSN_TASK',
    issue_key,
    project_key,
    project_name,
    summary,
    issue_type,
    status,
    CAST(NULL AS STRING) AS priority,
    assignee,
    task_type,
    msn_type,
    CAST(NULL AS STRING) AS assignor,
    CAST(NULL AS STRING) AS work_group,
    CAST(NULL AS STRING) AS department_center,
    created_time,
    updated_time,
    CAST(NULL AS TIMESTAMP) AS resolved_time,
    due_date_time,
    start_date_time
FROM jira_silver.msn_task

UNION ALL
SELECT
    'OPERATION',
    issue_key,
    CAST(NULL AS STRING) AS project_key,
    CAST(NULL AS STRING) AS project_name,
    summary,
    issue_type,
    status,
    priority,
    assignee,
    CAST(NULL AS STRING) AS task_type,
    CAST(NULL AS DOUBLE) AS msn_type,
    assignor,
    work_group,
    department_center,
    created_time,
    updated_time,
    CAST(NULL AS TIMESTAMP) AS resolved_time,
    due_date_time,
    start_date_time
FROM jira_silver.task_operation

UNION ALL
SELECT
    'UAT_BUG',
    issue_key,
    project_key,
    project_name,
    summary,
    issue_type,
    status,
    CAST(NULL AS STRING) AS priority,
    CAST(NULL AS STRING) AS assignee,
    CAST(NULL AS STRING) AS task_type,
    CAST(NULL AS DOUBLE) AS msn_type,
    CAST(NULL AS STRING) AS assignor,
    CAST(NULL AS STRING) AS work_group,
    CAST(NULL AS STRING) AS department_center,
    created_time,
    updated_time,
    CAST(NULL AS TIMESTAMP) AS resolved_time,
    CAST(NULL AS TIMESTAMP) AS due_date_time,
    CAST(NULL AS TIMESTAMP) AS start_date_time
FROM jira_silver.os_project_uat_bugs;


