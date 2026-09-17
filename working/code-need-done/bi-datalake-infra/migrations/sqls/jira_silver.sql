CREATE SCHEMA hive.jira_silver 
with (location='s3a://vcs-silver/jira-silver/');

-- hive.jira_silver.bug_prod definition

CREATE TABLE hive.jira_silver.bug_prod (
   issue_key varchar,
   project_key varchar,
   project_name varchar,
   summary varchar,
   issue_type varchar,
   status varchar,
   assignee varchar,
   priority varchar,
   created_time timestamp(3),
   updated_time timestamp(3),
   resolved_time timestamp(3),
   due_date_time timestamp(3)
)
WITH (
   format = 'PARQUET'
);


-- hive.jira_silver.defect_task definition

CREATE TABLE hive.jira_silver.defect_task (
   issue_key varchar,
   project_key varchar,
   project_name varchar,
   summary varchar,
   status varchar,
   priority varchar,
   created_time timestamp(3),
   updated_time timestamp(3),
   resolved_time timestamp(3)
)
WITH (
   format = 'PARQUET'
);


-- hive.jira_silver.estimated_effort definition

CREATE TABLE hive.jira_silver.estimated_effort (
   issue_key varchar,
   project_key varchar,
   project_name varchar,
   summary varchar,
   issue_type varchar,
   status varchar,
   assignee varchar,
   ttsx_product varchar,
   total_net_effort double,
   resolved_time timestamp(3),
   updated_time timestamp(3)
)
WITH (
   format = 'PARQUET'
);


-- hive.jira_silver.jira_issue_effort_fct_vw definition

CREATE TABLE hive.jira_silver.jira_issue_effort_fct_vw (
   issue_key varchar,
   project_key varchar,
   status varchar,
   display_name varchar,
   total_net_effort double,
   resolved_time timestamp(3)
)
WITH (
   format = 'PARQUET'
);


-- hive.jira_silver.jira_issue_full_fct_vw definition

CREATE TABLE hive.jira_silver.jira_issue_full_fct_vw (
   issue_key varchar,
   project_key varchar,
   project_name varchar,
   issue_type varchar,
   status varchar,
   priority varchar,
   assignee varchar,
   assignee_name varchar,
   created_time timestamp(3),
   updated_time timestamp(3),
   resolved_time timestamp(3),
   total_net_effort double,
   quarterly_kpi_result double,
   monthly_accumulated_result double,
   execution_result double,
   target varchar,
   unit_of_measure double,
   kpi_reporting_date timestamp(3)
)
WITH (
   format = 'PARQUET'
);


-- hive.jira_silver.jira_issue_task_fct_vw definition

CREATE TABLE hive.jira_silver.jira_issue_task_fct_vw (
   source_type varchar,
   issue_key varchar,
   project_key varchar,
   project_name varchar,
   summary varchar,
   issue_type varchar,
   status varchar,
   priority varchar,
   assignee varchar,
   task_type varchar,
   msn_type double,
   assignor varchar,
   work_group varchar,
   department_center varchar,
   created_time timestamp(3),
   updated_time timestamp(3),
   resolved_time timestamp(3),
   due_date_time timestamp(3),
   start_date_time timestamp(3)
)
WITH (
   format = 'PARQUET'
);


-- hive.jira_silver.jira_kpi_fct_vw definition

CREATE TABLE hive.jira_silver.jira_kpi_fct_vw (
   kpi_type varchar,
   issue_key varchar,
   summary varchar,
   issue_type varchar,
   status varchar,
   product_name varchar,
   reporting_date timestamp(3),
   kpi_name varchar,
   kpi_value double,
   target_value double,
   unit_of_measure double,
   updated_time timestamp(3)
)
WITH (
   format = 'PARQUET'
);


-- hive.jira_silver.jira_user definition

CREATE TABLE hive.jira_silver.jira_user (
   user_id bigint,
   user_key varchar,
   username varchar,
   display_name varchar,
   email varchar,
   is_active boolean,
   directory_id bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.jira_silver.jira_worklog_fct_vw definition

CREATE TABLE hive.jira_silver.jira_worklog_fct_vw (
   issue_key varchar,
   project_key varchar,
   assignee varchar,
   author varchar,
   timespent_hours double,
   work_start_time timestamp(3)
)
WITH (
   format = 'PARQUET'
);


-- hive.jira_silver.kpi_binh_staff definition

CREATE TABLE hive.jira_silver.kpi_binh_staff (
   issue_key varchar,
   summary varchar,
   issue_type varchar,
   status varchar,
   description varchar,
   created_time timestamp(3),
   updated_time timestamp(3),
   reporting_date_time timestamp(3),
   update_start_date_time timestamp(3),
   update_date_time timestamp(3),
   agent_online double,
   agent_latest double,
   latency_delay double,
   has_soc247 boolean,
   is_exception boolean,
   exception_reason varchar,
   latest_version varchar,
   version varchar,
   contract_type varchar,
   product_type varchar,
   product_name varchar,
   update_frequency varchar,
   created_ts bigint,
   updated_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.jira_silver.kpi_kqi definition

CREATE TABLE hive.jira_silver.kpi_kqi (
   issue_key varchar,
   summary varchar,
   service_product varchar,
   condition varchar,
   target varchar,
   quarterly_kpi_result double,
   monthly_accumulated_result double,
   execution_result double,
   unit_of_measure double,
   reporting_date_time timestamp(3),
   updated_time timestamp(3)
)
WITH (
   format = 'PARQUET'
);


-- hive.jira_silver.msn_task definition

CREATE TABLE hive.jira_silver.msn_task (
   issue_key varchar,
   project_key varchar,
   project_name varchar,
   summary varchar,
   issue_type varchar,
   status varchar,
   assignee varchar,
   task_type varchar,
   msn_type double,
   created_time timestamp(3),
   updated_time timestamp(3),
   due_date_time timestamp(3),
   start_date_time timestamp(3)
)
WITH (
   format = 'PARQUET'
);


-- hive.jira_silver.os_project_uat_bugs definition

CREATE TABLE hive.jira_silver.os_project_uat_bugs (
   issue_key varchar,
   project_key varchar,
   project_name varchar,
   summary varchar,
   issue_type varchar,
   status varchar,
   created_time timestamp(3),
   updated_time timestamp(3)
)
WITH (
   format = 'PARQUET'
);


-- hive.jira_silver.process_compliance definition

CREATE TABLE hive.jira_silver.process_compliance (
   issue_key varchar,
   summary varchar,
   issue_type varchar,
   ttqt_product_name varchar,
   process_compliance_rate double,
   due_date_time timestamp(3),
   updated_time timestamp(3)
)
WITH (
   format = 'PARQUET'
);


-- hive.jira_silver.task_operation definition

CREATE TABLE hive.jira_silver.task_operation (
   issue_key varchar,
   summary varchar,
   issue_type varchar,
   status varchar,
   priority varchar,
   assignee varchar,
   assignor varchar,
   work_group varchar,
   department_center varchar,
   created_time timestamp(3),
   updated_time timestamp(3),
   start_date_time timestamp(3),
   due_date_time timestamp(3)
)
WITH (
   format = 'PARQUET'
);


-- hive.jira_silver.worklog definition

CREATE TABLE hive.jira_silver.worklog (
   worklog_id bigint,
   issue_id bigint,
   issue_key varchar,
   project_key varchar,
   summary varchar,
   author varchar,
   comment varchar,
   timespent_hours double,
   started_time timestamp(3),
   created_time timestamp(3),
   updated_time timestamp(3)
)
WITH (
   format = 'PARQUET'
);