CREATE SCHEMA hive.cx_cso_raw 
with (location='s3a://vcs-raw/cx-cso-raw/');

-- hive.cx_cso_raw.cx_company definition

CREATE TABLE hive.cx_cso_raw.cx_company (
   id varchar,
   name varchar,
   company_alias varchar,
   tax_code varchar,
   customer_segment_l1 varchar,
   customer_segment_l2 varchar,
   customer_segment_l3 varchar,
   crawled_at_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.cx_cso_raw.cx_dim_journey definition

CREATE TABLE hive.cx_cso_raw.cx_dim_journey (
   id varchar,
   journey_name varchar,
   crawled_at_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.cx_cso_raw.cx_product_category definition

CREATE TABLE hive.cx_cso_raw.cx_product_category (
   issue_category varchar,
   product_category_code varchar,
   product_category varchar,
   nan varchar,
   crawled_at_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.cx_cso_raw.dim_cso_admin_group definition

CREATE TABLE hive.cx_cso_raw.dim_cso_admin_group (
   id bigint,
   name varchar,
   description varchar,
   escalate_to varchar,
   unassigned_for varchar,
   agent_ids array(bigint),
   created_at varchar,
   updated_at varchar,
   allow_agents_to_change_availability boolean,
   agent_availability_status boolean,
   business_calendar_id bigint,
   type varchar,
   automatic_agent_assignment ROW(enabled boolean, type varchar, settings array(ROW(channel varchar, assignment_type varchar))),
   created_at_ts bigint,
   updated_at_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.cx_cso_raw.dim_cso_agents definition

CREATE TABLE hive.cx_cso_raw.dim_cso_agents (
   available boolean,
   occasional boolean,
   id bigint,
   ticket_scope bigint,
   created_at varchar,
   updated_at varchar,
   last_active_at varchar,
   available_since varchar,
   type varchar,
   contact ROW(active boolean, email varchar, job_title varchar, language varchar, last_login_at varchar, mobile varchar, name varchar, phone varchar, time_zone varchar, created_at varchar, updated_at varchar, avatar ROW(id bigint, name varchar, content_type varchar, size bigint, created_at varchar, updated_at varchar, attachment_url varchar, thumb_url varchar)),
   deactivated boolean,
   signature varchar,
   agent_level_id bigint,
   focus_mode boolean,
   agent_operational_status varchar,
   created_at_ts bigint,
   updated_at_ts bigint,
   last_active_at_ts bigint,
   available_since_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.cx_cso_raw.dim_cso_companies definition

CREATE TABLE hive.cx_cso_raw.dim_cso_companies (
   id bigint,
   name varchar,
   description varchar,
   note varchar,
   domains array(varchar),
   created_at varchar,
   updated_at varchar,
   custom_fields ROW(alias varchar, tax_code varchar, company_segment varchar, company_type varchar),
   health_score varchar,
   account_tier varchar,
   renewal_date varchar,
   industry varchar,
   org_company_id varchar,
   created_at_ts bigint,
   updated_at_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.cx_cso_raw.dim_cso_company_fields definition

CREATE TABLE hive.cx_cso_raw.dim_cso_company_fields (
   id bigint,
   name varchar,
   label varchar,
   position bigint,
   required_for_agents boolean,
   type varchar,
   default boolean,
   created_at varchar,
   updated_at varchar,
   created_at_ts bigint,
   updated_at_ts bigint,
   choices array(varchar)
)
WITH (
   format = 'PARQUET'
);


-- hive.cx_cso_raw.dim_cso_contact_fields definition

CREATE TABLE hive.cx_cso_raw.dim_cso_contact_fields (
   editable_in_signup boolean,
   id bigint,
   name varchar,
   label varchar,
   position bigint,
   required_for_agents boolean,
   type varchar,
   default boolean,
   customers_can_edit boolean,
   label_for_customers varchar,
   required_for_customers boolean,
   displayed_for_customers boolean,
   created_at varchar,
   updated_at varchar,
   created_at_ts bigint,
   updated_at_ts bigint,
   choices varchar
)
WITH (
   format = 'PARQUET'
);


-- hive.cx_cso_raw.dim_cso_contacts definition

CREATE TABLE hive.cx_cso_raw.dim_cso_contacts (
   active boolean,
   address varchar,
   description varchar,
   email varchar,
   id bigint,
   job_title varchar,
   language varchar,
   mobile varchar,
   name varchar,
   phone varchar,
   time_zone varchar,
   twitter_id varchar,
   custom_fields ROW(language_mail varchar, gender varchar, contact_segment varchar),
   facebook_id varchar,
   created_at varchar,
   updated_at varchar,
   csat_rating varchar,
   preferred_source varchar,
   company_id bigint,
   company ROW(id bigint, view_all_tickets boolean, name varchar, avatar varchar, org_company_id varchar),
   other_companies array(varchar),
   unique_external_id varchar,
   first_name varchar,
   last_name varchar,
   visitor_id varchar,
   org_contact_id bigint,
   org_contact_id_str varchar,
   other_phone_numbers array(ROW(label varchar, value varchar)),
   created_at_ts bigint,
   updated_at_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.cx_cso_raw.dim_cso_email_configs definition

CREATE TABLE hive.cx_cso_raw.dim_cso_email_configs (
   id bigint,
   name varchar,
   product_id bigint,
   to_email varchar,
   reply_email varchar,
   group_id varchar,
   primary_role boolean,
   active boolean,
   created_at varchar,
   updated_at varchar,
   created_at_ts bigint,
   updated_at_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.cx_cso_raw.dim_cso_groups definition

CREATE TABLE hive.cx_cso_raw.dim_cso_groups (
   id bigint,
   name varchar,
   description varchar,
   escalate_to varchar,
   unassigned_for varchar,
   business_hour_id bigint,
   group_type varchar,
   created_at varchar,
   updated_at varchar,
   auto_ticket_assign bigint,
   agent_availability_status boolean,
   created_at_ts bigint,
   updated_at_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.cx_cso_raw.dim_cso_roles definition

CREATE TABLE hive.cx_cso_raw.dim_cso_roles (
   id bigint,
   name varchar,
   description varchar,
   default boolean,
   created_at varchar,
   updated_at varchar,
   agent_type bigint,
   created_at_ts bigint,
   updated_at_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.cx_cso_raw.dim_cso_sla_policies definition

CREATE TABLE hive.cx_cso_raw.dim_cso_sla_policies (
   id bigint,
   name varchar,
   description varchar,
   active boolean,
   sla_target ROW(priority_4 ROW(respond_within bigint, resolve_within bigint, next_respond_within bigint, business_hours boolean, escalation_enabled boolean), priority_3 ROW(respond_within bigint, resolve_within bigint, next_respond_within bigint, business_hours boolean, escalation_enabled boolean), priority_2 ROW(respond_within bigint, resolve_within bigint, next_respond_within bigint, business_hours boolean, escalation_enabled boolean), priority_1 ROW(respond_within bigint, resolve_within bigint, next_respond_within bigint, business_hours boolean, escalation_enabled boolean)),
   applicable_to ROW(ticket_types array(varchar), _dummy_ bigint),
   is_default boolean,
   position bigint,
   created_at varchar,
   updated_at varchar,
   escalation ROW(reminder_response ROW(_dummy_ bigint), reminder_resolution ROW(_dummy_ bigint), response ROW(_dummy_ bigint), resolution ROW(_dummy_ bigint), reminder_next_response ROW(_dummy_ bigint), next_response ROW(_dummy_ bigint)),
   created_at_ts bigint,
   updated_at_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.cx_cso_raw.dim_cso_ticket_fields definition

CREATE TABLE hive.cx_cso_raw.dim_cso_ticket_fields (
   id bigint,
   name varchar,
   label varchar,
   description varchar,
   position bigint,
   required_for_closure boolean,
   required_for_agents boolean,
   type varchar,
   default boolean,
   customers_can_edit boolean,
   customers_can_filter boolean,
   label_for_customers varchar,
   required_for_customers boolean,
   displayed_to_customers boolean,
   created_at varchar,
   updated_at varchar,
   portal_cc boolean,
   portal_cc_to varchar,
   created_at_ts bigint,
   updated_at_ts bigint,
   choices varchar,
   nested_ticket_fields array(ROW(id bigint, name varchar, description varchar, label varchar, label_in_portal varchar, level bigint, ticket_field_id bigint, created_at varchar, updated_at varchar))
)
WITH (
   format = 'PARQUET'
);


-- hive.cx_cso_raw.dim_cso_time_entries definition

CREATE TABLE hive.cx_cso_raw.dim_cso_time_entries (
   billable boolean,
   note varchar,
   id bigint,
   timer_running boolean,
   agent_id bigint,
   ticket_id bigint,
   company_id bigint,
   time_spent varchar,
   executed_at varchar,
   start_time varchar,
   created_at varchar,
   updated_at varchar,
   time_spent_in_seconds bigint,
   executed_at_ts bigint,
   start_time_ts bigint,
   created_at_ts bigint,
   updated_at_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.cx_cso_raw.dim_question_customer_journey definition

CREATE TABLE hive.cx_cso_raw.dim_question_customer_journey (
   survey_id varchar,
   survey_name varchar,
   question_id varchar,
   question_type varchar,
   question_text varchar,
   answer_choice_id varchar,
   answer_choice_content varchar,
   question_type__h varchar,
   active varchar,
   kpi varchar,
   product_category varchar,
   crawled_at_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.cx_cso_raw.dim_surveys_customer_journey definition

CREATE TABLE hive.cx_cso_raw.dim_surveys_customer_journey (
   survey_id varchar,
   journey varchar,
   customer_touchpoint varchar,
   product_category varchar,
   note varchar,
   created_ticket_vs_negative varchar,
   other varchar,
   crawled_at_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.cx_cso_raw.fact_cso_tickets definition

CREATE TABLE hive.cx_cso_raw.fact_cso_tickets (
   associated_tickets_count bigint,
   associated_tickets_list array(bigint),
   association_type bigint,
   attachments array(ROW(attachment_url varchar, content_type varchar, created_at varchar, id bigint, name varchar, size bigint, updated_at varchar)),
   cc_emails array(varchar),
   company ROW(_dummy_ bigint, id bigint, name varchar),
   company_id bigint,
   created_at varchar,
   created_at_ts bigint,
   custom_fields ROW(cf__call_reminder varchar, cf__components varchar, cf__isvcs boolean, cf__l1_time_actual varchar, cf__l1_time_allowed varchar, cf__l1_violated varchar, cf__l2_time_actual varchar, cf__l2_time_allowed varchar, cf__l2_violated varchar, cf__l3_time_actual varchar, cf__l3_time_allowed varchar, cf__l3_violated varchar, cf__l4_time_actual varchar, cf__l4_time_allowed varchar, cf__l4_violated varchar, cf__merge varchar, cf__segment varchar, cf__ticket_cx_report boolean, cf_agent_l1 varchar, cf_ai_sentiment varchar, cf_chng_trnh_hng_ng varchar, cf_company varchar, cf_component varchar, cf_csat_rating bigint, cf_customer_entry_channel varchar, cf_customer_respond_date varchar, cf_exception varchar, cf_hide_field varchar, cf_hiu_qu_trao_i varchar, cf_imported_ticket_date varchar, cf_imported_ticket_due_date varchar, cf_imported_ticket_id varchar, cf_imported_ticket_link varchar, cf_incident_root_cause varchar, cf_issues_type varchar, cf_l1 boolean, cf_l2 boolean, cf_l3 boolean, cf_l4 boolean, cf_legacy_id varchar, cf_link_kb varchar, cf_nh_gi_mc_x_l varchar, cf_note varchar, cf_number_of_due_date_changes bigint, cf_old_due_date varchar, cf_onedaybeforedue boolean, cf_onehourbeforedue boolean, cf_product_category varchar, cf_reason varchar, cf_related_ticket varchar, cf_reminder_update boolean, cf_rt_notification boolean, cf_rt_overdue varchar, cf_rt_time varchar, cf_sentiment varchar, cf_severity varchar, cf_spam_type varchar, cf_subtype varchar, cf_thirdfourtime boolean, cf_timer_history varchar, cf_ttr_overdue varchar, cf_ttr_time varchar, cf_twothirdtime boolean, cf_urgency varchar),
   deleted boolean,
   description varchar,
   description_text varchar,
   due_by varchar,
   due_by_ts bigint,
   email_config_id bigint,
   form_id bigint,
   fr_due_by varchar,
   fr_due_by_ts bigint,
   fr_escalated boolean,
   fwd_emails array(varchar),
   group_id bigint,
   id bigint,
   is_escalated boolean,
   nr_due_by varchar,
   nr_due_by_ts bigint,
   nr_escalated boolean,
   priority bigint,
   product_id bigint,
   reply_cc_emails array(varchar),
   requester ROW(email varchar, id bigint, mobile varchar, name varchar, phone varchar),
   requester_id bigint,
   responder_id bigint,
   source bigint,
   spam boolean,
   stats ROW(agent_responded_at varchar, closed_at varchar, first_responded_at varchar, pending_since varchar, reopened_at varchar, requester_responded_at varchar, resolved_at varchar, status_updated_at varchar),
   status bigint,
   subject varchar,
   support_email varchar,
   tags array(varchar),
   ticket_bcc_emails array(varchar),
   ticket_cc_emails array(varchar),
   to_emails array(varchar),
   type varchar,
   updated_at varchar,
   updated_at_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.cx_cso_raw.fact_cso_tickets_sla definition

CREATE TABLE hive.cx_cso_raw.fact_cso_tickets_sla (
   ticket_id bigint,
   agent_name varchar,
   status varchar,
   priority varchar,
   resolution_time_in_business_hours double,
   agent_reply_count bigint,
   first_response_date varchar,
   ticket_type varchar,
   tickets_first_responded_within_sla varchar,
   tickets_resolved_within_sla varchar,
   ttr_time varchar,
   crawled_at_ts bigint
)
WITH (
   external_location = 's3a://vcs-raw/cx-cso-raw/fact_cso_tickets_sla',
   format = 'PARQUET'
);