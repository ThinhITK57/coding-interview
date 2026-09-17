CREATE SCHEMA hive.cx_survicate_raw 
with (location='s3a://vcs-raw/cx-survicate-raw/');

-- hive.cx_survicate_raw.dim_survicate_questions definition

CREATE TABLE hive.cx_survicate_raw.dim_survicate_questions (
   id bigint,
   type varchar,
   question varchar,
   introduction varchar,
   survey_id varchar,
   answer_choices array(ROW(id bigint, content varchar)),
   columns array(varchar),
   fields array(ROW(type varchar, label varchar))
)
WITH (
   format = 'PARQUET'
);


-- hive.cx_survicate_raw.dim_survicate_survey_details definition

CREATE TABLE hive.cx_survicate_raw.dim_survicate_survey_details (
   id varchar,
   type varchar,
   name varchar,
   created_at varchar,
   enabled boolean,
   responses bigint,
   launch ROW(start_at varchar, end_at varchar, responses_limit varchar),
   author ROW(name varchar, email varchar),
   folder varchar,
   first_response_at varchar,
   last_response_at varchar,
   attributes array(varchar),
   survey_id varchar,
   created_at_ts bigint,
   first_response_at_ts bigint,
   last_response_at_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.cx_survicate_raw.dim_survicate_surveys definition

CREATE TABLE hive.cx_survicate_raw.dim_survicate_surveys (
   id varchar,
   type varchar,
   name varchar,
   created_at varchar,
   enabled boolean,
   responses bigint,
   launch ROW(start_at varchar, end_at varchar, responses_limit varchar),
   created_at_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.cx_survicate_raw.fact_survicate_personal_data definition

CREATE TABLE hive.cx_survicate_raw.fact_survicate_personal_data (
   responses bigint,
   respondents bigint,
   insights_hub ROW(authors bigint, author_attributes bigint, notes bigint, notes_attributes bigint, total bigint),
   email varchar
)
WITH (
   format = 'PARQUET'
);


-- hive.cx_survicate_raw.fact_survicate_responses definition

CREATE TABLE hive.cx_survicate_raw.fact_survicate_responses (
   survey_id varchar,
   response ROW(uuid varchar, url varchar, device_type varchar, operating_system varchar, language varchar, answers array(ROW(question_id bigint, question_type varchar, action_performed boolean, disclaimer_accepted varchar, answers array(ROW(id bigint, content varchar, score varchar, comment varchar, translated_comment varchar, disclaimer_accepted boolean, insight varchar, rank bigint)), answer varchar, translated_answer varchar, insight varchar, ai_followups varchar, translated_ai_followups varchar, fields array(ROW(type varchar, content varchar)), comment varchar, translated_comment varchar)), collected_at varchar, respondent ROW(uuid varchar, attributes array(varchar)), platform varchar, cso_ticket_id varchar, cso_uid varchar, cso_email varchar),
   attributes ROW(_dummy_ bigint),
   all_responses array(varchar)
)
WITH (
   format = 'PARQUET'
);


-- hive.cx_survicate_raw.survey_question_master definition

CREATE TABLE hive.cx_survicate_raw.survey_question_master (
   survey_id varchar,
   survey_name varchar,
   question_id varchar,
   question_type varchar,
   question_text varchar,
   answer_choice_id varchar,
   answer_choice_content varchar,
   active varchar,
   kpi varchar,
   product_service varchar,
   layer varchar,
   kpi_checked varchar,
   diem_cham_survey varchar,
   source_file varchar,
   crawled_at_ts varchar,
   journey varchar
)
WITH (
   csv_escape = '\',
   csv_quote = '"',
   csv_separator = ',',
   external_location = 's3a://vcs-raw/cx-survicate-raw/survey_question_master',
   format = 'CSV',
   skip_header_line_count = 1
);


-- hive.cx_survicate_raw.excel_surveys definition

CREATE TABLE hive.cx_survicate_raw.excel_surveys (
   survey_id varchar,
   survey_name varchar,
   created_at varchar,
   folder varchar,
   question_count varchar,
   response_count varchar,
   survey_active varchar,
   journey_id varchar,
   journey varchar,
   touchpoint varchar,
   product_service varchar,
   note varchar,
   allow_negative_feedback_ticket varchar,
   source_file varchar,
   crawled_at_ts varchar,
   created_at_ts varchar
)
WITH (
   csv_escape = '\',
   csv_quote = '"',
   csv_separator = ',',
   external_location = 's3a://vcs-raw/cx-survicate-raw/excel_surveys',
   format = 'CSV',
   skip_header_line_count = 1
);