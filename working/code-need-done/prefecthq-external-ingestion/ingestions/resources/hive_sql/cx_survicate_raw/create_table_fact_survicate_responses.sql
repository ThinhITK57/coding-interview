CREATE TABLE IF NOT EXISTS cx_survicate_raw.raw_fact_survicate_responses (
          survey_id STRING,
  response STRUCT<uuid:STRING, url:STRING, device_type:STRING, operating_system:STRING, language:STRING, answers:ARRAY<STRUCT<question_id:BIGINT, question_type:STRING, action_performed:BOOLEAN, disclaimer_accepted:STRING, answers:ARRAY<STRUCT<id:BIGINT, content:STRING, score:STRING, comment:STRING, translated_comment:STRING, disclaimer_accepted:BOOLEAN, insight:STRING, rank:BIGINT>>, answer:STRING, translated_answer:STRING, insight:STRING, ai_followups:STRING, translated_ai_followups:STRING, fields:ARRAY<STRUCT<type:STRING, content:STRING>>, comment:STRING, translated_comment:STRING>>, collected_at:STRING, respondent:STRUCT<uuid:STRING, attributes:ARRAY<STRING>>, platform:STRING, cso_ticket_id:STRING, cso_uid:STRING, cso_email:STRING>,
  attributes STRUCT<_dummy_:BIGINT>,
  all_responses ARRAY<STRING>,
  crawled_at_ts BIGINT
        )
        USING PARQUET
        LOCATION 's3a://vcs-raw/cx-survicate-raw/fact_survicate_responses'