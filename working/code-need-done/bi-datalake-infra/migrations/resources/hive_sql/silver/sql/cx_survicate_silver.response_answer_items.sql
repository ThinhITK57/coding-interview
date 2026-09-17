SELECT
    fr.survey_id,

    fr.response.uuid            AS response_uuid,
    a.question_id,
    a.question_type,

    ai.id                       AS answer_id,
    ai.content                  AS answer_content,
    ai.score                    AS answer_score,
    ai.rank                     AS answer_rank,
    ai.comment                  AS answer_comment,
    ai.translated_comment       AS translated_answer_comment,
    ai.disclaimer_accepted,
    ai.insight
FROM cx_survicate_raw.fact_survicate_responses fr
LATERAL VIEW explode(fr.response.answers) a_tbl AS a
LATERAL VIEW explode(a.answers) ai_tbl AS ai