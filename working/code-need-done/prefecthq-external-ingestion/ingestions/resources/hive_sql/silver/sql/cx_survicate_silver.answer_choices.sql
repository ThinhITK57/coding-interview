WITH last_tems_cte AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY id
                   ORDER BY  id DESC
               ) rn
        FROM hive.cx_survicate_raw.dim_survicate_questions ss
    ) t
    WHERE rn = 1
)

SELECT
    q.id            AS question_id,
    q.survey_id,
    q.type          AS question_type,
    q.question,
    ac_id           AS answer_choice_id,
    ac_content      AS answer_choice_content
FROM last_tems_cte q
LEFT JOIN UNNEST(q.answer_choices)
     AS t(ac_id, ac_content)
     ON TRUE;