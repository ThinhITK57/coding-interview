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
    f_type          AS field_type,
    f_label         AS field_label
FROM last_tems_cte q
LEFT JOIN UNNEST(q.fields)
    AS f(f_type, f_label)
    ON TRUE;