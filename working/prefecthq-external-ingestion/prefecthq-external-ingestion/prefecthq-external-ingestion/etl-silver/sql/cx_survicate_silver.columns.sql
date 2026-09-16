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
    col_value       AS column_value
FROM last_tems_cte q
LEFT JOIN UNNEST(q.columns)
    AS c(col_value)
    ON TRUE;
