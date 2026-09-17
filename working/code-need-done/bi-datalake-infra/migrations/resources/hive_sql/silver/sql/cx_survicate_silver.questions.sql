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
    id,
    type,
    question,
    introduction,
    survey_id
--    answer_choices,
--    columns,
--    fields
FROM last_tems_cte;