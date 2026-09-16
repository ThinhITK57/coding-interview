SELECT
    fr.survey_id,
    fr.response.uuid     AS response_uuid,
    ar                   AS all_response_value
FROM cx_survicate_raw.fact_survicate_responses fr
LATERAL VIEW OUTER explode(fr.all_responses) ar_tbl AS ar