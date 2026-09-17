%livy.pyspark
spark.sql("""
CREATE OR REPLACE VIEW bi_gold.cx_sur_question_response AS
select
	collected_date,
 
    UPPER(product_category) as product_category,
    UPPER(question_type) as question_type,
    UPPER(answer_string) as answer_string,
    UPPER(answer_tag) as answer_tag,
    item_score_value,
    UPPER(item_score_label) as item_score_label,
    UPPER(kpi_type) as kpi_type,
    UPPER(journey) as journey,
    UPPER(customer_touchpoint) as customer_touchpoint,
    UPPER(kpi_group) as kpi_group,
    UPPER(choice_content) as choice_content,
    UPPER(question_clean) as question_clean,
    
    survey_id,
    response_id,
    respondent_uuid,
    question_id,
    choice_id,
    answer_number
    
    
FROM bi_silver.cx_sur_question_response
""")

# -- Truy vấn kiểm tra kết quả
print("DONE")
