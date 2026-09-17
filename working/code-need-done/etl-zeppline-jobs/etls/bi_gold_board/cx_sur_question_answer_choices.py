%livy.pyspark
spark.sql("""
CREATE OR REPLACE VIEW bi_gold.cx_sur_question_answer_choices AS
select
    UPPER(product_category) as product_category,
    UPPER(question_type) as question_type,
    UPPER(journey) as journey,
    UPPER(kpi_type) as kpi_type,
    UPPER(customer_touchpoint) as customer_touchpoint,
    UPPER(question_clean) as question_clean,
    
    survey_id,
    question_id,
    answer_choice_id
    
FROM bi_silver.cx_sur_question_answer_choices
""")

# -- Truy vấn kiểm tra kết quả
print("DONE")
