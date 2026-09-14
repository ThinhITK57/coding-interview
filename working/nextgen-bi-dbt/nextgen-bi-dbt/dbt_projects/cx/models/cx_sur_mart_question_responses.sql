select *
from {{ source('bi_silver__cx', 'cx_sur_mart_question_responses') }}
