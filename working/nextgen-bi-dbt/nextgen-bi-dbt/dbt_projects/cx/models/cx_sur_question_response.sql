select *
from {{ source('bi_silver__cx', 'cx_sur_question_response') }}
