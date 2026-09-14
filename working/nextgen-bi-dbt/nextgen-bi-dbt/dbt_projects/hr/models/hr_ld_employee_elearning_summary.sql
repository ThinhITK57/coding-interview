select *
from {{ source('bi_silver__hr', 'hr_ld_employee_elearning_summary') }}
