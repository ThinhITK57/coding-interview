select *
from {{ source('bi_silver__hr', 'hr_employee_headcount') }}
