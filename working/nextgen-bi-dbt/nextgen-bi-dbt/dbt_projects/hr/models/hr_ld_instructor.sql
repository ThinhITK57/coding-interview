select *
from {{ source('bi_silver__hr', 'hr_ld_instructor') }}
