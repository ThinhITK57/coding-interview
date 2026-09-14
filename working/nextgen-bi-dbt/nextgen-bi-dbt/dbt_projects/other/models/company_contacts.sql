select *
from {{ source('bi_silver__other', 'company_contacts') }}
