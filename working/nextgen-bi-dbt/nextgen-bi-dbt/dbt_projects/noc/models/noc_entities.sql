select *
from {{ source('bi_silver__noc', 'noc_entities') }}
