select *
from {{ source('bi_silver__other', 'giang_test_masterplan') }}
