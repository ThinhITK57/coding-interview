select *
from {{ source('bi_silver__other', 'contract_collected_invoices') }}
