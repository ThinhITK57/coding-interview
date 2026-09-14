select *
from {{ source('bi_silver__crm', 'finance_contract_collected_invoices') }}
