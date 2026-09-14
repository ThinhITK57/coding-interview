select *
from {{ source('bi_silver__crm', 'crm_business_unit_revenue_kpi_plan') }}
