select *
from {{ source('pbi_silver__production_management_1', 'production_management_1_post_acceptance_bug_projects') }}
