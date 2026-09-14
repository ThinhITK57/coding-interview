select *
from {{ source('pbi_silver__production_management_1', 'production_management_1_milestone_versions_projects') }}
