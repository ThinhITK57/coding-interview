%livy.pyspark

target_view = "bi_silver.hr_labor_map"

spark.sql("REFRESH TABLE bi_silver.hr_headcount_demand_snapshot")

spark.sql("""
CREATE OR REPLACE VIEW bi_silver.hr_labor_map AS

WITH tmp_labor_base AS (
    SELECT DISTINCT
        employee_type,
        resource_type,
        service_group_new,

        is_ai_group,
        is_ai_org,
        is_domestic_business,
        is_international_business,
        is_rnd,
        is_indirect_group,
        is_business_support
    FROM bi_silver.hr_headcount_demand_snapshot
),

tmp_labor_classified AS (
    SELECT
        MD5(CONCAT_WS('|',
            COALESCE(employee_type, ''),
            COALESCE(resource_type, ''),
            COALESCE(service_group_new, ''),
            CAST(is_ai_group AS STRING),
            CAST(is_ai_org AS STRING),
            CAST(is_domestic_business AS STRING),
            CAST(is_international_business AS STRING),
            CAST(is_rnd AS STRING),
            CAST(is_indirect_group AS STRING),
            CAST(is_business_support AS STRING)
        )) AS labor_map_key,

        employee_type,
        resource_type,
        service_group_new,

        is_ai_group,
        is_ai_org,
        is_domestic_business,
        is_international_business,
        is_rnd,
        is_indirect_group,
        is_business_support,

        CASE
            WHEN is_indirect_group = 1 THEN 'Indirect'
            WHEN is_business_support = 1 THEN 'Business Support'
            WHEN is_rnd = 1 THEN 'R&D'
            ELSE 'Direct'
        END AS labor_group,

        CASE
            WHEN resource_type = 'TDS-NDS-HDDV' THEN 'Core Workforce'
            WHEN resource_type = 'CTV' THEN 'Collaborator'
            WHEN resource_type = 'SV+Fresher' THEN 'Trainee/Future Talent'
            WHEN resource_type = 'IS/OS' THEN 'Outsourcing'
            ELSE 'Unknown'
        END AS labor_type,

        CASE
            WHEN is_domestic_business = 1 THEN 'Domestic'
            WHEN is_international_business = 1 THEN 'International'
            ELSE 'Unknown'
        END AS market_group
    FROM tmp_labor_base
)

SELECT *
FROM tmp_labor_classified
""")

print("Done:", target_view)