%livy.pyspark

target_view = "bi_silver.hr_headcount_demand"

spark.sql("REFRESH TABLE bi_silver.hr_headcount_demand_snapshot")

spark.sql("""
CREATE OR REPLACE VIEW bi_silver.hr_headcount_demand AS

WITH tmp_demand_base AS (
    SELECT
        plan_year,
        plan_type,
        snapshot_month_start,
        snapshot_year_month,

        division_n,
        unit_n_1,
        department_n_2,
        department_n_3,

        role_base,
        specialization,
        position_level,
        hierarchy_level,
        resource_type,
        service_group_new,

        is_ai_group,
        is_ai_org,
        is_domestic_business,
        is_international_business,
        is_rnd,
        is_indirect_group,
        is_business_support,

        is_filled,
        is_vacant
    FROM bi_silver.hr_headcount_demand_snapshot
),

tmp_demand_agg AS (
    SELECT
        plan_year,
        plan_type,
        snapshot_month_start,
        snapshot_year_month,

        division_n,
        unit_n_1,
        department_n_2,
        department_n_3,

        role_base,
        specialization,
        position_level,
        hierarchy_level,
        resource_type,
        service_group_new,

        is_ai_group,
        is_ai_org,
        is_domestic_business,
        is_international_business,
        is_rnd,
        is_indirect_group,
        is_business_support,

        COUNT(*) AS planned_positions,
        SUM(is_filled) AS filled_positions,
        SUM(is_vacant) AS vacant_positions
    FROM tmp_demand_base
    GROUP BY
        plan_year,
        plan_type,
        snapshot_month_start,
        snapshot_year_month,

        division_n,
        unit_n_1,
        department_n_2,
        department_n_3,

        role_base,
        specialization,
        position_level,
        hierarchy_level,
        resource_type,
        service_group_new,

        is_ai_group,
        is_ai_org,
        is_domestic_business,
        is_international_business,
        is_rnd,
        is_indirect_group,
        is_business_support
)

SELECT
    *,
    filled_positions * 1.0 / NULLIF(planned_positions, 0) AS fill_rate,
    vacant_positions * 1.0 / NULLIF(planned_positions, 0) AS vacancy_rate
FROM tmp_demand_agg
""")

print("Done:", target_view)