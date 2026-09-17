# %livy.pyspark

spark.sql("""
CREATE OR REPLACE VIEW bi_silver.hr_workforce_monthly_ytd
SECURITY DEFINER AS

WITH base AS (
    SELECT *
    FROM bi_silver.hr_workforce_monthly
),

ytd AS (
    SELECT
        b.*,

        FIRST_VALUE(opening_headcount) OVER w AS opening_headcount_ytd,

        closing_headcount AS closing_headcount_ytd,

        AVG(avg_headcount) OVER w AS avg_headcount_ytd,

        SUM(new_hire_count) OVER w AS new_hire_count_ytd,
        SUM(resigned_count) OVER w AS resigned_count_ytd,
        SUM(voluntary_exit_count) OVER w AS voluntary_exit_count_ytd,
        SUM(company_termination_count) OVER w AS company_termination_count_ytd,
        SUM(quick_quit_count) OVER w AS quick_quit_count_ytd,
        SUM(promotion_count) OVER w AS promotion_count_ytd,
        SUM(position_level_change_count) OVER w AS position_level_change_count_ytd,
        SUM(competency_zone_change_count) OVER w AS competency_zone_change_count_ytd,
        SUM(internal_transfer_in) OVER w AS internal_transfer_in_ytd,
        SUM(internal_transfer_out) OVER w AS internal_transfer_out_ytd,
        SUM(net_change) OVER w AS net_change_ytd,

        SUM(performance_employee_count) OVER w AS performance_employee_count_ytd,
        SUM(performance_not_pass_count) OVER w AS performance_not_pass_count_ytd,
        SUM(performance_pass_count) OVER w AS performance_pass_count_ytd,
        SUM(performance_exceed_count) OVER w AS performance_exceed_count_ytd,

        ROW_NUMBER() OVER (
            PARTITION BY report_year
            ORDER BY snapshot_year_month DESC
        ) AS ytd_month_rank_desc,

        CASE
            WHEN snapshot_year_month = MAX(snapshot_year_month) OVER (
                PARTITION BY report_year
            )
            THEN true
            ELSE false
        END AS is_latest_ytd_month

    FROM base b

    WINDOW w AS (
        PARTITION BY
            report_year,
            division_n,
            department_n_1,
            department_n_2,
            department_n_3,
            contract_type,
            resource_type,
            job_level,
            position_level,
            hierarchy_level,
            competency_zone,
            tenure_group,
            gender
        ORDER BY snapshot_year_month ASC
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )
)

SELECT
    ytd.*,

    internal_transfer_in_ytd - internal_transfer_out_ytd
        AS internal_transfer_net_ytd,

    closing_headcount_ytd - performance_employee_count_ytd
        AS performance_not_evaluated_count_ytd,

    CASE WHEN resource_type = 'TDS-NDS-HDDV'
        THEN opening_headcount_ytd ELSE 0 END
        AS official_opening_headcount_ytd,

    CASE WHEN resource_type = 'TDS-NDS-HDDV'
        THEN closing_headcount_ytd ELSE 0 END
        AS official_headcount_ytd,

    CASE WHEN resource_type = 'TDS-NDS-HDDV'
        THEN avg_headcount_ytd ELSE 0 END
        AS official_avg_headcount_ytd,

    CASE WHEN resource_type = 'TDS-NDS-HDDV'
        THEN new_hire_count_ytd ELSE 0 END
        AS official_new_hire_count_ytd,

    CASE WHEN resource_type = 'TDS-NDS-HDDV'
        THEN resigned_count_ytd ELSE 0 END
        AS official_resigned_count_ytd,

    CASE WHEN resource_type = 'TDS-NDS-HDDV'
        THEN voluntary_exit_count_ytd ELSE 0 END
        AS official_voluntary_exit_count_ytd,

    CASE WHEN resource_type = 'TDS-NDS-HDDV'
        THEN company_termination_count_ytd ELSE 0 END
        AS official_company_termination_count_ytd,

    CASE WHEN resource_type = 'TDS-NDS-HDDV'
        THEN quick_quit_count_ytd ELSE 0 END
        AS official_quick_quit_count_ytd,

    CASE WHEN resource_type = 'TDS-NDS-HDDV'
        THEN promotion_count_ytd ELSE 0 END
        AS official_promotion_count_ytd,

    CASE WHEN resource_type = 'TDS-NDS-HDDV'
        THEN performance_employee_count_ytd ELSE 0 END
        AS official_performance_employee_count_ytd,

    CASE WHEN resource_type = 'TDS-NDS-HDDV'
        THEN closing_headcount_ytd - performance_employee_count_ytd ELSE 0 END
        AS official_performance_not_evaluated_count_ytd,

    CASE WHEN resource_type = 'TDS-NDS-HDDV'
        THEN performance_not_pass_count_ytd ELSE 0 END
        AS official_performance_not_pass_count_ytd,

    CASE WHEN resource_type = 'TDS-NDS-HDDV'
        THEN performance_pass_count_ytd ELSE 0 END
        AS official_performance_pass_count_ytd,

    CASE WHEN resource_type = 'TDS-NDS-HDDV'
        THEN performance_exceed_count_ytd ELSE 0 END
        AS official_performance_exceed_count_ytd

FROM ytd
""")
