# %livy.pyspark
# Sinh đủ tháng theo dim_date, từ tháng 1 tới hết dim_date.
# Với mỗi tháng báo cáo:
# - Nếu có snapshot đúng/trước tháng đó: lấy snapshot gần nhất trước hoặc bằng tháng báo cáo.
# - Nếu snapshot đầu tiên bắt đầu từ tháng 9 nhưng cần xem từ tháng 1: dùng snapshot tháng 9 backfill cho các tháng 1-8 cùng năm.
# - Nếu tháng tương lai sau snapshot mới nhất: dùng snapshot mới nhất carry-forward cho tới hết dim_date.

target_table = "bi_silver.hr_workforce_monthly"
target_path = "s3a://bi-silver/hr_workforce_monthly"

spark.sql("REFRESH TABLE bi_silver.dim_date")
spark.sql("REFRESH TABLE bi_silver.hr_employee_onboard_snapshot")
spark.sql("REFRESH TABLE bi_silver.hr_employee_resigned_snapshot")
spark.sql("REFRESH TABLE bi_silver.hr_performance_rate")

# spark.sql("DROP TABLE IF EXISTS bi_silver.hr_workforce_monthly")

sql_query = """
WITH tmp_month_spine AS (
    SELECT DISTINCT
        d.date_year AS report_year,
        d.date_year_month AS snapshot_year_month,
        d.date_month_start AS snapshot_month,
        DATE_FORMAT(d.date_month_start, 'yyyy-MMM') AS snapshot_month_label,
        d.date_month_start AS month_start,
        d.date_month_end AS month_end
    FROM bi_silver.dim_date d
    WHERE d.date_value >= CAST('2025-01-01' AS DATE)
),
tmp_snapshot_dates AS (
    SELECT
        YEAR(snapshot_date) AS report_year,
        snapshot_date,
        MAX(crawled_at_ts) AS crawled_at_ts
    FROM bi_silver.hr_employee_onboard_snapshot
    WHERE snapshot_date IS NOT NULL
    GROUP BY
        YEAR(snapshot_date),
        snapshot_date
),

tmp_snapshot_for_month AS (
    SELECT
        report_year,
        snapshot_year_month,
        snapshot_month,
        snapshot_month_label,
        month_start,
        month_end,
        snapshot_date,
        crawled_at_ts,
        snapshot_fill_type
    FROM (
        SELECT
            m.report_year,
            m.snapshot_year_month,
            m.snapshot_month,
            m.snapshot_month_label,
            m.month_start,
            m.month_end,

            s.snapshot_date,
            s.crawled_at_ts,

            CASE
                WHEN YEAR(s.snapshot_date) * 100 + MONTH(s.snapshot_date) = m.snapshot_year_month
                    THEN 'NORMAL'
                WHEN s.snapshot_date < m.month_start
                    THEN 'CARRY_FORWARD'
                WHEN s.snapshot_date > m.month_end
                    THEN 'BACKFILL'
                ELSE 'UNKNOWN'
            END AS snapshot_fill_type,

            ROW_NUMBER() OVER (
                PARTITION BY m.snapshot_year_month
                ORDER BY
                    CASE
                        WHEN s.snapshot_date <= m.month_end THEN 1
                        ELSE 2
                    END,
                    CASE
                        WHEN s.snapshot_date <= m.month_end THEN s.snapshot_date
                    END DESC,
                    CASE
                        WHEN s.snapshot_date > m.month_end THEN s.snapshot_date
                    END ASC,
                    s.crawled_at_ts DESC
            ) AS rn

        FROM tmp_month_spine m
        JOIN tmp_snapshot_dates s
            ON (
                   s.report_year = m.report_year
                OR s.snapshot_date <= m.month_end
            )
    ) x
    WHERE rn = 1
),
tmp_onboard_for_month AS (
    SELECT *
    FROM (
        SELECT
            m.report_year,
            m.snapshot_year_month,
            m.snapshot_month,
            m.snapshot_month_label,
            m.month_start,
            m.month_end,
            m.snapshot_date AS source_snapshot_date,
            m.snapshot_fill_type,

            s.employee_code,
            s.full_name,
            s.business_email,
            s.hire_date_vcs,
            s.termination_date,

            s.division_n,
            s.department_n_1,
            s.department_n_2,
            s.department_n_3,
            s.contract_type,
            s.resource_type,

            s.job_level,
            s.position_level,
            CASE
                WHEN s.position_level = 'N' THEN 1
                WHEN s.position_level = 'N-1' THEN 2
                WHEN s.position_level = 'N-2' THEN 3
                WHEN s.position_level = 'Expert' THEN 4
                WHEN s.position_level = 'Specialist' THEN 5
                WHEN s.position_level = 'Senior' THEN 6
                WHEN s.position_level = 'Experienced' THEN 7
                WHEN s.position_level = 'Junior' THEN 8
                WHEN s.position_level = 'Entry' THEN 9
                WHEN s.position_level = 'CTV' THEN 10
                WHEN s.position_level = 'Fresher' THEN 11
                WHEN s.position_level = 'SV' THEN 12
                WHEN s.position_level = 'IS' THEN 13
                WHEN s.position_level = 'OS' THEN 14
            END AS hierarchy_level,

            s.competency_zone,
            s.tenure_group,
            s.gender,

            s.employee_type,
            s.is_active,
            s.crawled_at_ts,

            ROW_NUMBER() OVER (
                PARTITION BY m.snapshot_year_month, s.employee_code
                ORDER BY s.snapshot_date DESC, s.crawled_at_ts DESC
            ) AS rn

        FROM tmp_snapshot_for_month m
        JOIN bi_silver.hr_employee_onboard_snapshot s
            ON s.snapshot_date = m.snapshot_date
           AND s.crawled_at_ts = m.crawled_at_ts
    ) x
    WHERE rn = 1
),

tmp_resigned_latest AS (
    SELECT *
    FROM (
        SELECT
            r.*,
            ROW_NUMBER() OVER (
                PARTITION BY r.employee_code
                ORDER BY r.resigned_date DESC, r.crawled_at_ts DESC
            ) AS rn
        FROM bi_silver.hr_employee_resigned_snapshot r
        WHERE r.resigned_date IS NOT NULL
    ) x
    WHERE rn = 1
),

tmp_employee_month_union AS (
    SELECT
        o.report_year,
        o.snapshot_year_month,
        o.snapshot_month,
        o.snapshot_month_label,
        o.month_start,
        o.month_end,
        o.source_snapshot_date,
        o.snapshot_fill_type,

        o.employee_code,
        o.full_name,
        o.business_email,
        o.hire_date_vcs,

        COALESCE(r.resigned_date, o.termination_date) AS resigned_date,

        o.division_n,
        o.department_n_1,
        o.department_n_2,
        o.department_n_3,
        o.contract_type,
        o.resource_type,

        o.job_level,
        o.position_level,
        o.hierarchy_level,
        o.competency_zone,
        o.tenure_group,
        o.gender,

        o.employee_type,

        COALESCE(r.is_voluntary_exit, 0) AS is_voluntary_exit,
        COALESCE(r.is_company_termination, 0) AS is_company_termination,

        o.is_active AS snapshot_is_active,

        1 AS source_priority

    FROM tmp_onboard_for_month o
    LEFT JOIN tmp_resigned_latest r
        ON o.employee_code = r.employee_code
    WHERE o.hire_date_vcs IS NOT NULL

    UNION ALL

    SELECT
        m.report_year,
        m.snapshot_year_month,
        m.snapshot_month,
        m.snapshot_month_label,
        m.month_start,
        m.month_end,
        CAST(NULL AS DATE) AS source_snapshot_date,
        'RESIGNED_ONLY' AS snapshot_fill_type,

        r.employee_code,
        r.full_name,
        r.business_email,
        r.hire_date_vcs,

        r.resigned_date,

        r.division_n,
        r.department_n_1,
        r.department_n_2,
        r.department_n_3,
        r.contract_type,
        r.resource_type,

        r.job_level,
        r.position_level,
        CASE
            WHEN r.position_level = 'N' THEN 1
            WHEN r.position_level = 'N-1' THEN 2
            WHEN r.position_level = 'N-2' THEN 3
            WHEN r.position_level = 'Expert' THEN 4
            WHEN r.position_level = 'Specialist' THEN 5
            WHEN r.position_level = 'Senior' THEN 6
            WHEN r.position_level = 'Experienced' THEN 7
            WHEN r.position_level = 'Junior' THEN 8
            WHEN r.position_level = 'Entry' THEN 9
            WHEN r.position_level = 'CTV' THEN 10
            WHEN r.position_level = 'Fresher' THEN 11
            WHEN r.position_level = 'SV' THEN 12
            WHEN r.position_level = 'IS' THEN 13
            WHEN r.position_level = 'OS' THEN 14
        END AS hierarchy_level,

        r.competency_zone,
        r.tenure_group,
        r.gender,

        r.employee_type,

        COALESCE(r.is_voluntary_exit, 0) AS is_voluntary_exit,
        COALESCE(r.is_company_termination, 0) AS is_company_termination,

        0 AS snapshot_is_active,

        2 AS source_priority

    FROM tmp_month_spine m
    JOIN tmp_resigned_latest r
        ON r.hire_date_vcs <= m.month_end
    WHERE r.hire_date_vcs IS NOT NULL
),

tmp_employee_month_base AS (
    SELECT *
    FROM (
        SELECT
            u.*,
            ROW_NUMBER() OVER (
                PARTITION BY u.snapshot_year_month, u.employee_code
                ORDER BY
                    u.source_priority ASC,
                    u.source_snapshot_date DESC
            ) AS rn
        FROM tmp_employee_month_union u
    ) x
    WHERE rn = 1
),

tmp_employee_month_status AS (
    SELECT
        e.report_year,
        e.snapshot_year_month,
        e.snapshot_month,
        e.snapshot_month_label,
        e.month_start,
        e.month_end,
        e.snapshot_fill_type,

        e.employee_code,

        e.division_n,
        e.department_n_1,
        e.department_n_2,
        e.department_n_3,
        e.contract_type,
        e.resource_type,
        e.job_level,
        e.position_level,
        e.hierarchy_level,
        e.competency_zone,
        e.tenure_group,
        e.gender,

        e.hire_date_vcs,
        e.resigned_date,

        CASE
            WHEN e.hire_date_vcs <= e.month_end
             AND (
                    e.resigned_date IS NULL
                 OR e.resigned_date > e.month_end
             )
            THEN 1 ELSE 0
        END AS is_active_month_end

    FROM tmp_employee_month_base e
),

tmp_event_closing_hc AS (
    SELECT
        report_year,
        snapshot_year_month,
        snapshot_month,
        snapshot_month_label,
        month_start,
        month_end,

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
        gender,

        COUNT(DISTINCT employee_code) AS calculated_closing_headcount

    FROM tmp_employee_month_status
    WHERE is_active_month_end = 1
    GROUP BY
        report_year,
        snapshot_year_month,
        snapshot_month,
        snapshot_month_label,
        month_start,
        month_end,

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
),

tmp_new_hire AS (
    SELECT
        e.report_year,
        e.snapshot_year_month,
        e.snapshot_month,
        e.snapshot_month_label,

        e.division_n,
        e.department_n_1,
        e.department_n_2,
        e.department_n_3,
        e.contract_type,
        e.resource_type,

        e.job_level,
        e.position_level,
        e.hierarchy_level,
        e.competency_zone,
        e.tenure_group,
        e.gender,

        COUNT(DISTINCT e.employee_code) AS new_hire_count

    FROM tmp_employee_month_base e
    WHERE e.hire_date_vcs >= CAST('2025-01-01' AS DATE)
      AND YEAR(e.hire_date_vcs) * 100 + MONTH(e.hire_date_vcs) = e.snapshot_year_month

    GROUP BY
        e.report_year,
        e.snapshot_year_month,
        e.snapshot_month,
        e.snapshot_month_label,

        e.division_n,
        e.department_n_1,
        e.department_n_2,
        e.department_n_3,
        e.contract_type,
        e.resource_type,

        e.job_level,
        e.position_level,
        e.hierarchy_level,
        e.competency_zone,
        e.tenure_group,
        e.gender
),

tmp_resigned AS (
    SELECT
        e.report_year,
        e.snapshot_year_month,
        e.snapshot_month,
        e.snapshot_month_label,

        e.division_n,
        e.department_n_1,
        e.department_n_2,
        e.department_n_3,
        e.contract_type,
        e.resource_type,

        e.job_level,
        e.position_level,
        e.hierarchy_level,
        e.competency_zone,
        e.tenure_group,
        e.gender,

        COUNT(DISTINCT e.employee_code) AS resigned_count,

        SUM(COALESCE(e.is_voluntary_exit, 0)) AS voluntary_exit_count,
        SUM(COALESCE(e.is_company_termination, 0)) AS company_termination_count,

        COUNT(DISTINCT CASE
            WHEN e.hire_date_vcs IS NOT NULL
             AND e.resigned_date IS NOT NULL
             AND DATEDIFF(e.resigned_date, e.hire_date_vcs) <= 90
            THEN e.employee_code
        END) AS quick_quit_count

    FROM tmp_employee_month_base e
    WHERE e.resigned_date >= CAST('2025-01-01' AS DATE)
      AND YEAR(e.resigned_date) * 100 + MONTH(e.resigned_date) = e.snapshot_year_month

    GROUP BY
        e.report_year,
        e.snapshot_year_month,
        e.snapshot_month,
        e.snapshot_month_label,

        e.division_n,
        e.department_n_1,
        e.department_n_2,
        e.department_n_3,
        e.contract_type,
        e.resource_type,

        e.job_level,
        e.position_level,
        e.hierarchy_level,
        e.competency_zone,
        e.tenure_group,
        e.gender
),

tmp_snapshot_closing_hc AS (
    SELECT
        report_year,
        snapshot_year_month,
        snapshot_month,
        snapshot_month_label,
        month_start,
        month_end,

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
        gender,

        COUNT(DISTINCT employee_code) AS snapshot_closing_headcount

    FROM tmp_employee_month_base
    WHERE snapshot_is_active = 1

    GROUP BY
        report_year,
        snapshot_year_month,
        snapshot_month,
        snapshot_month_label,
        month_start,
        month_end,

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
),

tmp_promotion AS (
    SELECT
        report_year,
        snapshot_year_month,
        snapshot_month,
        snapshot_month_label,

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
        gender,

        COUNT(DISTINCT employee_code) AS promotion_count,

        COUNT(DISTINCT CASE
            WHEN COALESCE(position_level, '') <> COALESCE(prev_position_level, '')
            THEN employee_code
        END) AS position_level_change_count,

        COUNT(DISTINCT CASE
            WHEN COALESCE(competency_zone, '') <> COALESCE(prev_competency_zone, '')
            THEN employee_code
        END) AS competency_zone_change_count

    FROM (
        SELECT
            s.*,

            LAG(position_level) OVER (
                PARTITION BY employee_code
                ORDER BY snapshot_year_month
            ) AS prev_position_level,

            LAG(competency_zone) OVER (
                PARTITION BY employee_code
                ORDER BY snapshot_year_month
            ) AS prev_competency_zone

        FROM tmp_employee_month_status s
        WHERE is_active_month_end = 1
        AND snapshot_fill_type = 'NORMAL'
    ) t

    WHERE
        (
            prev_position_level IS NOT NULL
            AND COALESCE(position_level, '') <> COALESCE(prev_position_level, '')
        )
        OR
        (
            prev_competency_zone IS NOT NULL
            AND COALESCE(competency_zone, '') <> COALESCE(prev_competency_zone, '')
        )

    GROUP BY
        report_year,
        snapshot_year_month,
        snapshot_month,
        snapshot_month_label,

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
),

tmp_internal_transfer_base AS (
    SELECT
        s.*,

        LAG(division_n) OVER (
            PARTITION BY employee_code
            ORDER BY snapshot_year_month
        ) AS prev_division_n,

        LAG(department_n_1) OVER (
            PARTITION BY employee_code
            ORDER BY snapshot_year_month
        ) AS prev_department_n_1,

        LAG(department_n_2) OVER (
            PARTITION BY employee_code
            ORDER BY snapshot_year_month
        ) AS prev_department_n_2,

        LAG(department_n_3) OVER (
            PARTITION BY employee_code
            ORDER BY snapshot_year_month
        ) AS prev_department_n_3,

        LAG(job_level) OVER (
            PARTITION BY employee_code
            ORDER BY snapshot_year_month
        ) AS prev_job_level,

        LAG(position_level) OVER (
            PARTITION BY employee_code
            ORDER BY snapshot_year_month
        ) AS prev_position_level,

        LAG(hierarchy_level) OVER (
            PARTITION BY employee_code
            ORDER BY snapshot_year_month
        ) AS prev_hierarchy_level,

        LAG(competency_zone) OVER (
            PARTITION BY employee_code
            ORDER BY snapshot_year_month
        ) AS prev_competency_zone,

        LAG(tenure_group) OVER (
            PARTITION BY employee_code
            ORDER BY snapshot_year_month
        ) AS prev_tenure_group,

        LAG(gender) OVER (
            PARTITION BY employee_code
            ORDER BY snapshot_year_month
        ) AS prev_gender,

        LAG(contract_type) OVER (
            PARTITION BY employee_code
            ORDER BY snapshot_year_month
        ) AS prev_contract_type,

        LAG(resource_type) OVER (
            PARTITION BY employee_code
            ORDER BY snapshot_year_month
        ) AS prev_resource_type

    FROM tmp_employee_month_status s
    WHERE is_active_month_end = 1
      AND snapshot_fill_type = 'NORMAL'
),

tmp_internal_transfer AS (
    SELECT
        report_year,
        snapshot_year_month,
        snapshot_month,
        snapshot_month_label,

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
        gender,

        SUM(internal_transfer_in) AS internal_transfer_in,
        SUM(internal_transfer_out) AS internal_transfer_out

    FROM (
        -- Transfer OUT: ghi nhận ở dimension cũ
        SELECT
            report_year,
            snapshot_year_month,
            snapshot_month,
            snapshot_month_label,

            prev_division_n AS division_n,
            prev_department_n_1 AS department_n_1,
            prev_department_n_2 AS department_n_2,
            prev_department_n_3 AS department_n_3,
            prev_contract_type AS contract_type,
            prev_resource_type AS resource_type,

            prev_job_level AS job_level,
            prev_position_level AS position_level,
            prev_hierarchy_level AS hierarchy_level,
            prev_competency_zone AS competency_zone,
            prev_tenure_group AS tenure_group,
            prev_gender AS gender,

            0 AS internal_transfer_in,
            COUNT(DISTINCT employee_code) AS internal_transfer_out

        FROM tmp_internal_transfer_base
        WHERE prev_division_n IS NOT NULL
          AND (
            COALESCE(prev_division_n, '') <> COALESCE(division_n, '')
            OR COALESCE(prev_department_n_1, '') <> COALESCE(department_n_1, '')
            OR COALESCE(prev_department_n_2, '') <> COALESCE(department_n_2, '')
            OR COALESCE(prev_department_n_3, '') <> COALESCE(department_n_3, '')
        )

        GROUP BY
            report_year,
            snapshot_year_month,
            snapshot_month,
            snapshot_month_label,

            prev_division_n,
            prev_department_n_1,
            prev_department_n_2,
            prev_department_n_3,
            prev_contract_type,
            prev_resource_type,

            prev_job_level,
            prev_position_level,
            prev_hierarchy_level,
            prev_competency_zone,
            prev_tenure_group,
            prev_gender

        UNION ALL

        -- Transfer IN: ghi nhận ở dimension mới
        SELECT
            report_year,
            snapshot_year_month,
            snapshot_month,
            snapshot_month_label,

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
            gender,

            COUNT(DISTINCT employee_code) AS internal_transfer_in,
            0 AS internal_transfer_out

        FROM tmp_internal_transfer_base
        WHERE prev_division_n IS NOT NULL
          AND (
            COALESCE(prev_division_n, '') <> COALESCE(division_n, '')
            OR COALESCE(prev_department_n_1, '') <> COALESCE(department_n_1, '')
            OR COALESCE(prev_department_n_2, '') <> COALESCE(department_n_2, '')
            OR COALESCE(prev_department_n_3, '') <> COALESCE(department_n_3, '')
        )

        GROUP BY
            report_year,
            snapshot_year_month,
            snapshot_month,
            snapshot_month_label,

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
    ) x

    GROUP BY
        report_year,
        snapshot_year_month,
        snapshot_month,
        snapshot_month_label,

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
),

tmp_performance_dedup AS (
    SELECT *
    FROM (
        SELECT
            p.*,
            ROW_NUMBER() OVER (
                PARTITION BY p.employee_code, p.evaluation_date
                ORDER BY p.crawled_at_ts DESC
            ) AS rn
        FROM bi_silver.hr_performance_rate p
        WHERE p.evaluation_date IS NOT NULL
          AND p.employee_code IS NOT NULL
    ) x
    WHERE rn = 1
),
tmp_performance AS (
    SELECT
        m.report_year,
        m.snapshot_year_month,
        m.snapshot_month,
        m.snapshot_month_label,
        m.month_start,
        m.month_end,

        p.division_n,
        p.department_n_1,
        p.department_n_2,
        p.department_n_3,
        p.contract_type,
        p.resource_type,

        p.job_level,
        p.position_level,
        CASE
            WHEN p.position_level = 'N' THEN 1
            WHEN p.position_level = 'N-1' THEN 2
            WHEN p.position_level = 'N-2' THEN 3
            WHEN p.position_level = 'Expert' THEN 4
            WHEN p.position_level = 'Specialist' THEN 5
            WHEN p.position_level = 'Senior' THEN 6
            WHEN p.position_level = 'Experienced' THEN 7
            WHEN p.position_level = 'Junior' THEN 8
            WHEN p.position_level = 'Entry' THEN 9
            WHEN p.position_level = 'CTV' THEN 10
            WHEN p.position_level = 'Fresher' THEN 11
            WHEN p.position_level = 'SV' THEN 12
            WHEN p.position_level = 'IS' THEN 13
            WHEN p.position_level = 'OS' THEN 14
        END AS hierarchy_level,
        p.competency_zone,
        p.tenure_group,
        p.gender,

        COUNT(DISTINCT p.employee_code) AS performance_employee_count,

        COUNT(DISTINCT CASE
            WHEN p.performance_result = 'Không đạt'
            THEN p.employee_code
        END) AS performance_not_pass_count,

        COUNT(DISTINCT CASE
            WHEN p.performance_result = 'Đạt'
            THEN p.employee_code
        END) AS performance_pass_count,

        COUNT(DISTINCT CASE
            WHEN p.performance_result = 'Vượt yêu cầu'
            THEN p.employee_code
        END) AS performance_exceed_count

    FROM tmp_month_spine m
    JOIN tmp_performance_dedup p
        ON YEAR(p.evaluation_date) = m.report_year
    AND QUARTER(p.evaluation_date) = QUARTER(m.month_end)
    AND m.month_end <= p.evaluation_date

    GROUP BY
        m.report_year,
        m.snapshot_year_month,
        m.snapshot_month,
        m.snapshot_month_label,
        m.month_start,
        m.month_end,

        p.division_n,
        p.department_n_1,
        p.department_n_2,
        p.department_n_3,
        p.contract_type,
        p.resource_type,

        p.job_level,
        p.position_level,
        CASE
            WHEN p.position_level = 'N' THEN 1
            WHEN p.position_level = 'N-1' THEN 2
            WHEN p.position_level = 'N-2' THEN 3
            WHEN p.position_level = 'Expert' THEN 4
            WHEN p.position_level = 'Specialist' THEN 5
            WHEN p.position_level = 'Senior' THEN 6
            WHEN p.position_level = 'Experienced' THEN 7
            WHEN p.position_level = 'Junior' THEN 8
            WHEN p.position_level = 'Entry' THEN 9
            WHEN p.position_level = 'CTV' THEN 10
            WHEN p.position_level = 'Fresher' THEN 11
            WHEN p.position_level = 'SV' THEN 12
            WHEN p.position_level = 'IS' THEN 13
            WHEN p.position_level = 'OS' THEN 14
        END,
        p.competency_zone,
        p.tenure_group,
        p.gender
),

tmp_month_dim_keys AS (
    SELECT
        report_year,
        snapshot_year_month,
        snapshot_month,
        snapshot_month_label,
        month_start,
        month_end,

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
    FROM tmp_event_closing_hc

    UNION

    SELECT
        m.report_year,
        m.snapshot_year_month,
        m.snapshot_month,
        m.snapshot_month_label,
        m.month_start,
        m.month_end,

        n.division_n,
        n.department_n_1,
        n.department_n_2,
        n.department_n_3,
        n.contract_type,
        n.resource_type,

        n.job_level,
        n.position_level,
        n.hierarchy_level,
        n.competency_zone,
        n.tenure_group,
        n.gender
    FROM tmp_new_hire n
    JOIN tmp_month_spine m
        ON n.snapshot_year_month = m.snapshot_year_month

    UNION

    SELECT
        m.report_year,
        m.snapshot_year_month,
        m.snapshot_month,
        m.snapshot_month_label,
        m.month_start,
        m.month_end,

        r.division_n,
        r.department_n_1,
        r.department_n_2,
        r.department_n_3,
        r.contract_type,
        r.resource_type,

        r.job_level,
        r.position_level,
        r.hierarchy_level,
        r.competency_zone,
        r.tenure_group,
        r.gender
    FROM tmp_resigned r
    JOIN tmp_month_spine m
        ON r.snapshot_year_month = m.snapshot_year_month

    UNION

    SELECT
        report_year,
        snapshot_year_month,
        snapshot_month,
        snapshot_month_label,
        month_start,
        month_end,

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
    FROM tmp_snapshot_closing_hc
    UNION

    SELECT
        report_year,
        snapshot_year_month,
        snapshot_month,
        snapshot_month_label,
        month_start,
        month_end,

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
    FROM tmp_performance
    UNION

    SELECT
        m.report_year,
        m.snapshot_year_month,
        m.snapshot_month,
        m.snapshot_month_label,
        m.month_start,
        m.month_end,

        p.division_n,
        p.department_n_1,
        p.department_n_2,
        p.department_n_3,
        p.contract_type,
        p.resource_type,

        p.job_level,
        p.position_level,
        p.hierarchy_level,
        p.competency_zone,
        p.tenure_group,
        p.gender
    FROM tmp_promotion p
    JOIN tmp_month_spine m
        ON p.snapshot_year_month = m.snapshot_year_month
    UNION

    SELECT
        t.report_year,
        t.snapshot_year_month,
        t.snapshot_month,
        t.snapshot_month_label,
        m.month_start,
        m.month_end,

        t.division_n,
        t.department_n_1,
        t.department_n_2,
        t.department_n_3,
        t.contract_type,
        t.resource_type,

        t.job_level,
        t.position_level,
        t.hierarchy_level,
        t.competency_zone,
        t.tenure_group,
        t.gender
    FROM tmp_internal_transfer t
    JOIN tmp_month_spine m
        ON t.snapshot_year_month = m.snapshot_year_month
),

tmp_monthly_joined AS (
    SELECT
        k.report_year,
        k.snapshot_year_month,
        k.snapshot_month,
        k.snapshot_month_label,
        k.month_start,
        k.month_end,

        k.division_n,
        k.department_n_1,
        k.department_n_2,
        k.department_n_3,
        k.contract_type,
        k.resource_type,

        k.job_level,
        k.position_level,
        k.hierarchy_level,
        k.competency_zone,
        k.tenure_group,
        k.gender,

        COALESCE(c.calculated_closing_headcount, 0) AS calculated_closing_headcount,

        COALESCE(n.new_hire_count, 0) AS new_hire_count,
        COALESCE(r.resigned_count, 0) AS resigned_count,
        COALESCE(r.voluntary_exit_count, 0) AS voluntary_exit_count,
        COALESCE(r.company_termination_count, 0) AS company_termination_count,
        COALESCE(r.quick_quit_count, 0) AS quick_quit_count,

        COALESCE(p.promotion_count, 0) AS promotion_count,
        COALESCE(p.position_level_change_count, 0) AS position_level_change_count,
        COALESCE(p.competency_zone_change_count, 0) AS competency_zone_change_count,
        COALESCE(t.internal_transfer_in, 0) AS internal_transfer_in,
        COALESCE(t.internal_transfer_out, 0) AS internal_transfer_out,
        COALESCE(perf.performance_employee_count, 0) AS performance_employee_count,
        COALESCE(perf.performance_not_pass_count, 0) AS performance_not_pass_count,
        COALESCE(perf.performance_pass_count, 0) AS performance_pass_count,
        COALESCE(perf.performance_exceed_count, 0) AS performance_exceed_count,
        sf.snapshot_date AS source_snapshot_date,
        sf.snapshot_fill_type,
        s.snapshot_closing_headcount

    FROM tmp_month_dim_keys k

    LEFT JOIN tmp_event_closing_hc c
        ON k.snapshot_year_month = c.snapshot_year_month 
       AND COALESCE(k.division_n, '') = COALESCE(c.division_n, '')
       AND COALESCE(k.department_n_1, '') = COALESCE(c.department_n_1, '')
       AND COALESCE(k.department_n_2, '') = COALESCE(c.department_n_2, '')
       AND COALESCE(k.department_n_3, '') = COALESCE(c.department_n_3, '')
       AND COALESCE(k.contract_type, '') = COALESCE(c.contract_type, '')
       AND COALESCE(k.resource_type, '') = COALESCE(c.resource_type, '')
       AND COALESCE(k.job_level, '') = COALESCE(c.job_level, '')
       AND COALESCE(k.position_level, '') = COALESCE(c.position_level, '')
       AND COALESCE(k.hierarchy_level, -1) = COALESCE(c.hierarchy_level, -1)
       AND COALESCE(k.competency_zone, '') = COALESCE(c.competency_zone, '')
       AND COALESCE(k.tenure_group, '') = COALESCE(c.tenure_group, '')
       AND COALESCE(k.gender, '') = COALESCE(c.gender, '')
       

    LEFT JOIN tmp_new_hire n
        ON k.snapshot_year_month = n.snapshot_year_month
       AND COALESCE(k.division_n, '') = COALESCE(n.division_n, '')
       AND COALESCE(k.department_n_1, '') = COALESCE(n.department_n_1, '')
       AND COALESCE(k.department_n_2, '') = COALESCE(n.department_n_2, '')
       AND COALESCE(k.department_n_3, '') = COALESCE(n.department_n_3, '')
       AND COALESCE(k.contract_type, '') = COALESCE(n.contract_type, '')
       AND COALESCE(k.resource_type, '') = COALESCE(n.resource_type, '')
       AND COALESCE(k.job_level, '') = COALESCE(n.job_level, '')
       AND COALESCE(k.position_level, '') = COALESCE(n.position_level, '')
       AND COALESCE(k.hierarchy_level, -1) = COALESCE(n.hierarchy_level, -1)
       AND COALESCE(k.competency_zone, '') = COALESCE(n.competency_zone, '')
       AND COALESCE(k.tenure_group, '') = COALESCE(n.tenure_group, '')
       AND COALESCE(k.gender, '') = COALESCE(n.gender, '')

    LEFT JOIN tmp_resigned r
        ON k.snapshot_year_month = r.snapshot_year_month
       AND COALESCE(k.division_n, '') = COALESCE(r.division_n, '')
       AND COALESCE(k.department_n_1, '') = COALESCE(r.department_n_1, '')
       AND COALESCE(k.department_n_2, '') = COALESCE(r.department_n_2, '')
       AND COALESCE(k.department_n_3, '') = COALESCE(r.department_n_3, '')
       AND COALESCE(k.contract_type, '') = COALESCE(r.contract_type, '')
       AND COALESCE(k.resource_type, '') = COALESCE(r.resource_type, '')
       AND COALESCE(k.job_level, '') = COALESCE(r.job_level, '')
       AND COALESCE(k.position_level, '') = COALESCE(r.position_level, '')
       AND COALESCE(k.hierarchy_level, -1) = COALESCE(r.hierarchy_level, -1)
       AND COALESCE(k.competency_zone, '') = COALESCE(r.competency_zone, '')
       AND COALESCE(k.tenure_group, '') = COALESCE(r.tenure_group, '')
       AND COALESCE(k.gender, '') = COALESCE(r.gender, '')

    LEFT JOIN tmp_promotion p
        ON k.snapshot_year_month = p.snapshot_year_month
       AND COALESCE(k.division_n, '') = COALESCE(p.division_n, '')
       AND COALESCE(k.department_n_1, '') = COALESCE(p.department_n_1, '')
       AND COALESCE(k.department_n_2, '') = COALESCE(p.department_n_2, '')
       AND COALESCE(k.department_n_3, '') = COALESCE(p.department_n_3, '')
       AND COALESCE(k.contract_type, '') = COALESCE(p.contract_type, '')
       AND COALESCE(k.resource_type, '') = COALESCE(p.resource_type, '')
       AND COALESCE(k.job_level, '') = COALESCE(p.job_level, '')
       AND COALESCE(k.position_level, '') = COALESCE(p.position_level, '')
       AND COALESCE(k.hierarchy_level, -1) = COALESCE(p.hierarchy_level, -1)
       AND COALESCE(k.competency_zone, '') = COALESCE(p.competency_zone, '')
       AND COALESCE(k.tenure_group, '') = COALESCE(p.tenure_group, '')
       AND COALESCE(k.gender, '') = COALESCE(p.gender, '')
    LEFT JOIN tmp_internal_transfer t
         ON k.snapshot_year_month = t.snapshot_year_month
        AND COALESCE(k.division_n, '') = COALESCE(t.division_n, '')
        AND COALESCE(k.department_n_1, '') = COALESCE(t.department_n_1, '')
        AND COALESCE(k.department_n_2, '') = COALESCE(t.department_n_2, '')
        AND COALESCE(k.department_n_3, '') = COALESCE(t.department_n_3, '')
        AND COALESCE(k.contract_type, '') = COALESCE(t.contract_type, '')
        AND COALESCE(k.resource_type, '') = COALESCE(t.resource_type, '')
        AND COALESCE(k.job_level, '') = COALESCE(t.job_level, '')
        AND COALESCE(k.position_level, '') = COALESCE(t.position_level, '')
        AND COALESCE(k.hierarchy_level, -1) = COALESCE(t.hierarchy_level, -1)
        AND COALESCE(k.competency_zone, '') = COALESCE(t.competency_zone, '')
        AND COALESCE(k.tenure_group, '') = COALESCE(t.tenure_group, '')
        AND COALESCE(k.gender, '') = COALESCE(t.gender, '')

    LEFT JOIN tmp_snapshot_closing_hc s
        ON k.snapshot_year_month = s.snapshot_year_month
       AND COALESCE(k.division_n, '') = COALESCE(s.division_n, '')
       AND COALESCE(k.department_n_1, '') = COALESCE(s.department_n_1, '')
       AND COALESCE(k.department_n_2, '') = COALESCE(s.department_n_2, '')
       AND COALESCE(k.department_n_3, '') = COALESCE(s.department_n_3, '')
       AND COALESCE(k.contract_type, '') = COALESCE(s.contract_type, '')
       AND COALESCE(k.resource_type, '') = COALESCE(s.resource_type, '')
       AND COALESCE(k.job_level, '') = COALESCE(s.job_level, '')
       AND COALESCE(k.position_level, '') = COALESCE(s.position_level, '')
       AND COALESCE(k.hierarchy_level, -1) = COALESCE(s.hierarchy_level, -1)
       AND COALESCE(k.competency_zone, '') = COALESCE(s.competency_zone, '')
       AND COALESCE(k.tenure_group, '') = COALESCE(s.tenure_group, '')
       AND COALESCE(k.gender, '') = COALESCE(s.gender, '')
    LEFT JOIN tmp_snapshot_for_month sf
         ON k.snapshot_year_month = sf.snapshot_year_month
    LEFT JOIN tmp_performance perf
            ON k.snapshot_year_month = perf.snapshot_year_month
        AND COALESCE(k.division_n, '') = COALESCE(perf.division_n, '')
        AND COALESCE(k.department_n_1, '') = COALESCE(perf.department_n_1, '')
        AND COALESCE(k.department_n_2, '') = COALESCE(perf.department_n_2, '')
        AND COALESCE(k.department_n_3, '') = COALESCE(perf.department_n_3, '')
        AND COALESCE(k.contract_type, '') = COALESCE(perf.contract_type, '')
        AND COALESCE(k.resource_type, '') = COALESCE(perf.resource_type, '')
        AND COALESCE(k.job_level, '') = COALESCE(perf.job_level, '')
        AND COALESCE(k.position_level, '') = COALESCE(perf.position_level, '')
        AND COALESCE(k.hierarchy_level, -1) = COALESCE(perf.hierarchy_level, -1)
        AND COALESCE(k.competency_zone, '') = COALESCE(perf.competency_zone, '')
        AND COALESCE(k.tenure_group, '') = COALESCE(perf.tenure_group, '')
        AND COALESCE(k.gender, '') = COALESCE(perf.gender, '')
),

tmp_final_calc AS (
    SELECT
        *,

        COALESCE(
            LAG(calculated_closing_headcount) OVER (
                PARTITION BY
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
                ORDER BY snapshot_year_month
            ),
            0
        ) AS opening_headcount

    FROM tmp_monthly_joined
)

SELECT
    report_year,
    snapshot_year_month,
    snapshot_month,
    snapshot_month_label,
    month_start,
    month_end,

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
    gender,
    source_snapshot_date,
    snapshot_fill_type,

    opening_headcount,

    calculated_closing_headcount AS closing_headcount,

    CAST(
        opening_headcount + calculated_closing_headcount
        AS DOUBLE
    ) / 2.0 AS avg_headcount,

    new_hire_count,
    resigned_count,
    voluntary_exit_count,
    company_termination_count,
    quick_quit_count,

    promotion_count,
    position_level_change_count,
    competency_zone_change_count,
    internal_transfer_in,
    internal_transfer_out,
    internal_transfer_in - internal_transfer_out AS internal_transfer_net,

    new_hire_count
    + internal_transfer_in
    - internal_transfer_out
    - resigned_count AS net_change,

    snapshot_closing_headcount,

    CASE
        WHEN snapshot_closing_headcount IS NOT NULL
        THEN snapshot_closing_headcount - calculated_closing_headcount
        ELSE NULL
    END AS headcount_variance,

    CAST(resigned_count AS DOUBLE)
        / NULLIF(
            CAST(
                CAST(opening_headcount + calculated_closing_headcount AS DOUBLE) / 2.0
                AS DOUBLE
            ),
            0.0
        ) AS attrition_rate,

    CAST(resigned_count AS DOUBLE)
        / NULLIF(
            CAST(
                CAST(opening_headcount + calculated_closing_headcount AS DOUBLE) / 2.0
                AS DOUBLE
            ),
            0.0
        ) AS turnover_rate,

    CAST(voluntary_exit_count AS DOUBLE)
        / NULLIF(
            CAST(
                CAST(opening_headcount + calculated_closing_headcount AS DOUBLE) / 2.0
                AS DOUBLE
            ),
            0.0
        ) AS voluntary_turnover_rate,

    CAST(company_termination_count AS DOUBLE)
        / NULLIF(
            CAST(
                CAST(opening_headcount + calculated_closing_headcount AS DOUBLE) / 2.0
                AS DOUBLE
            ),
            0.0
        ) AS involuntary_turnover_rate,

    CAST(quick_quit_count AS DOUBLE)
        / NULLIF(
            CAST(
                CAST(opening_headcount + calculated_closing_headcount AS DOUBLE) / 2.0
                AS DOUBLE
            ),
            0.0
        ) AS quick_quit_rate_by_avg_headcount,

    CAST(quick_quit_count AS DOUBLE)
        / NULLIF(CAST(new_hire_count AS DOUBLE), 0.0) AS quick_quit_rate_by_new_hire,

    CAST(new_hire_count AS DOUBLE)
        / NULLIF(
            CAST(
                CAST(opening_headcount + calculated_closing_headcount AS DOUBLE) / 2.0
                AS DOUBLE
            ),
            0.0
        ) AS hiring_rate,

    CAST(promotion_count AS DOUBLE)
        / NULLIF(
            CAST(
                CAST(opening_headcount + calculated_closing_headcount AS DOUBLE) / 2.0
                AS DOUBLE
            ),
            0.0
        ) AS promotion_rate,
    performance_employee_count,
    performance_not_pass_count,
    performance_pass_count,
    performance_exceed_count
FROM tmp_final_calc
"""

df = spark.sql(sql_query)

df.write \
    .mode("overwrite") \
    .format("parquet") \
    .option("path", target_path) \
    .saveAsTable(target_table)

spark.catalog.refreshTable(target_table)

print("Done:", target_table)