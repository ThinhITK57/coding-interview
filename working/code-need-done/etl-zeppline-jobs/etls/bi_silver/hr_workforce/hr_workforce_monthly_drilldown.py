# %livy.pyspark

target_table = "bi_silver.hr_workforce_monthly_drilldown"
target_path = "/opt/datasets/crawlers/vcs_silver/bi_silver/data/hr_workforce_monthly_drilldown"

spark.sql("REFRESH TABLE bi_silver.dim_date")
spark.sql("REFRESH TABLE bi_silver.hr_employee_onboard_snapshot")
spark.sql("REFRESH TABLE bi_silver.hr_employee_resigned_snapshot")
spark.sql("REFRESH TABLE bi_silver.hr_performance_rate")

spark.sql("DROP TABLE IF EXISTS {}".format(target_table))

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
      AND d.date_month_start <= DATE_TRUNC('MONTH', CURRENT_DATE())
),

tmp_snapshot_dates AS (
    SELECT
        YEAR(snapshot_date) AS report_year,
        snapshot_date,
        snapshot_year_month,
        MAX(crawled_at_ts) AS crawled_at_ts
    FROM bi_silver.hr_employee_onboard_snapshot
    WHERE snapshot_date IS NOT NULL
    GROUP BY
        YEAR(snapshot_date),
        snapshot_date,
        snapshot_year_month
),

tmp_snapshot_for_month AS (
    SELECT *
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
                WHEN YEAR(s.snapshot_date) * 100 + MONTH(s.snapshot_date) = m.snapshot_year_month THEN 'NORMAL'
                WHEN s.snapshot_date < m.month_start THEN 'CARRY_FORWARD'
                WHEN s.snapshot_date > m.month_end THEN 'BACKFILL'
                ELSE 'UNKNOWN'
            END AS snapshot_fill_type,

            ROW_NUMBER() OVER (
                PARTITION BY m.snapshot_year_month
                ORDER BY
                    CASE WHEN s.snapshot_date <= m.month_end THEN 1 ELSE 2 END,
                    CASE WHEN s.snapshot_date <= m.month_end THEN s.snapshot_date END DESC,
                    CASE WHEN s.snapshot_date > m.month_end THEN s.snapshot_date END ASC,
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
            s.email_nametag,
            s.date_of_birth,
            s.mobile_phone,

            s.status AS onboard_status,
            s.hire_status,

            s.employee_type,
            s.contract_type,
            s.resource_type,
            s.branch,
            s.division_n,
            s.department_n_1,
            s.department_n_2,
            s.department_n_3,
            s.role_base,
            s.job_level,
            s.management_level,
            s.position_level,
            s.hierarchy_level,
            s.competency_zone,

            s.hire_date_vcs,
            s.termination_date,

            s.gender,
            s.age,
            s.age_group,
            s.tenure,
            s.tenure_months,
            s.tenure_group,

            s.is_active AS onboard_is_active,
            s.is_new_hire AS onboard_is_new_hire,
            s.is_voluntary_exit AS onboard_is_voluntary_exit,
            s.is_company_termination AS onboard_is_company_termination,

            s.performance_ranking,
            s.potential_ranking,
            s.talent_group,

            s.crawled_at_ts AS onboard_crawled_at_ts,
            s.filename AS onboard_filename,

            ROW_NUMBER() OVER (
                PARTITION BY m.snapshot_year_month, s.employee_code
                ORDER BY s.snapshot_date DESC, s.crawled_at_ts DESC
            ) AS rn

        FROM tmp_snapshot_for_month m
        JOIN bi_silver.hr_employee_onboard_snapshot s
            ON s.snapshot_date = m.snapshot_date
           AND s.crawled_at_ts = m.crawled_at_ts
        WHERE s.employee_code IS NOT NULL
          AND s.hire_date_vcs IS NOT NULL
    ) x
    WHERE rn = 1
),

tmp_resigned_for_month AS (
    SELECT *
    FROM (
        SELECT
            m.report_year,
            m.snapshot_year_month,
            m.month_start,
            m.month_end,

            r.employee_code,

            r.current_status AS resigned_current_status,
            r.new_hire_status AS resigned_new_hire_status,

            r.resigned_date,
            r.resigned_month,
            r.resigned_month_start,
            r.resigned_year_month,

            r.resignation_status,
            r.resignation_reason_level_1,
            r.resignation_reason_level_2,
            r.resignation_reason_detail,
            r.next_workplace,

            r.is_voluntary_exit AS resigned_is_voluntary_exit,
            r.is_company_termination AS resigned_is_company_termination,

            r.full_name AS resigned_full_name,
            r.business_email AS resigned_business_email,
            r.email_nametag AS resigned_email_nametag,
            r.date_of_birth AS resigned_date_of_birth,
            r.mobile_phone AS resigned_mobile_phone,
            r.talent_group AS resigned_talent_group,
            r.gender AS resigned_gender,
            r.employee_type AS resigned_employee_type,
            r.contract_type AS resigned_contract_type,
            r.resource_type AS resigned_resource_type,
            r.branch AS resigned_branch,
            r.division_n AS resigned_division_n,
            r.department_n_1 AS resigned_department_n_1,
            r.department_n_2 AS resigned_department_n_2,
            r.department_n_3 AS resigned_department_n_3,
            r.role_base AS resigned_role_base,
            r.job_level AS resigned_job_level,
            r.management_level AS resigned_management_level,
            r.position_level AS resigned_position_level,
            r.hierarchy_level AS resigned_hierarchy_level,
            r.competency_zone AS resigned_competency_zone,
            r.hire_date_vcs AS resigned_hire_date_vcs,
            r.tenure AS resigned_tenure,
            r.age AS resigned_age,
            r.age_group AS resigned_age_group,
            r.tenure_months AS resigned_tenure_months,
            r.tenure_group AS resigned_tenure_group,

            r.snapshot_date AS resigned_snapshot_date,
            r.crawled_at_ts AS resigned_crawled_at_ts,
            r.filename AS resigned_filename,

            ROW_NUMBER() OVER (
                PARTITION BY m.snapshot_year_month, r.employee_code
                ORDER BY
                    r.snapshot_date DESC,
                    r.crawled_at_ts DESC,
                    r.resigned_date DESC
            ) AS rn

        FROM tmp_month_spine m
        JOIN bi_silver.hr_employee_resigned_snapshot r
            ON r.snapshot_year_month = m.snapshot_year_month
           AND r.employee_code IS NOT NULL
           AND r.hire_date_vcs IS NOT NULL
           AND r.resigned_date IS NOT NULL
    ) x
    WHERE rn = 1
),

tmp_employee_month_merged AS (
    SELECT
        o.report_year,
        o.snapshot_year_month,
        o.snapshot_month,
        o.snapshot_month_label,
        o.month_start,
        o.month_end,
        o.source_snapshot_date,
        o.snapshot_fill_type,

        COALESCE(o.employee_code, r.employee_code) AS employee_code,
        COALESCE(o.full_name, r.resigned_full_name) AS full_name,
        COALESCE(o.business_email, r.resigned_business_email) AS business_email,
        COALESCE(o.email_nametag, r.resigned_email_nametag) AS email_nametag,
        COALESCE(o.date_of_birth, r.resigned_date_of_birth) AS date_of_birth,
        COALESCE(o.mobile_phone, r.resigned_mobile_phone) AS mobile_phone,

        COALESCE(o.gender, r.resigned_gender) AS gender,
        COALESCE(o.age, r.resigned_age) AS age,
        COALESCE(o.age_group, r.resigned_age_group) AS age_group,

        COALESCE(o.hire_date_vcs, r.resigned_hire_date_vcs) AS hire_date_vcs,

        CASE
            WHEN r.resigned_date IS NOT NULL THEN r.resigned_date
            WHEN o.termination_date IS NOT NULL THEN o.termination_date
            ELSE NULL
        END AS resigned_date,

        o.termination_date,

        COALESCE(r.resigned_year_month, YEAR(o.termination_date) * 100 + MONTH(o.termination_date)) AS resigned_year_month,

        COALESCE(o.employee_type, r.resigned_employee_type) AS employee_type,
        COALESCE(o.contract_type, r.resigned_contract_type) AS contract_type,
        COALESCE(o.resource_type, r.resigned_resource_type) AS resource_type,
        COALESCE(o.branch, r.resigned_branch) AS branch,
        COALESCE(o.division_n, r.resigned_division_n) AS division_n,
        COALESCE(o.department_n_1, r.resigned_department_n_1) AS department_n_1,
        COALESCE(o.department_n_2, r.resigned_department_n_2) AS department_n_2,
        COALESCE(o.department_n_3, r.resigned_department_n_3) AS department_n_3,
        COALESCE(o.role_base, r.resigned_role_base) AS role_base,
        COALESCE(o.job_level, r.resigned_job_level) AS job_level,
        COALESCE(o.management_level, r.resigned_management_level) AS management_level,
        COALESCE(o.position_level, r.resigned_position_level) AS position_level,
        COALESCE(o.hierarchy_level, r.resigned_hierarchy_level) AS hierarchy_level,
        COALESCE(o.competency_zone, r.resigned_competency_zone) AS competency_zone,

        COALESCE(o.tenure, r.resigned_tenure) AS tenure,
        COALESCE(o.tenure_months, r.resigned_tenure_months) AS tenure_months,
        COALESCE(o.tenure_group, r.resigned_tenure_group) AS tenure_group,

        COALESCE(o.talent_group, r.resigned_talent_group) AS talent_group,

        o.onboard_status,
        o.hire_status,

        r.resigned_current_status,
        r.resigned_new_hire_status,
        r.resignation_status,
        r.resignation_reason_level_1,
        r.resignation_reason_level_2,
        r.resignation_reason_detail,
        r.next_workplace,

        CASE
            WHEN r.resigned_date IS NOT NULL THEN COALESCE(r.resigned_current_status, r.resignation_status, 'Đã nghỉ')
            WHEN o.termination_date IS NOT NULL
             AND o.termination_date <= o.month_end THEN 'Đã nghỉ'
            ELSE COALESCE(o.onboard_status, 'Không xác định')
        END AS employee_status,

        CASE
            WHEN r.resigned_date IS NOT NULL
             AND r.resigned_date BETWEEN o.month_start AND o.month_end
            THEN 'Nghỉ trong tháng'

            WHEN o.termination_date IS NOT NULL
             AND o.termination_date BETWEEN o.month_start AND o.month_end
            THEN 'Nghỉ trong tháng'

            WHEN COALESCE(r.resigned_date, o.termination_date) IS NULL
             AND o.hire_date_vcs <= o.month_end
            THEN 'Active trong tháng'

            WHEN COALESCE(r.resigned_date, o.termination_date) >= o.month_start
             AND o.hire_date_vcs <= o.month_end
            THEN 'Active trong tháng'

            ELSE 'Không xác định'
        END AS employee_status_month,

        COALESCE(r.resigned_is_voluntary_exit, o.onboard_is_voluntary_exit, 0) AS is_voluntary_exit,
        COALESCE(r.resigned_is_company_termination, o.onboard_is_company_termination, 0) AS is_company_termination,

        o.onboard_is_active AS snapshot_is_active,
        o.onboard_is_new_hire AS snapshot_is_new_hire,

        o.performance_ranking,
        o.potential_ranking,

        o.onboard_crawled_at_ts,
        r.resigned_crawled_at_ts,
        o.onboard_filename,
        r.resigned_filename

    FROM tmp_onboard_for_month o
    LEFT JOIN tmp_resigned_for_month r
        ON o.snapshot_year_month = r.snapshot_year_month
       AND o.employee_code = r.employee_code
),

tmp_employee_month_status AS (
    SELECT
        e.*,

        CASE
            WHEN e.hire_date_vcs <= e.month_end
             AND (
                    e.resigned_date IS NULL
                 OR e.resigned_date >= e.month_start
             )
            THEN 1 ELSE 0
        END AS is_active_month,

        CASE
            WHEN e.hire_date_vcs <= e.month_end
             AND (
                    e.resigned_date IS NULL
                 OR e.resigned_date > e.month_end
             )
            THEN 1 ELSE 0
        END AS is_active_month_end,

        CASE
            WHEN e.hire_date_vcs >= CAST('2025-01-01' AS DATE)
             AND YEAR(e.hire_date_vcs) * 100 + MONTH(e.hire_date_vcs) = e.snapshot_year_month
            THEN 1 ELSE 0
        END AS is_new_hire_month,

        CASE
            WHEN e.resigned_date >= CAST('2025-01-01' AS DATE)
             AND YEAR(e.resigned_date) * 100 + MONTH(e.resigned_date) = e.snapshot_year_month
            THEN 1 ELSE 0
        END AS is_resigned_month,

        CASE
            WHEN e.hire_date_vcs IS NOT NULL
             AND e.resigned_date IS NOT NULL
             AND DATEDIFF(e.resigned_date, e.hire_date_vcs) <= 90
             AND YEAR(e.resigned_date) * 100 + MONTH(e.resigned_date) = e.snapshot_year_month
            THEN 1 ELSE 0
        END AS is_quick_quit_month

    FROM tmp_employee_month_merged e
    WHERE e.hire_date_vcs <= e.month_end
      AND (
            e.resigned_date IS NULL
         OR e.resigned_date >= e.month_start
      )
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

tmp_performance_for_month AS (
    SELECT *
    FROM (
        SELECT
            e.snapshot_year_month,
            e.employee_code,

            p.evaluation_date,
            p.score,
            p.performance_result,
            p.ki,
            p.crawled_at_ts AS performance_crawled_at_ts,

            ROW_NUMBER() OVER (
                PARTITION BY e.snapshot_year_month, e.employee_code
                ORDER BY p.evaluation_date DESC, p.crawled_at_ts DESC
            ) AS rn

        FROM tmp_employee_month_status e
        LEFT JOIN tmp_performance_dedup p
            ON e.employee_code = p.employee_code
           AND YEAR(p.evaluation_date) = e.report_year
           AND QUARTER(p.evaluation_date) = QUARTER(e.month_end)
           AND e.month_end <= p.evaluation_date
    ) x
    WHERE rn = 1
)

SELECT
    e.report_year,
    e.snapshot_year_month,
    e.snapshot_month,
    e.snapshot_month_label,
    e.month_start,
    e.month_end,
    e.source_snapshot_date,
    e.snapshot_fill_type,

    e.employee_code,
    e.full_name,
    e.business_email,

    CASE
        WHEN e.business_email IS NOT NULL
         AND INSTR(e.business_email, '@') > 0
        THEN LOWER(SPLIT(e.business_email, '@')[0])
        ELSE e.email_nametag
    END AS username,

    e.date_of_birth,
    e.mobile_phone,
    e.gender,
    e.age,
    e.age_group,

    e.hire_date_vcs,
    e.termination_date,
    e.resigned_date,
    e.resigned_year_month,

    e.employee_status,
    e.employee_status_month,
    e.onboard_status,
    e.hire_status,
    e.resigned_current_status,
    e.resigned_new_hire_status,
    e.resignation_status,
    e.resignation_reason_level_1,
    e.resignation_reason_level_2,
    e.resignation_reason_detail,
    e.next_workplace,

    e.employee_type,
    e.contract_type,
    e.resource_type,
    e.branch,
    e.division_n,
    e.department_n_1,
    e.department_n_2,
    e.department_n_3,
    e.role_base,
    e.job_level,
    e.management_level,
    e.position_level,
    e.hierarchy_level,
    e.competency_zone,

    e.tenure,
    e.tenure_months,
    e.tenure_group,
    e.talent_group,

    e.snapshot_is_active,
    e.snapshot_is_new_hire,

    e.is_active_month,
    e.is_active_month_end,
    e.is_new_hire_month,
    e.is_resigned_month,
    e.is_quick_quit_month,
    e.is_voluntary_exit,
    e.is_company_termination,

    e.performance_ranking,
    e.potential_ranking,

    COALESCE(
        p.evaluation_date,
        CASE
            WHEN QUARTER(e.month_end) = 1 THEN CAST(CONCAT(CAST(e.report_year AS STRING), '-03-31') AS DATE)
            WHEN QUARTER(e.month_end) = 2 THEN CAST(CONCAT(CAST(e.report_year AS STRING), '-06-30') AS DATE)
            WHEN QUARTER(e.month_end) = 3 THEN CAST(CONCAT(CAST(e.report_year AS STRING), '-09-30') AS DATE)
            WHEN QUARTER(e.month_end) = 4 THEN CAST(CONCAT(CAST(e.report_year AS STRING), '-12-31') AS DATE)
        END
    ) AS evaluation_date,

    p.score,
    COALESCE(p.performance_result, 'Không đánh giá') AS performance_result,
    p.ki,
    p.performance_crawled_at_ts,

    CASE
        WHEN p.evaluation_date IS NOT NULL THEN 1
        ELSE 0
    END AS has_performance_evaluation,

    e.onboard_crawled_at_ts,
    e.resigned_crawled_at_ts,
    e.onboard_filename,
    e.resigned_filename

FROM tmp_employee_month_status e
LEFT JOIN tmp_performance_for_month p
    ON e.snapshot_year_month = p.snapshot_year_month
   AND e.employee_code = p.employee_code
"""

df = spark.sql(sql_query)

df.write \
    .mode("overwrite") \
    .format("parquet") \
    .option("path", target_path) \
    .saveAsTable(target_table)

spark.catalog.refreshTable(target_table)

print("Done:", target_table)

# Validate count
spark.sql("SELECT COUNT(*) AS cnt FROM {}".format(target_table)).show()

# Validate duplicate employee per month
spark.sql("""
SELECT
    snapshot_year_month,
    employee_code,
    COUNT(*) AS cnt
FROM {}
GROUP BY
    snapshot_year_month,
    employee_code
HAVING COUNT(*) > 1
LIMIT 20
""".format(target_table)).show(truncate=False)

# Validate monthly metrics
spark.sql("""
SELECT
    snapshot_year_month,
    snapshot_month,
    snapshot_month_label,
    COUNT(DISTINCT employee_code) AS employee_count,
    SUM(is_active_month) AS active_employee_count,
    SUM(is_active_month_end) AS active_month_end_count,
    SUM(is_new_hire_month) AS new_hire_count,
    SUM(is_resigned_month) AS resigned_count,
    SUM(is_quick_quit_month) AS quick_quit_count
FROM {}
GROUP BY
    snapshot_year_month,
    snapshot_month,
    snapshot_month_label
ORDER BY snapshot_year_month ASC
LIMIT 50
""".format(target_table)).show(truncate=False)

# Validate resigned employee does not appear after resigned month
spark.sql("""
SELECT
    employee_code,
    full_name,
    resigned_date,
    snapshot_year_month,
    month_start,
    month_end,
    employee_status_month
FROM {}
WHERE resigned_date IS NOT NULL
  AND month_start > resigned_date
ORDER BY employee_code, snapshot_year_month
LIMIT 20
""".format(target_table)).show(truncate=False)

# Validate resigned reason
spark.sql("""
SELECT
    snapshot_year_month,
    resignation_status,
    resignation_reason_level_1,
    resignation_reason_level_2,
    resignation_reason_detail,
    COUNT(DISTINCT employee_code) AS resigned_employee_count
FROM {}
WHERE is_resigned_month = 1
GROUP BY
    snapshot_year_month,
    resignation_status,
    resignation_reason_level_1,
    resignation_reason_level_2,
    resignation_reason_detail
ORDER BY snapshot_year_month ASC, resigned_employee_count DESC
LIMIT 100
""".format(target_table)).show(truncate=False)

# Sample output
spark.sql("""
SELECT *
FROM {}
ORDER BY snapshot_year_month ASC, employee_code ASC
LIMIT 10
""".format(target_table)).show(truncate=False)