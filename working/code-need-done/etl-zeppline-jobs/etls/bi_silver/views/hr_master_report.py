%livy.pyspark

# spark.sql("DROP VIEW IF EXISTS bi_silver.hr_master_report")


spark.sql("""
CREATE OR REPLACE VIEW bi_silver.hr_master_report  AS

WITH full_hr_raw AS (

SELECT
o.*,
COALESCE(r.termination_date, o.termination_date) AS termination_date_r,
r.employment_status,
r.termination_reason,
r.region
FROM bi_silver.hr_employee_onboard o
LEFT JOIN bi_silver.hr_employee_resigned r
ON o.employee_code = r.employee_code

), 
ranked_hr AS (

SELECT *,
       ROW_NUMBER() OVER (
            PARTITION BY employee_code
            ORDER BY
                COALESCE(CAST(termination_date_r AS DATE), DATE '0001-01-01') DESC,
                COALESCE(CAST(hire_date AS DATE), DATE '0001-01-01') DESC
       ) AS rn
FROM full_hr_raw

)
SELECT 
employee_code,
UPPER(email_nametag) as email_username, 
UPPER(current_status) as current_status,
UPPER(division) as division,
UPPER(business_unit_level_1) as business_unit_level_1,
UPPER(department_level_2) as department_level_2,
UPPER(job_level) as job_level,
UPPER(management_level) as management_level,

CAST(hire_date AS TIMESTAMP) AS hire_date,
CAST(termination_date_r AS TIMESTAMP) AS termination_date,

gender,

--CAST(date_diff('day', date_of_birth, current_date) / 365) AS age,
CAST(
    (year(current_date) - year(date_of_birth))
    - CASE 
        WHEN date_format(current_date,'MM-dd') < date_format(date_of_birth,'MM-dd')
        THEN 1 ELSE 0
      END
AS INT) AS age,

UPPER(COALESCE(employment_status, 'ACTIVE')) AS employment_status,

termination_reason,

-- CAST(performance_score_q1_2024 AS DOUBLE) AS performance_score_q1_2024,
-- CAST(performance_score_q2_2024 AS DOUBLE) AS performance_score_q2_2024,
-- CAST(performance_score_q3_2024 AS DOUBLE) AS performance_score_q3_2024,
-- CAST(performance_score_q4_2024 AS DOUBLE) AS performance_score_q4_2024,

-- CAST(avg_performance_score_2024 AS DOUBLE) AS avg_performance_score_2024,

-- CAST(performance_score_q1_2025 AS DOUBLE) AS performance_score_q1_2025,
-- CAST(performance_score_q2_2025 AS DOUBLE) AS performance_score_q2_2025,
-- CAST(performance_score_q3_2025 AS DOUBLE) AS performance_score_q3_2025,


UPPER(talent_group) as talent_group,
UPPER(performance_level) as performance_level,
UPPER(potential_level) as potential_level,

CASE
    WHEN is_key_employee = 1 THEN 1
    ELSE 0
END AS is_key_employee_flag

FROM ranked_hr
WHERE rn = 1

""")
