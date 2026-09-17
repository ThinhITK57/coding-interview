%livy.pyspark
from pyspark.sql import functions as F

target_table = "bi_silver.cx_mixpanel_product_feature_usage_mart"
target_path  = "/opt/datasets/crawlers/vcs/bi_silver/data/cx_mixpanel_product_feature_usage_mart"

spark.catalog.clearCache()
spark.sql("REFRESH TABLE bi_silver.cx_mixpanel_product_feature")


# ============================================================
# 1. Base user
# ============================================================

product_feature_src = spark.sql("""
SELECT
    report_date,
    CAST(date_trunc('WEEK', report_date) AS DATE) AS report_week,
    CAST(trunc(report_date, 'MM') AS DATE) AS report_month,
    product_category,
    product_feature,
    user_id,
    is_active_user
FROM bi_silver.cx_mixpanel_product_feature
WHERE user_id IS NOT NULL
--AND is_blacklist_user = 0         --BLACKLIST DEACTIVE
""").dropDuplicates()

product_feature_src.createOrReplaceTempView("product_feature_src")


# ============================================================
# 2. Weekly user
# ============================================================

weekly_users = spark.sql("""
SELECT DISTINCT
    report_week AS report_period,
    product_category,
    product_feature,
    user_id
FROM product_feature_src
WHERE product_feature IS NOT NULL
""")

weekly_users.createOrReplaceTempView("weekly_users")


# ============================================================
# 3. Weekly user status
# ============================================================

weekly_user_status = spark.sql("""
SELECT
    report_period,
    product_category,
    product_feature,
    user_id,

    -- Kỳ trước mà user sử dụng tính năng
    LAG(report_period) OVER (
        PARTITION BY
            product_category,
            product_feature,
            user_id
        ORDER BY report_period
    ) AS previous_user_period

FROM weekly_users
""")

weekly_user_status.createOrReplaceTempView("weekly_user_status")


# ============================================================
# 4. Weekly metrics
# ============================================================

weekly_metrics = spark.sql("""
SELECT
    report_period,
    product_category,
    product_feature,

    -- Tử số growth: Tổng số người sử dụng tính năng kỳ này
    COUNT(DISTINCT user_id) AS current_period_users,

    -- Tử số retention: User kỳ trước tiếp tục sử dụng kỳ này
    COUNT(DISTINCT CASE
        WHEN previous_user_period = date_sub(report_period, 7)
        THEN user_id
    END) AS retained_users

FROM weekly_user_status

GROUP BY
    report_period,
    product_category,
    product_feature
""")

weekly_metrics.createOrReplaceTempView("weekly_metrics")


# ============================================================
# 5. Weekly breadth
# ============================================================

weekly_breadth = spark.sql("""
WITH user_features AS (
    SELECT
        report_period,
        product_category,
        user_id,
        COUNT(DISTINCT product_feature) AS feature_count
    FROM weekly_users
    GROUP BY
        report_period,
        product_category,
        user_id
),

active_users AS (
    SELECT
        report_week AS report_period,
        product_category,
        COUNT(DISTINCT CASE
            WHEN is_active_user = 1 THEN user_id
        END) AS active_users
    FROM product_feature_src
    GROUP BY
        report_week,
        product_category
)

SELECT
    u.report_period,
    u.product_category,

    -- Tổng số feature DISTINCT của từng user
    SUM(u.feature_count) AS total_feature_count,

    -- Số active user, bao gồm user không sử dụng feature
    MAX(a.active_users) AS active_users,

    -- Số tính năng trung bình sử dụng bởi active user
    CAST(SUM(u.feature_count) AS DOUBLE)
        / NULLIF(MAX(a.active_users), 0)
        AS average_features_per_active_user,

    -- Số user theo số lượng feature
    SUM(CASE WHEN u.feature_count = 1 THEN 1 ELSE 0 END)
        AS users_with_1_feature,

    SUM(CASE WHEN u.feature_count = 2 THEN 1 ELSE 0 END)
        AS users_with_2_features,

    SUM(CASE WHEN u.feature_count = 3 THEN 1 ELSE 0 END)
        AS users_with_3_features,

    SUM(CASE WHEN u.feature_count = 4 THEN 1 ELSE 0 END)
        AS users_with_4_features,

    SUM(CASE WHEN u.feature_count >= 5 THEN 1 ELSE 0 END)
        AS users_with_5_plus_features

FROM user_features u

LEFT JOIN active_users a
    ON u.report_period = a.report_period
    AND u.product_category = a.product_category

GROUP BY
    u.report_period,
    u.product_category
""")

weekly_breadth.createOrReplaceTempView("weekly_breadth")


# ============================================================
# 6. Weekly final metrics
# ============================================================

weekly_result = spark.sql("""
SELECT
    'week' AS period_type,
    m.report_period,
    m.product_category,
    m.product_feature,

    -- Tử số retention
    m.retained_users,

    -- Tử số growth
    m.current_period_users,

    -- Mẫu số dùng chung cho retention và growth
    COALESCE(
        LAG(m.current_period_users) OVER (
            PARTITION BY
                m.product_category,
                m.product_feature
            ORDER BY m.report_period
        ),
        0
    ) AS previous_period_users,

    -- Active user theo kỳ báo cáo và product category
    b.active_users,

    -- Breadth
    b.average_features_per_active_user,
    b.users_with_1_feature,
    b.users_with_2_features,
    b.users_with_3_features,
    b.users_with_4_features,
    b.users_with_5_plus_features

FROM weekly_metrics m

LEFT JOIN weekly_breadth b
    ON m.report_period = b.report_period
    AND m.product_category = b.product_category
""")

weekly_result.createOrReplaceTempView("weekly_result")

# ============================================================
# 7. Monthly user
# ============================================================

monthly_users = spark.sql("""
SELECT DISTINCT
    report_month AS report_period,
    product_category,
    product_feature,
    user_id
FROM product_feature_src
WHERE product_feature IS NOT NULL
""")

monthly_users.createOrReplaceTempView("monthly_users")


# ============================================================
# 8. Monthly user status
# ============================================================

monthly_user_status = spark.sql("""
SELECT
    report_period,
    product_category,
    product_feature,
    user_id,

    -- Kỳ trước mà user sử dụng tính năng
    LAG(report_period) OVER (
        PARTITION BY
            product_category,
            product_feature,
            user_id
        ORDER BY report_period
    ) AS previous_user_period

FROM monthly_users
""")

monthly_user_status.createOrReplaceTempView("monthly_user_status")


# ============================================================
# 9. Monthly metrics
# ============================================================

monthly_metrics = spark.sql("""
SELECT
    report_period,
    product_category,
    product_feature,

    -- Tử số growth: Tổng số người sử dụng tính năng kỳ này
    COUNT(DISTINCT user_id) AS current_period_users,

    -- Tử số retention: User kỳ trước tiếp tục sử dụng kỳ này
    COUNT(DISTINCT CASE
        WHEN previous_user_period = add_months(report_period, -1)
        THEN user_id
    END) AS retained_users

FROM monthly_user_status

GROUP BY
    report_period,
    product_category,
    product_feature
""")

monthly_metrics.createOrReplaceTempView("monthly_metrics")


# ============================================================
# 10. Monthly breadth
# ============================================================

monthly_breadth = spark.sql("""
WITH user_features AS (
    SELECT
        report_period,
        product_category,
        user_id,
        COUNT(DISTINCT product_feature) AS feature_count
    FROM monthly_users
    GROUP BY
        report_period,
        product_category,
        user_id
),

active_users AS (
    SELECT
        report_month AS report_period,
        product_category,
        COUNT(DISTINCT CASE
            WHEN is_active_user = 1 THEN user_id
        END) AS active_users
    FROM product_feature_src
    GROUP BY
        report_month,
        product_category
)

SELECT
    u.report_period,
    u.product_category,

    -- Tổng số feature DISTINCT của từng user
    SUM(u.feature_count) AS total_feature_count,

    -- Số active user, bao gồm user không sử dụng feature
    MAX(a.active_users) AS active_users,

    -- Số tính năng trung bình sử dụng bởi active user
    CAST(SUM(u.feature_count) AS DOUBLE)
        / NULLIF(MAX(a.active_users), 0)
        AS average_features_per_active_user,

    -- Số user theo số lượng feature
    SUM(CASE WHEN u.feature_count = 1 THEN 1 ELSE 0 END)
        AS users_with_1_feature,

    SUM(CASE WHEN u.feature_count = 2 THEN 1 ELSE 0 END)
        AS users_with_2_features,

    SUM(CASE WHEN u.feature_count = 3 THEN 1 ELSE 0 END)
        AS users_with_3_features,

    SUM(CASE WHEN u.feature_count = 4 THEN 1 ELSE 0 END)
        AS users_with_4_features,

    SUM(CASE WHEN u.feature_count >= 5 THEN 1 ELSE 0 END)
        AS users_with_5_plus_features

FROM user_features u

LEFT JOIN active_users a
    ON u.report_period = a.report_period
    AND u.product_category = a.product_category

GROUP BY
    u.report_period,
    u.product_category
""")

monthly_breadth.createOrReplaceTempView("monthly_breadth")


# ============================================================
# 11. Monthly final metrics
# ============================================================

monthly_result = spark.sql("""
SELECT
    'month' AS period_type,
    m.report_period,
    m.product_category,
    m.product_feature,

    -- Tử số retention
    m.retained_users,

    -- Tử số growth
    m.current_period_users,

    -- Mẫu số dùng chung cho retention và growth
    COALESCE(
        LAG(m.current_period_users) OVER (
            PARTITION BY
                m.product_category,
                m.product_feature
            ORDER BY m.report_period
        ),
        0
    ) AS previous_period_users,

    -- Active user theo kỳ báo cáo và product category
    b.active_users,

    -- Breadth
    b.average_features_per_active_user,
    b.users_with_1_feature,
    b.users_with_2_features,
    b.users_with_3_features,
    b.users_with_4_features,
    b.users_with_5_plus_features

FROM monthly_metrics m

LEFT JOIN monthly_breadth b
    ON m.report_period = b.report_period
    AND m.product_category = b.product_category
""")


monthly_result.createOrReplaceTempView("monthly_result")


# ============================================================
# 12. Final data mart
# ============================================================

final_metrics = spark.sql("""
SELECT
    period_type,
    report_period,
    product_category,
    product_feature,

    -- Tử số retention
    retained_users,

    -- Tử số growth
    current_period_users,

    -- Mẫu số dùng chung cho retention và growth
    previous_period_users,

    -- Active user theo kỳ báo cáo và product category
    active_users,

    -- Tỷ lệ duy trì sử dụng tính năng
    CASE
        WHEN previous_period_users = 0 THEN 0
        ELSE CAST(retained_users AS DOUBLE) / previous_period_users
    END AS retention_rate,

    -- Tốc độ tăng trưởng người dùng tính năng
    CASE
        WHEN previous_period_users = 0 THEN 0
        ELSE CAST(current_period_users AS DOUBLE) / previous_period_users
    END AS user_growth_rate,

    -- Tỷ lệ sử dụng tính năng
    CASE
        WHEN active_users = 0 THEN 0
        ELSE CAST(current_period_users AS DOUBLE) / active_users
    END AS feature_usage_rate,

    -- Số tính năng trung bình sử dụng bởi active user
    average_features_per_active_user,

    -- Số user theo số lượng feature
    users_with_1_feature,
    users_with_2_features,
    users_with_3_features,
    users_with_4_features,
    users_with_5_plus_features

FROM weekly_result

UNION ALL

SELECT
    period_type,
    report_period,
    product_category,
    product_feature,

    -- Tử số retention
    retained_users,

    -- Tử số growth
    current_period_users,

    -- Mẫu số dùng chung cho retention và growth
    previous_period_users,

    -- Active user theo kỳ báo cáo và product category
    active_users,

    -- Tỷ lệ duy trì sử dụng tính năng
    CASE
        WHEN previous_period_users = 0 THEN 0
        ELSE CAST(retained_users AS DOUBLE) / previous_period_users
    END AS retention_rate,

    -- Tốc độ tăng trưởng người dùng tính năng
    CASE
        WHEN previous_period_users = 0 THEN 0
        ELSE CAST(current_period_users AS DOUBLE) / previous_period_users
    END AS user_growth_rate,

    -- Tỷ lệ sử dụng tính năng
    CASE
        WHEN active_users = 0 THEN 0
        ELSE CAST(current_period_users AS DOUBLE) / active_users
    END AS feature_usage_rate,

    -- Số tính năng trung bình sử dụng bởi active user
    average_features_per_active_user,

    -- Số user theo số lượng feature
    users_with_1_feature,
    users_with_2_features,
    users_with_3_features,
    users_with_4_features,
    users_with_5_plus_features

FROM monthly_result
""")


# ============================================================
# 13. Write
# ============================================================

final_metrics.write \
    .mode("overwrite") \
    .format("parquet") \
    .option("path", target_path) \
    .saveAsTable(target_table)

spark.catalog.refreshTable(target_table)

print("DONE. Rows:", final_metrics.count())
