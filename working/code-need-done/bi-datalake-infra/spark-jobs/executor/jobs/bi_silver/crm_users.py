# %livy.pyspark
tgt_table = "bi_silver.crm_users"
tgt_path  = "s3a://bi-silver/crm_users"


spark.catalog.clearCache()

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
spark.sql("REFRESH TABLE crm_raw.st_users")


# spark.sql("DROP TABLE IF EXISTS bi_silver.crm_users")

query = """
WITH territory_mapping (idd, territory_name) AS (
    SELECT *
    FROM (
        VALUES
            (50000302741, 'Miền Bắc'),
            (50000302740, 'Miền Bắc'),
            (50000302739, 'Miền Bắc'),
            (50000301728, 'Miền Bắc'),
            (50000301638, 'Miền Bắc'),
            (50000300774, 'Philippines'),
            (50000300394, 'Miền Nam'),
            (50000300287, 'Miền Nam'),
            (50000300234, 'Miền Bắc'),
            (50000300107, 'Miền Bắc'),
            (50000300106, 'Miền Bắc'),
            (50000299974, 'Japan'),
            (50000294183, 'Philippines'),
            (50000294178, 'Miền Nam'),
            (50000293605, 'Global Sale'),
            (50000293595, 'Philippines'),
            (50000293594, 'Philippines'),
            (50000293592, 'Global Sale'),
            (50000293591, 'Global Sale'),
            (50000292996, 'Miền Bắc'),
            (50000291833, 'Miền Nam'),
            (50000291832, 'Miền Nam'),
            (50000291831, 'Miền Nam'),
            (50000291830, 'Miền Nam'),
            (50000291828, 'Miền Bắc'),
            (50000291827, 'Miền Bắc'),
            (50000291825, 'Miền Bắc'),
            (50000291824, 'Miền Bắc'),
            (50000291823, 'Miền Bắc'),
            (50000291821, 'Miền Bắc'),
            (50000291820, 'Miền Bắc'),
            (50000291818, 'Miền Bắc'),
            (50000291817, 'Miền Bắc'),
            (50000291816, 'Miền Bắc'),
            (50000291815, 'Miền Bắc'),
            (50000291813, 'Miền Bắc')
    ) AS t (id, territories)
),
user_teams AS (
    SELECT
        u.id AS user_id,
        team_id
    FROM crm_raw.st_users u
    LATERAL VIEW explode(u.team_ids) exploded_team_ids AS team_id
),

role_users AS (
    SELECT
        user_id,
        r.id AS role_id,
        r.name AS role_name
    FROM crm_raw.roles r
    LATERAL VIEW explode(r.user_ids) exploded_user_ids AS user_id
),

user_base AS (
    SELECT
        u.id AS user_id,
        u.email,
        u.display_name,
        u.is_active,
        u.job_title,
        ru.role_name,
        concat_ws(';', sort_array(collect_set(t.name))) AS team_name,
        MAX(CAST(u.last_login_at AS TIMESTAMP)) AS last_login_at
    FROM crm_raw.st_users u
    LEFT JOIN role_users ru
        ON u.id = ru.user_id
    LEFT JOIN user_teams ut
        ON u.id = ut.user_id
    LEFT JOIN crm_raw.teams t
        ON ut.team_id = t.id
    GROUP BY
        u.id,
        u.email,
        u.display_name,
        u.is_active,
        u.job_title,
        ru.role_name
)

SELECT
    user_id,
    email,
    display_name,
    is_active,
    job_title,
    role_name,
    team_name,
    last_login_at,

    CASE
        WHEN LOWER(TRIM(team_name)) = 'unnamed'
             AND LOWER(TRIM(job_title)) = 'am unnamed account'
            THEN 'UNNAMED'

        WHEN LOWER(TRIM(team_name)) = 'trọng điểm'
             OR LOWER(TRIM(job_title)) = 'am vvip'
            THEN 'VVIP'

        WHEN LOWER(TRIM(job_title)) IN (
                'am kênh',
                'am kinh doanh kênh'
             )
             OR LOWER(TRIM(team_name)) = 'kênh'
            THEN 'CHANNEL'

        WHEN LOWER(TRIM(job_title)) = 'am nội bộ'
             OR LOWER(TRIM(team_name)) = 'nội bộ'
            THEN 'INTERNAL'

        WHEN LOWER(TRIM(job_title)) IN (
                'am',
                'bdm',
                'global business development'
             )
            THEN 'INTERNATIONAL'

        ELSE 'OTHER'
    END AS am_group,
    COALESCE(tm.territory_name, 'Others') as territory_name

FROM user_base a
LEFT JOIN territory_mapping tm   ON a.user_id = tm.idd
"""

df = spark.sql(query)

# 2) Write
df.write \
    .mode("overwrite") \
    .format("parquet") \
    .save(tgt_path)
    
#  .save(tgt_path)
# .option("path", tgt_path) \
#     .saveAsTable(tgt_table)

spark.catalog.refreshTable(tgt_table)

print(tgt_table)
print("Rows:", df.count())

