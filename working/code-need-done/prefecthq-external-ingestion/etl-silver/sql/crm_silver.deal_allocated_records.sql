WITH last_deals AS (
    SELECT
        id,
        custom_field.cf__allocated_records AS allocated_records
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY id
                ORDER BY updated_at_ts DESC, id DESC
            ) AS rn
        FROM hive.crm_raw.deals ss
        WHERE is_deleted = false
          AND NOT EXISTS (
              SELECT 1
              FROM hive.crm_raw.deleted_deals d
              WHERE CAST(d.id AS bigint) = ss.id
          )
    ) t
    WHERE rn = 1
)

SELECT
    t.id                       AS deal_id,

    u.id                       AS record_id,
    u.pid                      AS pid,
    u.name                     AS product_name,

    u.category                 AS category,
    u.territory                AS territory,
    u.recurring                AS is_recurring,

    u.totalVcsValue            AS total_vcs_value,
    u.totalValue               AS total_value,
    u.count                    AS allocation_count,

    u.currency                 AS currency,
    u.period                   AS period,

    u.forecastStart            AS forecast_start_date,
    u.actualStart              AS actual_start_date,

    u.vcsValue                 AS vcs_value,
    u.forecastValue            AS forecast_value,
    u.actualValue              AS actual_value

FROM last_deals t
CROSS JOIN UNNEST (
    CAST(
        COALESCE(
            CAST(json_parse(t.allocated_records) AS json),
            json_parse('[]')
        )
        AS ARRAY(
            ROW(
                id BIGINT,
                pid BIGINT,
                name VARCHAR,

                category VARCHAR,
                territory VARCHAR,
                recurring BOOLEAN,

                totalVcsValue DOUBLE,
                totalValue DOUBLE,
                count INTEGER,

                currency VARCHAR,
                period VARCHAR,

                forecastStart VARCHAR,
                actualStart VARCHAR,

                vcsValue DOUBLE,
                forecastValue DOUBLE,
                actualValue DOUBLE,

                allocationOverrides ARRAY(JSON)
            )
        )
    )
) AS u (
    id,
    pid,
    name,

    category,
    territory,
    recurring,

    totalVcsValue,
    totalValue,
    count,

    currency,
    period,

    forecastStart,
    actualStart,

    vcsValue,
    forecastValue,
    actualValue,

    allocationOverrides
);
