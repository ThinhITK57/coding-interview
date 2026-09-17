WITH last_deals AS (
    SELECT
        id,
        custom_field.cf__allocated_products AS allocated_products
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
    t.id                        AS deal_id,

    u.id                        AS product_id,
    u.pid                       AS pid,
    u.name                      AS product_name,

    u.allocationValue           AS allocation_value,
    u.allocationDuration        AS allocation_duration,

    u.forecastDate              AS forecast_date,
    u.actualDate                AS actual_date,

    u.coefficient               AS coefficient,
    u.type                      AS allocation_type,

    u.productType               AS product_type,
    u.spdvType                  AS spdv_type,

    u.region                    AS region,
    u.currency                  AS currency
FROM last_deals t
CROSS JOIN UNNEST (
    CAST(
        COALESCE(
            CAST(json_parse(t.allocated_products) AS json),
            json_parse('[]')
        ) AS ARRAY(
            ROW(
                id BIGINT,
                pid BIGINT,
                name VARCHAR,

                allocationValue DOUBLE,
                allocationDuration INTEGER,

                forecastDate VARCHAR,
                actualDate VARCHAR,

                coefficient VARCHAR,
                type VARCHAR,

                productType VARCHAR,
                spdvType VARCHAR,

                region VARCHAR,
                currency VARCHAR
            )
        )
    )
) AS u( 
	 id ,
                pid ,
                name ,

                allocationValue ,
                allocationDuration ,

                forecastDate ,
                actualDate ,

                coefficient ,
                type ,

                productType ,
                spdvType ,

                region ,
                currency 
      );
