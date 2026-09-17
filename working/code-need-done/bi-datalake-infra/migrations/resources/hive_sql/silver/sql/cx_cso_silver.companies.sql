WITH last_cso_companies AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY id
                   ORDER BY updated_at_ts DESC, id DESC
               ) rn
        FROM hive.cx_cso_raw.dim_cso_companies ss
    ) t
    WHERE rn = 1
)

SELECT
    -- ===== core =====
    id,
    name,
    description,
    note,

    -- ===== array =====
--    domains,

    -- ===== datetime string =====
   from_unixtime(created_at_ts) AS created_at,
    from_unixtime(updated_at_ts) AS updated_at,

    -- ===== struct / row =====
    custom_fields.alias as company_alias,
    custom_fields.tax_code as tax_code,
    custom_fields.company_segment as company_segment,
    custom_fields.company_type as company_type,


    -- ===== business fields =====
    health_score,
    account_tier,
    renewal_date,
    industry
--    org_company_id
FROM last_cso_companies
