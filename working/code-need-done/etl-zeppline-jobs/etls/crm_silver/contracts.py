%livy.pyspark

sql_query = """
WITH last_cm_contracts AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY id
                   ORDER BY updated_at_ts DESC, id DESC
               ) rn
        FROM crm_raw.cm_contracts ss
    ) t
    WHERE rn = 1
)

SELECT
    c.id,
    c.name,
    c.owner_id as user_am_id,

    -- ===== custom_field =====
    c.custom_field.cf_contract_id        AS contract_number,
    c.custom_field.cf_type               AS contract_type,
    c.custom_field.cf_signing_method     AS signing_method,
    c.custom_field.cf_company            AS sale_account_id,
    c.custom_field.cf_alias              AS company_alias,
    c.custom_field.cf_tax_code           AS tax_code,
    c.custom_field.cf_group              AS customer_group,
    c.custom_field.cf_segment            AS segment_l1,
    c.custom_field.cf_segment2           AS segment_l2,
    c.custom_field.cf_segment3           AS segment_l3,
    -- c.custom_field.cf_products_in_contract AS products_in_contract,
    -- c.custom_field.cf_deployed_products  AS deployed_products,
    c.custom_field.cf_status             AS status,
    c.custom_field.cf_sign_date          AS sign_date,
    c.custom_field.cf_fac_date           AS fac_date,
    c.custom_field.cf_duration           AS duration,
    c.custom_field.cf_expire_date        AS expire_date,
    c.custom_field.cf_usd_to_vnd          AS usd_exchange_rate,
--    c.custom_field.cf_doanh_thu           AS cf_doanh_thu,
    c.custom_field.cf_currency           AS currency_code,
    c.custom_field.cf_revenue            AS revenue,
    c.custom_field.cf_vat                AS vat,
    c.custom_field.cf_vcs_revenue         AS vcs_revenue,
    c.custom_field.cf_viettel_revenue     AS viettel_revenue,
    c.custom_field.cf_partner            AS partner_id,
     CAST(c.custom_field.cf_opportunity AS STRING)        AS deal_id
--    c.custom_field.cf_bidding_required   AS cf_bidding_required,
--    c.custom_field.cf__actual             AS cf__actual,
--    c.custom_field.cf__opp_id             AS cf__opp_id,

    -- ===== metadata =====
--    c.created_at,
--    c.creator_id,
--    c.updated_at,
--    c.updater_id,
--    c.avatar,
--    c.recent_note

    -- links
--    c.links.document_associations         AS document_associations,
--    c.links.notes                         AS notes,

--    c.record_type_id,
--    c.created_at_ts,
--    c.updated_at_ts

FROM last_cm_contracts c


"""

df = spark.sql(sql_query)

df.write \
    .mode("overwrite") \
    .format("parquet") \
    .option(
        "path",
        "/opt/datasets/crawlers/vcs_silver/crm_silver/data/contracts"
    ) \
    .saveAsTable("crm_silver.contracts")
