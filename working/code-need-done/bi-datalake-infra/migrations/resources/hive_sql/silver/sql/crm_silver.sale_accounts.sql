WITH last_sales_accounts AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY id
                   ORDER BY updated_at_ts DESC, id DESC
               ) rn
        FROM hive.crm_raw.sales_accounts ss
        WHERE NOT EXISTS (
            SELECT 1
            FROM hive.crm_raw.deleted_sales_accounts d
            WHERE CAST(d.id AS BIGINT) = ss.id
        )
    ) t
    WHERE rn = 1
)

SELECT
    CAST(s.id AS VARCHAR)                          AS sale_id,
    s.name                                         AS name,
    s.address                                      AS address,
    s.city                                         AS city,
    s.state                                        AS state,
    s.zipcode                                      AS zipcode,
    s.country                                      AS country,
    s.number_of_employees                          AS number_of_employees,
    s.annual_revenue                               AS annual_revenue,
    s.website                                      AS website,
    s.owner_id                                     AS user_am_id,
    s.phone                                        AS phone,
    s.open_deals_amount                            AS open_deals_amount,
    s.open_deals_count                             AS open_deals_count,
    s.won_deals_amount                             AS won_deals_amount,
    s.won_deals_count                              AS won_deals_count,
    s.last_contacted                               AS last_contacted,
    s.last_contacted_mode                          AS last_contacted_mode,
    s.facebook                                     AS facebook,
    s.twitter                                      AS twitter,
--    s.linkedin                                     AS linkedin,

    -- links (ROW)
--    s.links.conversations                          AS links_conversations,
--    s.links.document_associations                  AS links_document_associations,
--    s.links.notes                                  AS links_notes,
--    s.links.tasks                                  AS links_tasks,
--    s.links.appointments                           AS links_appointments,

    -- custom_field (ROW)
    s.custom_field.cf_alias                        AS company_alias,
    s.custom_field.cf_tax_code                     AS tax_code,
--    s.custom_field.cf_incorporation_date           AS cf_incorporation_date,
--    s.custom_field.cf_old_birthday                 AS cf_old_birthday,
    s.custom_field.cf_using_soc                    AS is_using_soc,
    s.custom_field.cf_vcs_socothers                AS current_soc_provider,
    s.custom_field.cf_soc_brand                    AS current_soc_brand,
    s.custom_field.cf_date_using_soc               AS first_date_using_soc,
--    s.custom_field.cf_expected_outcome             AS cf_expected_outcome,
--    s.custom_field.cf_service_level                AS cf_service_level,
--    s.custom_field.cf_interested_products          AS cf_interested_products,
--    s.custom_field.cf_product_history              AS cf_product_history,
    s.custom_field.cf_country                      AS country,
    s.custom_field.cf_province                     AS province,
    s.custom_field.cf_group                        AS customer_group,
    s.custom_field.cf_segment                      AS segment_l1,
    s.custom_field.cf_segment2                     AS segment_l2,
    s.custom_field.cf_segment3                     AS segment_l3,
    s.custom_field.cf_initial_source               AS lead_initial_source,
    s.custom_field.cf_warm_up_source               AS lead_warm_up_source,
--    s.custom_field.cf__new_birthday                AS cf__new_birthday,
    s.custom_field.cf__am                          AS user_am_fullname,
    s.custom_field.cf__domain                      AS company_domain,

--    s.created_at                                  AS created_at,
--    s.updated_at                                  AS updated_at,
--    s.avatar                                      AS avatar,
    s.parent_sales_account_id                     AS parent_id,
--    s.recent_note                                 AS recent_note,
--    s.last_contacted_via_sales_activity           AS last_contacted_via_sales_activity,
--    s.last_contacted_sales_activity_mode          AS last_contacted_sales_activity_mode,
--    s.completed_sales_sequences                  AS completed_sales_sequences,
--    s.active_sales_sequences                     AS active_sales_sequences,
    s.last_assigned_at                           AS last_assigned_at,
--    s.is_deleted                                 AS is_deleted,

    -- array fields
--    s.team_user_ids                              AS team_user_ids,
--    s.domains                                    AS domains,
--    s.tags                                       AS tags,

--    s.record_type_id                            AS record_type_id,
--    s.web_form_ids                              AS web_form_ids,
--    s.description                               AS description,
--    s.note                                      AS note,
--    s.health_score                              AS health_score,
--    s.account_tier                              AS account_tier,
    s.renewal_date                              AS renewal_date,

    s.business_type_id                          AS business_type_id,
    bt.name as business_type_name,
    s.industry_type_id                          AS industry_type_id,
    idt.name as industry_type_name

--    s.created_at_ts                             AS created_at_ts,
--    s.updated_at_ts                             AS updated_at_ts,
--    s.last_assigned_at_ts                       AS last_assigned_at_ts,
--    s.last_contacted_via_sales_activity_ts      AS last_contacted_via_sales_activity_ts,
--    s.last_contacted_ts                         AS last_contacted_ts

FROM last_sales_accounts s
left join hive.crm_raw.business_types bt on bt.id = s.business_type_id
left join hive.crm_raw.industry_types idt on idt.id = s.industry_type_id
where is_deleted = false