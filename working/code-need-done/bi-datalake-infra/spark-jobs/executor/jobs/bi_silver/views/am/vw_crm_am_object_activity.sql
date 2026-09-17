CREATE OR REPLACE VIEW hive.bi_silver.vw_crm_am_object_activity AS

SELECT
    activity_date,
    am_user_id,
    am_user_name,
    object_type,
    object_id,

    is_created,
    is_updated,
    0 AS is_progressed,
    0 AS is_contacted,
    0 AS is_signed,
    0 AS is_business_outcome

FROM hive.bi_silver.vw_crm_contact_activity

UNION ALL

SELECT
    activity_date,
    am_user_id,
    am_user_name,
    object_type,
    object_id,

    is_created,
    is_updated,
    is_stage_progressed,
    0,
    is_signed,
    0

FROM hive.bi_silver.vw_crm_deal_activity

UNION ALL

SELECT
    activity_date,
    am_user_id,
    am_user_name,
    object_type,
    object_id,

    is_created,
    0,
    0,
    0,
    0,
    0

FROM hive.bi_silver.vw_crm_quotation_activity

UNION ALL

SELECT
    activity_date,
    am_user_id,
    am_user_name,
    object_type,
    object_id,

    is_created,
    is_updated,
    0,
    0,
    is_signed,
    is_first_contract

FROM hive.bi_silver.vw_crm_contract_activity

UNION ALL

SELECT
    activity_date,
    am_user_id,
    am_user_name,
    object_type,
    object_id,

    is_created,
    is_updated,
    0,
    is_contacted,
    0,
    0

FROM hive.bi_silver.vw_crm_account_activity;