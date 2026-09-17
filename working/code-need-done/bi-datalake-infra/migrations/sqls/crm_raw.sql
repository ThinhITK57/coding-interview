CREATE SCHEMA hive.crm_raw 
with (location='s3a://vcs-raw/crm-raw/');


-- hive.crm_raw.business_types definition

CREATE TABLE hive.crm_raw.business_types (
   id bigint,
   name varchar,
   position bigint,
   partial boolean
)
WITH (
   format = 'PARQUET'
);


-- hive.crm_raw.cm_catalog definition

CREATE TABLE hive.crm_raw.cm_catalog (
   id bigint,
   name varchar,
   owner_id bigint,
   custom_field ROW(cf_category varchar, cf_version varchar, cf_max_discount bigint, cf_item_type varchar, cf_active boolean, cf_market bigint),
   created_at varchar,
   creator_id bigint,
   updated_at varchar,
   updater_id bigint,
   avatar varchar,
   recent_note varchar,
   links ROW(document_associations varchar, notes varchar),
   record_type_id varchar,
   created_at_ts bigint,
   updated_at_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.crm_raw.cm_contracts definition

CREATE TABLE hive.crm_raw.cm_contracts (
   id bigint,
   name varchar,
   owner_id bigint,
   custom_field ROW(cf_contract_id varchar, cf_type varchar, cf_signing_method varchar, cf_company bigint, cf_alias varchar, cf_tax_code varchar, cf_group varchar, cf_segment varchar, cf_segment2 varchar, cf_segment3 varchar, cf_products_in_contract varchar, cf_deployed_products varchar, cf_status varchar, cf_sign_date varchar, cf_fac_date varchar, cf_duration double, cf_expire_date varchar, cf_usd_to_vnd double, cf_revenue double, cf_currency varchar, cf_vat bigint, cf_vcs_revenue bigint, cf_viettel_revenue bigint, cf_partner varchar, cf_opportunity bigint, cf_bidding_required boolean, cf__actual boolean, cf__opp_id varchar),
   created_at varchar,
   creator_id bigint,
   updated_at varchar,
   updater_id bigint,
   avatar varchar,
   recent_note varchar,
   links ROW(document_associations varchar, notes varchar),
   record_type_id varchar,
   crawled_at_ts bigint,
   created_at_ts bigint,
   updated_at_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.crm_raw.cm_kpi definition

CREATE TABLE hive.crm_raw.cm_kpi (
   id bigint,
   name varchar,
   owner_id bigint,
   custom_field ROW(cf_kpi_scope varchar, cf_currency varchar, cf_relate_kpi bigint, cf_sale_target varchar, cf_revenue_target varchar, cf_sale_target_quarter_1 bigint, cf_sale_target_month_1 bigint, cf_sale_target_month_2 bigint, cf_sale_target_month_3 bigint, cf_revenue_target_quarter_1 bigint, cf_revenue_target_month_1 bigint, cf_revenue_target_month_2 bigint, cf_revenue_target_month_3 bigint, cf_sale_target_quarter_2 bigint, cf_sale_target_month_4 bigint, cf_sale_target_month_5 bigint, cf_sale_target_month_6 bigint, cf_revenue_target_quarter_2 bigint, cf_revenue_target_month_4 bigint, cf_revenue_target_month_5 bigint, cf_revenue_target_month_6 bigint, cf_sale_target_quarter_3 bigint, cf_sale_target_month_7 bigint, cf_sale_target_month_8 bigint, cf_sale_target_month_9 bigint, cf_revenue_target_quarter_3 bigint, cf_revenue_target_month_7 bigint, cf_revenue_target_month_8 bigint, cf_revenue_target_month_9 bigint, cf_sale_target_quarter_4 bigint, cf_sale_target_month_10 bigint, cf_sale_target_month_11 bigint, cf_sale_target_month_12 bigint, cf_revenue_target_quarter_4 bigint, cf_revenue_target_month_10 bigint, cf_revenue_target_month_11 bigint, cf_revenue_target_month_12 bigint),
   created_at varchar,
   creator_id bigint,
   updated_at varchar,
   updater_id bigint,
   avatar varchar,
   recent_note varchar,
   links ROW(document_associations varchar, notes varchar),
   record_type_id varchar,
   created_at_ts bigint,
   updated_at_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.crm_raw.cm_partners definition

CREATE TABLE hive.crm_raw.cm_partners (
   id bigint,
   name varchar,
   owner_id bigint,
   custom_field ROW(cf_document_number varchar, cf_alias varchar, cf_address varchar, cf_country varchar, cf_contact_name varchar, cf_contact_position varchar, cf_contact_mobile varchar, cf_contact_email varchar, cf_effective_date varchar, cf_expiration_date varchar, cf_type varchar, cf_document_format varchar, cf_status varchar),
   created_at varchar,
   creator_id bigint,
   updated_at varchar,
   updater_id bigint,
   avatar varchar,
   recent_note varchar,
   links ROW(document_associations varchar, notes varchar),
   record_type_id varchar,
   created_at_ts bigint,
   updated_at_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.crm_raw.cm_pricebook definition

CREATE TABLE hive.crm_raw.cm_pricebook (
   id bigint,
   name varchar,
   owner_id bigint,
   custom_field ROW(cf_sku varchar, cf_type varchar, cf_sub_type varchar, cf_license varchar, cf_price_type varchar, cf_package varchar, cf_price double, cf_currency varchar, cf_min bigint, cf_max bigint, cf_unit varchar, cf_is_quantity_based boolean, cf_is_related_csmp boolean, cf_csmp_discount bigint, cf_csmp_discount_silver bigint, cf_csmp_discount_gold bigint, cf_csmp_discount_diamond bigint, cf_catalog bigint, cf_related_pricebook bigint),
   created_at varchar,
   creator_id bigint,
   updated_at varchar,
   updater_id bigint,
   avatar varchar,
   recent_note varchar,
   links ROW(document_associations varchar, notes varchar),
   record_type_id varchar,
   created_at_ts bigint,
   updated_at_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.crm_raw.contact_statuses definition

CREATE TABLE hive.crm_raw.contact_statuses (
   id bigint,
   name varchar,
   position bigint,
   partial boolean,
   forecast_type varchar,
   lifecycle_stage_id bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.crm_raw.contacts definition

CREATE TABLE hive.crm_raw.contacts (
   id bigint,
   first_name varchar,
   last_name varchar,
   display_name varchar,
   avatar varchar,
   job_title varchar,
   city varchar,
   state varchar,
   zipcode varchar,
   country varchar,
   email varchar,
   emails array(ROW(id bigint, value varchar, is_primary boolean, label varchar, _destroy boolean)),
   time_zone varchar,
   work_number varchar,
   mobile_number varchar,
   address varchar,
   last_seen varchar,
   lead_score bigint,
   last_contacted varchar,
   open_deals_amount varchar,
   won_deals_amount varchar,
   links ROW(conversations varchar, timeline_feeds varchar, document_associations varchar, notes varchar, tasks varchar, appointments varchar, reminders varchar, duplicates varchar, connections varchar),
   last_contacted_sales_activity_mode varchar,
   custom_field ROW(cf_birthday varchar, cf_old_birthday varchar, cf_sex varchar, cf_level_of_interest varchar, cf_level_of_support varchar, cf_presales varchar, cf_partner bigint, cf__new_birthday varchar, cf__company varchar, cf__am varchar, cf__activities varchar, cf_warm_up_source varchar, cf_initial_source varchar, cf_center varchar, cf_job_title varchar),
   created_at varchar,
   updated_at varchar,
   keyword varchar,
   medium varchar,
   last_contacted_mode varchar,
   recent_note varchar,
   won_deals_count bigint,
   last_contacted_via_sales_activity varchar,
   completed_sales_sequences varchar,
   active_sales_sequences varchar,
   web_form_ids varchar,
   open_deals_count bigint,
   last_assigned_at varchar,
   facebook varchar,
   twitter varchar,
   linkedin varchar,
   is_deleted boolean,
   team_user_ids array(bigint),
   external_id varchar,
   work_email varchar,
   subscription_status bigint,
   subscription_types varchar,
   unsubscription_reason varchar,
   other_unsubscription_reason varchar,
   customer_fit bigint,
   record_type_id varchar,
   whatsapp_subscription_status bigint,
   sms_subscription_status bigint,
   last_seen_chat varchar,
   first_seen_chat varchar,
   locale varchar,
   total_sessions varchar,
   system_tags array(varchar),
   first_campaign varchar,
   first_medium varchar,
   first_source varchar,
   last_campaign varchar,
   last_medium varchar,
   last_source varchar,
   latest_campaign varchar,
   latest_medium varchar,
   latest_source varchar,
   mcr_id bigint,
   description varchar,
   amb_subscription_status varchar,
   phone_numbers array(varchar),
   tags array(varchar),
   sales_account_id bigint,
   owner_id bigint,
   contact_status_id bigint,
   crawled_at_ts bigint,
   created_at_ts bigint,
   updated_at_ts bigint,
   last_assigned_at_ts bigint,
   last_contacted_via_sales_activity_ts bigint,
   last_contacted_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.crm_raw.currencies definition

CREATE TABLE hive.crm_raw.currencies (
   partial boolean,
   id bigint,
   is_active boolean,
   currency_code varchar,
   exchange_rate varchar,
   currency_type bigint,
   schedule_info varchar
)
WITH (
   format = 'PARQUET'
);


-- hive.crm_raw.deal_payment_statuses definition

CREATE TABLE hive.crm_raw.deal_payment_statuses (
   id bigint,
   name varchar,
   position bigint,
   partial boolean
)
WITH (
   format = 'PARQUET'
);


-- hive.crm_raw.deal_pipelines definition

CREATE TABLE hive.crm_raw.deal_pipelines (
   partial boolean,
   id bigint,
   name varchar,
   position bigint,
   is_default boolean,
   rotting_days bigint,
   configs array(ROW(field_name varchar, position bigint, highlight boolean)),
   aggregated_field varchar,
   deal_stages array(ROW(id bigint, value varchar, name varchar, position bigint, forecast_type varchar, deal_pipeline_id bigint, choice_type bigint, is_deleted boolean, probability bigint, updated_at varchar)),
   crawled_at_ts  bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.crm_raw.deal_reasons definition

CREATE TABLE hive.crm_raw.deal_reasons (
   id bigint,
   name varchar,
   position bigint,
   partial boolean
)
WITH (
   format = 'PARQUET'
);


-- hive.crm_raw.deal_stages definition

CREATE TABLE hive.crm_raw.deal_stages (
   partial boolean,
   id bigint,
   name varchar,
   position bigint,
   forecast_type varchar,
   updated_at varchar,
   deal_pipeline_id bigint,
   choice_type bigint,
   probability bigint,
   updated_at_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.crm_raw.deal_types definition

CREATE TABLE hive.crm_raw.deal_types (
   id bigint,
   name varchar,
   position bigint,
   partial boolean
)
WITH (
   format = 'PARQUET'
);


-- hive.crm_raw.deals definition

CREATE TABLE hive.crm_raw.deals (
   id bigint,
   name varchar,
   amount varchar,
   base_currency_amount varchar,
   expected_close varchar,
   closed_date varchar,
   stage_updated_time varchar,
   custom_field ROW(cf_lock_revenue boolean, cf_usd_to_vnd double, cf_interested_products varchar, cf_get_live_date varchar, cf_periodicity varchar, cf_select_viettel boolean, cf_has_budget boolean, cf_alias varchar, cf_budget bigint, cf_group varchar, cf_segment varchar, cf_segment2 varchar, cf_segment3 varchar, cf_channel varchar, cf_weight varchar, cf_bidding_required boolean, cf__allocated varchar, cf__added varchar, cf_presales varchar, cf_project_manager varchar, cf_sale_admin varchar, cf_partner bigint, cf_contract bigint, cf_initial_source varchar, cf_warm_up_source varchar, cf__update_mss boolean, cf__contact varchar, cf__company varchar, cf__address varchar, cf__province varchar, cf__country varchar, cf__email varchar, cf__phone varchar, cf__currency varchar, cf__global_discount varchar, cf__deal_type varchar, cf__check_change boolean, cf__am varchar, cf__fac_date varchar, cf__quotation_status varchar, cf__quotations varchar, cf__expire_date varchar, cf__duration bigint, cf__territory varchar, cf__create_contract boolean, cf__products varchar, cf__allocated_products varchar, cf__allocated_records varchar),
   probability bigint,
   updated_at varchar,
   created_at varchar,
   deal_pipeline_id bigint,
   deal_stage_id bigint,
   age bigint,
   links ROW(conversations varchar, document_associations varchar, notes varchar, tasks varchar, appointments varchar),
   recent_note varchar,
   completed_sales_sequences varchar,
   active_sales_sequences varchar,
   web_form_id varchar,
   upcoming_activities_time varchar,
   collaboration ROW(_dummy_ bigint),
   last_assigned_at varchar,
   last_contacted_sales_activity_mode varchar,
   last_contacted_via_sales_activity varchar,
   expected_deal_value varchar,
   is_deleted boolean,
   team_user_ids array(bigint),
   avatar varchar,
   fc_widget_collaboration ROW(convo_token varchar, auth_token varchar, encoded_jwt_token varchar),
   forecast_category bigint,
   deal_prediction bigint,
   deal_prediction_last_updated_at varchar,
   record_type_id varchar,
   freddy_forecast_metrics varchar,
   last_deal_prediction bigint,
   has_products boolean,
   products array(varchar),
   deal_price_adjustments array(varchar),
   rotten_days varchar,
   tags array(varchar),
   owner_id bigint,
   sales_account_id bigint,
   deal_type_id bigint,
   deal_reason_id bigint,
   currency_id bigint,
   deal_payment_status_id bigint,
   crawled_at_ts bigint,
   expected_close_ts bigint,
   closed_date_ts bigint,
   stage_updated_time_ts bigint,
   updated_at_ts bigint,
   created_at_ts bigint,
   last_assigned_at_ts bigint,
   deal_prediction_last_updated_at_ts bigint,
   upcoming_activities_time_ts bigint,
   last_contacted_via_sales_activity_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.crm_raw.deleted_cm_pricebook definition

CREATE TABLE hive.crm_raw.deleted_cm_pricebook (
   id varchar
)
WITH (
   format = 'PARQUET'
);


-- hive.crm_raw.deleted_deals definition

CREATE TABLE hive.crm_raw.deleted_deals (
   id varchar
)
WITH (
   format = 'PARQUET'
);


-- hive.crm_raw.deleted_sales_accounts definition

CREATE TABLE hive.crm_raw.deleted_sales_accounts (
   id varchar
)
WITH (
   format = 'PARQUET'
);


-- hive.crm_raw.industry_types definition

CREATE TABLE hive.crm_raw.industry_types (
   id bigint,
   name varchar,
   position bigint,
   partial boolean
)
WITH (
   format = 'PARQUET'
);


-- hive.crm_raw.product_tree definition

CREATE TABLE hive.crm_raw.product_tree (
   category varchar,
   version varchar,
   product_type varchar,
   sub_type varchar,
   license varchar,
   price_type varchar,
   deployment_type varchar
)
WITH (
   format = 'PARQUET'
);


-- hive.crm_raw.roles definition

CREATE TABLE hive.crm_raw.roles (
   id bigint,
   name varchar,
   created_by varchar,
   updated_by varchar,
   updated_at varchar,
   created_at varchar,
   default_role boolean,
   addons array(varchar),
   internal_name varchar,
   licensed_users_count bigint,
   user_ids array(bigint),
   updated_at_ts bigint,
   created_at_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.crm_raw.sales_accounts definition

CREATE TABLE hive.crm_raw.sales_accounts (
   id bigint,
   name varchar,
   address varchar,
   city varchar,
   state varchar,
   zipcode varchar,
   country varchar,
   number_of_employees bigint,
   annual_revenue varchar,
   website varchar,
   owner_id bigint,
   phone varchar,
   open_deals_amount varchar,
   open_deals_count bigint,
   won_deals_amount varchar,
   won_deals_count bigint,
   last_contacted varchar,
   last_contacted_mode varchar,
   facebook varchar,
   twitter varchar,
   linkedin varchar,
   links ROW(conversations varchar, document_associations varchar, notes varchar, tasks varchar, appointments varchar),
   custom_field ROW(cf_alias varchar, cf_tax_code varchar, cf_incorporation_date varchar, cf_old_birthday varchar, cf_using_soc varchar, cf_vcs_socothers varchar, cf_soc_brand varchar, cf_date_using_soc varchar, cf_expected_outcome varchar, cf_service_level varchar, cf_interested_products varchar, cf_product_history varchar, cf_country varchar, cf_province varchar, cf_group varchar, cf_segment varchar, cf_segment2 varchar, cf_segment3 varchar, cf_initial_source varchar, cf_warm_up_source varchar, cf__new_birthday varchar, cf__am varchar, cf__domain varchar),
   created_at varchar,
   updated_at varchar,
   avatar varchar,
   parent_sales_account_id bigint,
   recent_note varchar,
   last_contacted_via_sales_activity varchar,
   last_contacted_sales_activity_mode varchar,
   completed_sales_sequences varchar,
   active_sales_sequences varchar,
   last_assigned_at varchar,
   is_deleted boolean,
   team_user_ids array(bigint),
   record_type_id varchar,
   web_form_ids varchar,
   description varchar,
   note varchar,
   health_score varchar,
   account_tier varchar,
   renewal_date varchar,
   domains array(varchar),
   tags array(varchar),
   business_type_id bigint,
   industry_type_id bigint,
   created_at_ts bigint,
   updated_at_ts bigint,
   last_assigned_at_ts bigint,
   last_contacted_via_sales_activity_ts bigint,
   last_contacted_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.crm_raw.st_users definition

CREATE TABLE hive.crm_raw.st_users (
   id bigint,
   display_name varchar,
   email varchar,
   is_active boolean,
   work_number varchar,
   mobile_number varchar,
   confirmed boolean,
   privileges varchar,
   deal_pipeline_id bigint,
   job_title varchar,
   language varchar,
   last_login_at varchar,
   time_zone varchar,
   avatar varchar,
   signature varchar,
   email_tracking boolean,
   access_scope varchar,
   abilities array(varchar),
   auto_create_entity bigint,
   email_association boolean,
   reply_to varchar,
   "from" varchar,
   email_deal_association boolean,
   freshchat_restore_id varchar,
   is_forgotten boolean,
   team_ids array(bigint),
   uuid varchar,
   email_mailbox_ids array(bigint),
   reports_to_id varchar,
   created_at varchar,
   updated_at varchar,
   role_id varchar,
   user_access_type varchar,
   meta ROW(_dummy_ bigint),
   last_login_at_ts bigint,
   created_at_ts bigint,
   updated_at_ts bigint
)
WITH (
   format = 'PARQUET'
);


-- hive.crm_raw.territories definition

CREATE TABLE hive.crm_raw.territories (
   id bigint,
   name varchar,
   position bigint,
   parent_territory_id varchar,
   tree_id varchar,
   level_id varchar
)
WITH (
   format = 'PARQUET'
);


-- hive.crm_raw.users definition

CREATE TABLE hive.crm_raw.users (
   id bigint,
   display_name varchar,
   email varchar,
   is_active boolean,
   work_number varchar,
   mobile_number varchar
)
WITH (
   format = 'PARQUET'
);