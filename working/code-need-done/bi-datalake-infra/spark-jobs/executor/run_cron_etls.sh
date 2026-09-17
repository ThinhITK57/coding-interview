#!/bin/bash
set -e

# DIR="$(cd "$(dirname "$0")" && pwd)"
# cd "$DIR"

# Example: docker exec -it spark-client entrypoint.sh shell
# docker exec -it spark-client entrypoint.sh bash


entrypoint.sh run jobs/cx_cso_silver/cso_support_tickets.py '{"dummy_param": true}'
entrypoint.sh run jobs/cx_cso_silver/cso_cx_company.py '{"dummy_param": true}'


entrypoint.sh run jobs/bi_silver/crm_contract_allocations.py '{"dummy_param": true}'
entrypoint.sh run jobs/bi_silver/crm_contracts.py '{"dummy_param": true}'
entrypoint.sh run jobs/bi_silver/crm_deal_interested_products.py '{"dummy_param": true}'
entrypoint.sh run jobs/bi_silver/crm_deal_quotation_products.py '{"dummy_param": true}'
entrypoint.sh run jobs/bi_silver/crm_deal_quotations.py '{"dummy_param": true}'
entrypoint.sh run jobs/bi_silver/crm_deal_reasons.py '{"dummy_param": true}'
entrypoint.sh run jobs/bi_silver/crm_deals.py '{"dummy_param": true}'
entrypoint.sh run jobs/bi_silver/crm_partners.py '{"dummy_param": true}'
entrypoint.sh run jobs/bi_silver/crm_payment_forecast.py '{"dummy_param": true}'
entrypoint.sh run jobs/bi_silver/crm_pricebook.py '{"dummy_param": true}'

entrypoint.sh run jobs/bi_silver/crm_allocated_revenue.py '{"dummy_param": true}'

# entrypoint.sh run jobs/bi_silver/crm_product_category.py '{"dummy_param": true}'
# entrypoint.sh run jobs/bi_silver/crm_product_tree_item_code.py '{"dummy_param": true}'
# entrypoint.sh run jobs/bi_silver/crm_product_tree.py '{"dummy_param": true}'

entrypoint.sh run jobs/bi_silver/crm_sales_accounts.py '{"dummy_param": true}'
entrypoint.sh run jobs/bi_silver/crm_users.py '{"dummy_param": true}'
entrypoint.sh run jobs/bi_silver/cx_cso_support_tickets.py '{"dummy_param": true}'
entrypoint.sh run jobs/bi_silver/cx_cso_ticket_tags.py '{"dummy_param": true}'
entrypoint.sh run jobs/bi_silver/cx_sur_question_answer_choices.py '{"dummy_param": true}'
entrypoint.sh run jobs/bi_silver/cx_sur_question_response.py '{"dummy_param": true}'
entrypoint.sh run jobs/bi_silver/cx_sur_surveys.py '{"dummy_param": true}'
# entrypoint.sh run jobs/bi_silver/cx_cso_customer_contract_lifecycle_snapshot.py '{"dummy_param": true}'

# entrypoint.sh run jobs/bi_silver/dim_customer_segment_l1.py '{"dummy_param": true}'
# entrypoint.sh run jobs/bi_silver/dim_date.py '{"dummy_param": true}'
# entrypoint.sh run jobs/bi_silver/dim_product_category.py '{"dummy_param": true}'
# entrypoint.sh run jobs/bi_silver/dim_territory_name.py '{"dummy_param": true}'
# entrypoint.sh run jobs/bi_silver/dim_unit_level_1.py '{"dummy_param": true}'

# entrypoint.sh run jobs/bi_silver/finance_actual_cost.py '{"dummy_param": true}'

entrypoint.sh run jobs/bi_silver/finance_allocated_revenue.py '{"dummy_param": true}'

# entrypoint.sh run jobs/bi_silver/finance_cash_collection_excel.py '{"dummy_param": true}'

entrypoint.sh run jobs/bi_silver/finance_contract_collected_invoices.py '{"dummy_param": true}'
entrypoint.sh run jobs/bi_silver/finance_cost_plan.py '{"dummy_param": true}'
entrypoint.sh run jobs/bi_silver/finance_fact_ratios.py '{"dummy_param": true}'
entrypoint.sh run jobs/bi_silver/finance_product_revenue_plan.py '{"dummy_param": true}'
entrypoint.sh run jobs/bi_silver/finance_production_cost_allocations.py '{"dummy_param": true}'
entrypoint.sh run jobs/bi_silver/finance_revenue_plan.py '{"dummy_param": true}'
entrypoint.sh run jobs/bi_silver/finance_sales_product_revenue.py '{"dummy_param": true}'

# entrypoint.sh run jobs/bi_silver/hr_employee_headcount.py '{"dummy_param": true}'
# entrypoint.sh run jobs/bi_silver/hr_employee_onboard.py '{"dummy_param": true}'
# entrypoint.sh run jobs/bi_silver/hr_employee_resigned.py '{"dummy_param": true}'
# entrypoint.sh run jobs/bi_silver/jira_task_operation.py '{"dummy_param": true}'
# entrypoint.sh run jobs/bi_silver/noc_entities.py '{"dummy_param": true}'
# entrypoint.sh run jobs/bi_silver/noc_metrics.py '{"dummy_param": true}'