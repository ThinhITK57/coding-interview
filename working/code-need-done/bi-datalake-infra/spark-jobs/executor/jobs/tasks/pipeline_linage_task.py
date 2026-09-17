import time
import subprocess
import json
from datetime import datetime
from prefect import get_run_logger, task
import os
from utils.spark_linage_visitor import *
from utils.om_client import OpenMetadataSDK

TARGET_DATABASE_NAME = "GenBI-DB.hive."
TARGET_PIPELINE_NAME = "prefecthq_minio"


# ===== JOB LIST =====
LINEAGE_JOBS = [
    "jobs/crm_silver/deal_products.py",
    "jobs/crm_silver/deal_allocated_products.py",
    
    "jobs/bi_silver/crm_partners.py",
    "jobs/bi_silver/crm_pricebook.py",
    "jobs/bi_silver/crm_contracts.py",
    "jobs/bi_silver/crm_deals.py",
    "jobs/bi_silver/crm_users.py",
    
    "jobs/bi_silver/crm_revenue_reconciliation.py",
    "jobs/bi_silver/crm_company_contacts.py",
    "jobs/bi_silver/crm_deal_interested_products.py",
    "jobs/bi_silver/crm_deal_quotation_products.py",
    "jobs/bi_silver/crm_deal_quotations.py",
    "jobs/bi_silver/crm_deal_reasons.py",
    
    "jobs/bi_silver/crm_expected_revenue.py",
    "jobs/bi_silver/crm_committed_revenue.py",

    "jobs/bi_silver/crm_product_category.py",
    "jobs/bi_silver/crm_product_tree_item_code.py",
    "jobs/bi_silver/crm_product_tree.py",

    "jobs/bi_silver/crm_sales_accounts.py",

    "jobs/bi_silver/crm_business_rules.py",
    "jobs/bi_silver/crm_activity_history.py",
    
    "jobs/bi_silver/crm_am_activity_daily.py",
    
    "jobs/bi_silver/crm_mart_estimated_revenue.py",
    "jobs/bi_silver/crm_mart_revenue_performance_month.py",

    "jobs/cx_cso_silver/cso_support_tickets.py",
    "jobs/cx_cso_silver/cso_cx_company.py",
    "jobs/bi_silver/cx_cso_support_tickets.py",
    "jobs/bi_silver/cx_cso_ticket_tags.py",
    "jobs/bi_silver/cx_sur_surveys.py",
    "jobs/bi_silver/cx_sur_question_answer_choices.py",
    "jobs/bi_silver/cx_sur_question_response.py",

    "jobs/bi_silver/cx_cso_customer_contract_lifecycle_snapshot.py",
    "jobs/bi_silver/cx_lifecycle_customer_contract_snapshot.py",
    "jobs/bi_silver/cx_lifecycle_customer_product_snapshot.py",
    "jobs/bi_silver/cx_lifecycle_customer_snapshot.py",
    
    "jobs/bi_silver/cx_sur_mart_question_responses.py",
    "jobs/bi_silver/cx_sur_mart_customer_experience.py",
    "jobs/bi_silver/cx_cso_mart_daily_ticket_summary.py",

    "jobs/bi_silver_snapshot/cx_cso_mart_daily_ticket_summary_snapshot.py",
    "jobs/bi_silver_snapshot/cx_cso_support_ticket_ttr_snapshot.py",

    # "jobs/bi_silver/dim_customer_segment_l1.py",
    # "jobs/bi_silver/dim_date.py",
    # "jobs/bi_silver/dim_product_category.py",
    # "jobs/bi_silver/dim_territory_name.py",
    # "jobs/bi_silver/dim_unit_level_1.py",

    "jobs/bi_silver/finance_actual_cost.py",
    
    "jobs/bi_silver/finance_allocated_revenue.py",
    
    # "jobs/bi_silver/finance_cash_collection_excel.py",
    
    "jobs/bi_silver/finance_contract_collected_invoices.py",
    "jobs/bi_silver/finance_cost_plan.py",
    "jobs/bi_silver/finance_fact_ratios.py",
    "jobs/bi_silver/finance_product_revenue_plan.py",
    "jobs/bi_silver/finance_production_cost_allocations.py",
    "jobs/bi_silver/finance_revenue_plan.py",
    # "jobs/bi_silver/finance_sales_product_revenue.py",
    "jobs/bi_silver/finance_revenue_cost_items.py",
    "jobs/bi_silver/finance_sales_revenue.py",
    "jobs/bi_silver/finance_daily_business_metrics.py",
    "jobs/bi_silver/finance_metrics.py",

    "jobs/bi_silver/hr_employee_onboard_snapshot.py",
    "jobs/bi_silver/hr_employee_resigned_snapshot.py",
    "jobs/bi_silver/hr_headcount_demand_snapshot.py",
    "jobs/bi_silver/hr_employee_headcount.py",
    "jobs/bi_silver/hr_employee_onboard.py",
    "jobs/bi_silver/hr_employee_resigned.py",
    "jobs/bi_silver/hr_workforce_monthly.py",
    "jobs/bi_silver/hr_performance_rate.py",
    "jobs/bi_silver/hr_workforce_monthly_drilldown.py",
    "jobs/bi_silver/hr_workforce_monthly_ytd.py",


    "jobs/bi_silver/hr_ld_budget_expense.py",
    "jobs/bi_silver/hr_ld_budget_quarterly.py",
    "jobs/bi_silver/hr_ld_budget_detail.py",
    "jobs/bi_silver/hr_ld_class_list.py",
    "jobs/bi_silver/hr_ld_employee_certification.py",
    "jobs/bi_silver/hr_ld_employee_elearning_summary.py",
    "jobs/bi_silver/hr_ld_fresher.py",
    "jobs/bi_silver/hr_ld_fresher_conversion.py",
    "jobs/bi_silver/hr_ld_fresher_resignation.py",
    "jobs/bi_silver/hr_ld_instructor.py",
    "jobs/bi_silver/hr_ld_learner.py",
    "jobs/bi_silver/hr_ld_trainee.py",
    "jobs/bi_silver/hr_ld_trainee_conversion.py",
    "jobs/bi_silver/hr_ld_trainee_resignation.py",



    # "jobs/bi_silver/jira_task_operation.py",
    # "jobs/bi_silver/noc_entities.py",
    # "jobs/bi_silver/noc_metrics.py",
]



@task
def import_table_linage(file_name: str, job_name: str, om: OpenMetadataSDK):
    logger = get_run_logger()
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    payload = {
        "name": job_name,
        "displayName": job_name,
        "service": TARGET_PIPELINE_NAME,
        "description": f"{now_str}: {file_name }",
        "sourceUrl": "https://prefect-internal.viettelcyber.com:9000/deployments/deployment/31278adc-0527-4011-94fc-30f7d15ecfa2?tab=Runs"
    }
    
    res = om.pipelines.create(payload)
    
    pipeline_id = res["id"]
    logger.info(f"pipeline_id = {pipeline_id}")
    
    with open(file_name, 'r', encoding="utf-8") as f:
        tree = ast.parse(f.read())

        visitor = SparkLineageVisitor()
        visitor.visit(tree)

        for edge in visitor.get_lineage():
            source_id = om.tables.safe_get_table_id(
                TARGET_DATABASE_NAME + edge["from"]
            )

            target_id = om.tables.safe_get_table_id(
                TARGET_DATABASE_NAME + edge["to"]
            )
            edge_type = edge['type']
            description = f"""
            {now_str}:{job_name}
            Spark ETL JOB ({edge_type})
            """
            try:
                om.lineage.create_edge(
                    source_id,
                    target_id,
                    pipeline_id=pipeline_id,
                    description=description
                )
            except Exception as e:
                logger.error(f"{ edge } - {str(e)}, ")
                # raise e


@task
def import_spark_linages(items, base_dir):
    logger = get_run_logger()
    logger.info(f"base_dir = {base_dir}")
    om = OpenMetadataSDK()
    
    
    for job_name in LINEAGE_JOBS:
        file_name = base_dir +"/" + job_name
        logger.info(f"Processing {file_name}")
        try:
            import_table_linage(file_name, job_name, om)
            logger.info(f"Successful {file_name}")
        except Exception as e:
            logger.error(f"Failed to upload {file_name}", e)