import subprocess
import json
from datetime import datetime
from utils.job_logger import JobLogger
from prefect import task, flow, get_run_logger
from tasks.pipeline_linage_task import import_spark_linages
from dateutil.relativedelta import relativedelta
from zoneinfo import ZoneInfo
from tasks.pipeline_snapshot_task import run_snapshot_task
from prefect.schedules import Cron



# ===== JOB LIST =====
JOBS = [
    "jobs/bi_silver/crm_users.py",
    
    "jobs/crm_silver/deal_products.py",
    "jobs/crm_silver/deal_allocated_products.py",

    "jobs/bi_silver/crm_partners.py",
    "jobs/bi_silver/crm_pricebook.py",
    "jobs/bi_silver/crm_contracts.py",
    "jobs/bi_silver/crm_deals.py",
    "jobs/bi_silver/crm_contacts.py",
    
    "jobs/bi_silver/crm_revenue_plan_month.py",
    "jobs/bi_silver/crm_revenue_reconciliation.py",
    "jobs/bi_silver/crm_company_contacts.py",
    "jobs/bi_silver/crm_deal_interested_products.py",
    "jobs/bi_silver/crm_deal_quotation_products.py",
    "jobs/bi_silver/crm_deal_quotations.py",
    "jobs/bi_silver/crm_deal_reasons.py",
    
    "jobs/bi_silver/crm_expected_revenue.py",
    "jobs/bi_silver/crm_committed_revenue.py",

    # # "jobs/bi_silver/crm_product_category.py",
    # # "jobs/bi_silver/crm_product_tree_item_code.py",
    # # "jobs/bi_silver/crm_product_tree.py",

    "jobs/bi_silver/crm_sales_accounts.py",

    "jobs/bi_silver/crm_business_rules.py",
    "jobs/bi_silver/crm_activity_history.py",
    
    "jobs/bi_silver/crm_am_activity_daily.py",
    
    "jobs/bi_silver/crm_mart_estimated_revenue.py",
    "jobs/bi_silver/crm_mart_revenue_performance_month.py",

    # Mart_deals_job
    "jobs/crm_silver/crm_mart_deals.py",

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

    # "jobs/bi_silver/dim_customer_segment_l1.py",
    # "jobs/bi_silver/dim_date.py",
    # "jobs/bi_silver/dim_product_category.py",
    # "jobs/bi_silver/dim_territory_name.py",
    # "jobs/bi_silver/dim_unit_level_1.py",

    # "jobs/bi_silver/finance_actual_cost.py",
    
    # "jobs/bi_silver/finance_allocated_revenue.py",
    
    # "jobs/bi_silver/finance_cash_collection_excel.py",
    
    # "jobs/bi_silver/finance_contract_collected_invoices.py",
    # "jobs/bi_silver/finance_cost_plan.py",
    # "jobs/bi_silver/finance_fact_ratios.py",
    # "jobs/bi_silver/finance_product_revenue_plan.py",
    # "jobs/bi_silver/finance_production_cost_allocations.py",
    # "jobs/bi_silver/finance_revenue_plan.py",
    # # "jobs/bi_silver/finance_sales_product_revenue.py",
    # "jobs/bi_silver/finance_revenue_cost_items.py",
    # "jobs/bi_silver/finance_sales_revenue.py",
    # "jobs/bi_silver/finance_daily_business_metrics.py",
    # "jobs/bi_silver/finance_metrics.py",

    # "jobs/bi_silver/hr_employee_onboard_snapshot.py",
    # "jobs/bi_silver/hr_employee_resigned_snapshot.py",
    # "jobs/bi_silver/hr_headcount_demand_snapshot.py",
    # "jobs/bi_silver/hr_employee_headcount.py",
    # "jobs/bi_silver/hr_employee_onboard.py",
    # "jobs/bi_silver/hr_employee_resigned.py",
    # "jobs/bi_silver/hr_workforce_monthly.py",
    # "jobs/bi_silver/hr_performance_rate.py",
    # "jobs/bi_silver/hr_workforce_monthly_drilldown.py",
    # "jobs/bi_silver/hr_workforce_monthly_ytd.py",


    # "jobs/bi_silver/hr_ld_budget_expense.py",
    # "jobs/bi_silver/hr_ld_budget_quarterly.py",
    # "jobs/bi_silver/hr_ld_budget_detail.py",
    # "jobs/bi_silver/hr_ld_class_list.py",
    # "jobs/bi_silver/hr_ld_employee_certification.py",
    # "jobs/bi_silver/hr_ld_employee_elearning_summary.py",
    # "jobs/bi_silver/hr_ld_fresher.py",
    # "jobs/bi_silver/hr_ld_fresher_conversion.py",
    # "jobs/bi_silver/hr_ld_fresher_resignation.py",
    # "jobs/bi_silver/hr_ld_instructor.py",
    # "jobs/bi_silver/hr_ld_learner.py",
    # "jobs/bi_silver/hr_ld_trainee.py",
    # "jobs/bi_silver/hr_ld_trainee_conversion.py",
    # "jobs/bi_silver/hr_ld_trainee_resignation.py",



    # "jobs/bi_silver/jira_task_operation.py",
    # "jobs/bi_silver/noc_entities.py",
    # "jobs/bi_silver/noc_metrics.py",
]


PARAMS = {"dummy_param": True}


@task
def run_job_task(job, params):
    logger = JobLogger()
    logger2 = get_run_logger()
    
    try:
        logger2.info(f"🚀 START {job} at {datetime.now()}")
        logger.start_job(job)

        cmd = [
            "bash",
            "/entrypoint.sh",
            "run",
            job,
            json.dumps(params),
        ]

        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )

        error_lines = []

        for line in process.stdout:
            # logger2.info(f"[{job}] {line}")
            if " INFO " not in line:
                error_lines.append(line)

        process.wait()

        if process.returncode != 0:
            raise Exception("".join(error_lines[-50:]))

        logger.success()
        logger2.info(f"✅ SUCCESS {job}")

    except Exception as e:
        logger.fail(e)
        logger2.info(f"❌ FAILED {job}")
        raise

    finally:
        logger.close()


@flow(name="spark_jobs_etl")
def run_all_jobs_flow(params={"dummy_param": True}):
    for job in JOBS:
        run_job_task(job, params)  

@flow(name="spark_single_job_etl")
def run_single_job(job_name, params):
    logger2 = get_run_logger()
    logger2.info(f"params =  {params}")
    run_job_task(job_name, params)


@flow(name="spark_table_linage")
def run_spark_table_linage():
    from pathlib import Path
    OM_SPARK_BASE_DIR =str(Path(__file__).resolve().parent.parent)
    import_spark_linages(JOBS, OM_SPARK_BASE_DIR)


@flow(name="spark_monthly_snapshot")
def run_monthly_snapshot_flow(snapshot_date=None):
    # ---------------------------------------------------------------
    # Resolve snapshot date
    # Default:
    # - Flow chạy ngày 01 của tháng mới
    # - Snapshot cho tháng trước
    #
    # Example:
    # Run: 2026-08-01 03:00 (VNT)
    # snapshot_date = 2026-07-01
    # ---------------------------------------------------------------
    VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")

    if snapshot_date is None:
        snapshot_date = (
            datetime.now(VN_TZ)
            .replace(day=1)
            - relativedelta(months=1)
        ).strftime("%Y-%m-%d")

    logger = get_run_logger()

    logger.info(f"📸 Monthly snapshot date: {snapshot_date}")

    run_snapshot_task(snapshot_date)


@flow(name="spark_snapshot_single_job")
def run_single_snapshot_flow(
    job_name,
    snapshot_date=None
):

    if snapshot_date is None:
        snapshot_date = (
            datetime.now()
            - relativedelta(months=1)
        ).replace(day=1).strftime("%Y-%m-%d")


    run_snapshot_task(
        job_name,
        snapshot_date
    )


    

# prefect deployment run "spark_single_job_etl/[Manual] spark_single_job_etl" \
#  --params '{"job_name": "jobs/bi_silver/crm_users.py", "params": {}}'

if __name__ == "__main__":
    d1 = run_all_jobs_flow.to_deployment(
        name="[Hourly] spark_job_etl",
        cron="30 * * * *",
        tags=["production", "spark", "etl"],
    )
    
    d2 = run_single_job.to_deployment(
        name="[Manual] spark_single_job_etl",
        tags=["production", "spark", "etl"],
    )
    
    d3 = run_spark_table_linage.to_deployment(
        name="[Daily] spark_table_linage",
        cron="30 19 * * *",
        tags=["production", "spark", "etl"],
    )

    d4 = run_monthly_snapshot_flow.to_deployment(
        name="[Monthly] spark_snapshot",
        schedule=Cron("0 3 1 * *", timezone="Asia/Ho_Chi_Minh"),
        tags= ["production", "spark", "snapshot", "monthly"],
    )

    d5 = run_single_snapshot_flow.to_deployment(
        name="[Manual] spark_snapshot_single_job",
        tags=["production", "spark", "snapshot"],
    )

    
    from prefect import serve
    serve(d1, d2, d3, d4, d5)
