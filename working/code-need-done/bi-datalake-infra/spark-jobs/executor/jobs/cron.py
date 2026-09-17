import time
import schedule
import subprocess
import json
from datetime import datetime
import sys
from utils.job_logger import JobLogger


run_single = len(sys.argv) > 1 and sys.argv[1] == "run_single"

# ===== CONFIG =====
num_hours = 1  # hoặc lấy từ sys.argv nếu cần

# ===== JOB LIST =====
JOBS = [
    # "jobs/cx_cso_silver/cso_support_tickets.py",

    "jobs/bi_silver/crm_partners.py",
    # "jobs/bi_silver/crm_pricebook.py",
    "jobs/bi_silver/crm_contracts.py",
    # "jobs/bi_silver/crm_deals.py",
    "jobs/bi_silver/crm_contract_allocations.py",
    # "jobs/bi_silver/crm_deal_interested_products.py",
    # "jobs/bi_silver/crm_deal_quotation_products.py",
    # "jobs/bi_silver/crm_deal_quotations.py",
    # "jobs/bi_silver/crm_deal_reasons.py",
    "jobs/bi_silver/crm_payment_forecast.py",

    "jobs/bi_silver/crm_allocated_revenue.py",

    # # "jobs/bi_silver/crm_product_category.py",
    # # "jobs/bi_silver/crm_product_tree_item_code.py",
    # # "jobs/bi_silver/crm_product_tree.py",

    "jobs/bi_silver/crm_sales_accounts.py",
    # "jobs/bi_silver/crm_users.py",
    # "jobs/bi_silver/cx_cso_support_tickets.py",
    # "jobs/bi_silver/cx_cso_ticket_tags.py",
    # "jobs/bi_silver/cx_sur_surveys.py",
    # "jobs/bi_silver/cx_sur_question_answer_choices.py",
    # "jobs/bi_silver/cx_sur_question_response.py",

    # "jobs/bi_silver/dim_customer_segment_l1.py",
    # "jobs/bi_silver/dim_date.py",
    # "jobs/bi_silver/dim_product_category.py",
    # "jobs/bi_silver/dim_territory_name.py",
    # "jobs/bi_silver/dim_unit_level_1.py",

    # "jobs/bi_silver/finance_actual_cost.py",
    
    "jobs/bi_silver/finance_allocated_revenue.py",
    
    # "jobs/bi_silver/finance_cash_collection_excel.py",
    
    # "jobs/bi_silver/finance_contract_collected_invoices.py",
    # "jobs/bi_silver/finance_cost_plan.py",
    # "jobs/bi_silver/finance_fact_ratios.py",
    # "jobs/bi_silver/finance_product_revenue_plan.py",
    # "jobs/bi_silver/finance_production_cost_allocations.py",
    # "jobs/bi_silver/finance_revenue_plan.py",
    # "jobs/bi_silver/finance_sales_product_revenue.py",

    # "jobs/bi_silver/hr_employee_headcount.py",
    # "jobs/bi_silver/hr_employee_onboard.py",
    # "jobs/bi_silver/hr_employee_resigned.py",
    # "jobs/bi_silver/jira_task_operation.py",
    # "jobs/bi_silver/noc_entities.py",
    # "jobs/bi_silver/noc_metrics.py",
]

PARAMS = {"dummy_param": True}


# ===== HOOKS =====
def before_run(job):
    print(f"🚀 START {job} at {datetime.now()}")


def after_run(job):
    print(f"✅ SUCCESS {job} at {datetime.now()}")


def on_error(job, error):
    print(f"❌ ERROR {job} at {datetime.now()}")
    print(error)


# ===== EXECUTOR =====


def run_job(job):
    cmd = [
        "bash",
        "/entrypoint.sh",
        "run",
        job,
        json.dumps(PARAMS),
    ]

    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,  # gộp stderr vào stdout
        text=True,
        bufsize=1,  # line buffered
    )
    error_lines = []
    # stream realtime
    for line in process.stdout:
        print(f"[{job}] {line}", end="")
        if line.find(" INFO ") == -1:
            error_lines.append(line)

    process.wait()

    if process.returncode != 0:
        if len(error_lines) > 50:
            error_lines = error_lines[len(error_lines) - 50:]
        raise Exception(f"Job failed with code {process.returncode} . {error_lines}")


def run_all_jobs():
    logger = JobLogger()
    print(f"\n🕒 Batch start {datetime.now()}")
    failed_job = []
    for job in JOBS:
        try:
            before_run(job)
            logger.start_job(job)

            run_job(job)

            after_run(job)
            logger.success()

        except Exception as e:
            on_error(job, e)
            failed_job.append(job)
            logger.fail(e)
            continue
    logger.close()
    
    for fe in failed_job:
        print(f"failed_job {fe}")
    print(f"🎯 Batch done {datetime.now()}\n")


# ===== SCHEDULER =====
schedule.every(num_hours).hours.do(run_all_jobs)

print(f"🕒 Cron started - every {num_hours} hours")

# run lần đầu ngay
if run_single:
    print("⚡ Run batch immediately")
    run_all_jobs()
    sys.exit(0)

while True:
    try:
        schedule.run_pending()
    except Exception as e:
        print("Scheduler error:", e)
        time.sleep(10)
    time.sleep(1)