from dotenv import load_dotenv

load_dotenv()
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from prefect import flow, get_run_logger
from noc_crawler_tool import run_crawl_noc_from_minio, VN_TZ
from datetime import datetime

@flow(log_prints=True, name="[Daily] Noc Metrics Minio - Data Lake")
def run_many_names():
    
    names = [
        "mem_project__util__mean",
        "mem_project__util__max",
        "mem_project__util__min",
        "swap_used_percent__mean",
        "swap_used_percent__max",
        "swap_used_percent__min",
        "la_per_cpu__mean",
        "la_per_cpu__max",
        "la_per_cpu__min",
        "usage__mean",
        "usage__max",
        "usage__min",
        "disk_used_percent__mean",
        "disk_used_percent__max",
        "disk_used_percent__min",
        "disk_project__io_util__mean",
        "disk_project__io_util__max",
        "disk_project__io_util__min",
        "net_customer__util__mean",
        "net_customer__util__max",
        "net_customer__util__min",
        "mem_usage_average__mean",
        "mem_usage_average__max",
        "mem_usage_average__min",
        "cpu_usage_average__mean",
        "cpu_usage_average__max",
        "cpu_usage_average__min",
        "free_space__capacity",
    ]
    crawl_mode = "modified_and_new"
    
    # start_day_str = "2026-01-26"
    # end_day_str   = "2026-01-27"
    now = datetime.now().astimezone(VN_TZ)
    start_day_str = now.strftime("%Y-%m-%d")
    # now = timedelta(1)
    end_day_str   = now.strftime("%Y-%m-%d")
    
    for name in names:
        run_crawl_noc_from_minio(name, crawl_mode,start_day_str, end_day_str )    
    
if __name__ == "__main__":
    run_many_names.serve(
        name="[Daily] Noc Metrics Minio - Data Lake",
        cron="0 2 * * *",
        tags=["production", "jira", "ingestion"],
    )

    