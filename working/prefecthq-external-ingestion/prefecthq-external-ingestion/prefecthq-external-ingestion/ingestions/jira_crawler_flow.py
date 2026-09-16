from dotenv import load_dotenv

load_dotenv()
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from prefect import flow, get_run_logger
from jira_crawler_tool import run_crawl_jira_from_minio, VN_TZ, CSV_ZIP, JSON_GZ
from datetime import datetime


JIRA_HDFS_WRITE_MODES = [
    ["ulnl","modified_and_new",CSV_ZIP],
    ["bug_prod","modified_and_new", CSV_ZIP],
    ["process_compliance","modified_and_new", CSV_ZIP],
    ["kpi_kqi","modified_and_new", CSV_ZIP],
    ["os_project_uat_bugs","modified_and_new", CSV_ZIP],
    ["kpi_binh_staff","modified_and_new", CSV_ZIP],
    ["task_operation","modified_and_new", CSV_ZIP],
    ["defect_task","modified_and_new", CSV_ZIP],
    ["project_management","modified_and_new", CSV_ZIP],
    ["log_work","modified_and_new", CSV_ZIP],

    ["msn_task","static", CSV_ZIP],
    ["jira_user","static", CSV_ZIP],
    
    ["field_list","static", JSON_GZ],
    ["project_list","static", JSON_GZ],

    # new tables from jira_v2
    # ["log_work_v2", "modified_and_new", JSON_GZ], du lieu co tu [work_logs]
    # ["bug_prod_v2", "modified_and_new", JSON_GZ],  du lieu co tu [versions]

    ["sprints", "static", JSON_GZ],
    # ["project_versions", "static", JSON_GZ], du lieu co tu [projects]
    
    ["projects", "static", JSON_GZ],
    ["versions", "static", JSON_GZ],
    ["users", "static", JSON_GZ],
    ["custom_fields", "static", JSON_GZ],
    
    # ["custom_field_options", "static", JSON_GZ], Xem xet phuong an chay khac hay khong
    
    ["app_users", "static", JSON_GZ],
    
    ["jira_issues", "modified_and_new", JSON_GZ],
    ["custom_field_values", "modified_and_new", JSON_GZ],
    ["version_issue", "modified_and_new", JSON_GZ],
    ["work_logs", "modified_and_new", JSON_GZ],
]


@flow(log_prints=True, name="[Daily] Jira Minio - Data Lake")
def run_many_names():
    now = datetime.now().astimezone(VN_TZ)
    start_day_str = now.strftime("%Y-%m-%d")
    # now = timedelta(1)
    end_day_str   = now.strftime("%Y-%m-%d")
    
    for name in JIRA_HDFS_WRITE_MODES:
        run_crawl_jira_from_minio(name[0], name[1],
                                  file_type=name[2],
                                  start_day_str=start_day_str,
                                  end_day_str=end_day_str )    
        

@flow(log_prints=True, name="[Manual] Jira Minio - Data Lake")
def run_reconcile_one_name(cate_name: str, start_date: str="2026-05-12", end_date: str="2026-05-12" ):
    for name in JIRA_HDFS_WRITE_MODES:
        if name[0] != cate_name:
            continue
        run_crawl_jira_from_minio(name[0], name[1],
                                  file_type=name[2],
                                  start_day_str=start_date,
                                  end_day_str=end_date )    
        
    
if __name__ == "__main__":
    d1 = run_many_names.to_deployment(
        name="[Daily] Jira Minio - Data Lake",
        cron="0 2 * * *",
        tags=["production", "jira", "ingestion"],
    )

    d2 = run_reconcile_one_name.to_deployment(
        name="[Manual] Jira Minio - Data Lake",
         tags=["production", "jira", "ingestion"],
    )
    
    from prefect import serve
    serve(d1, d2)
