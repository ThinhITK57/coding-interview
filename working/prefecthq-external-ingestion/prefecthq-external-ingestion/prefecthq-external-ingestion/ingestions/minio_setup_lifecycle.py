import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv

loaded = load_dotenv(verbose=True)

print(f"Loaded ENV = {loaded}")
import boto3

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

DAYS_TO_KEEP = 7

minio_sources = {
    "noc-metrics" :  [
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
    ],
    "jira-data": [
        "ulnl",
        "bug_prod",
        "process_compliance",
        "kpi_kqi",
        "os_project_uat_bugs",
        "kpi_binh_staff",
        "task_operation",
        "msn_task",
        "defect_task",
        "project_management",
        "log_work",
    ]
}

def create_minio_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_JIRA_ENDPOINT"),
        aws_access_key_id=os.getenv("MINIO_JIRA_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("MINIO_JIRA_SECRET_KEY"),
        verify=os.getenv("MINIO_JIRA_SECURE", "false") == "true",
        
    )
    
def set_minio_lifecycle(s3, bucket_name, prefix, days_to_keep):
    lifecycle_config = {
        'Rules': [
            {
                'ID': 'DeleteOldObjects',
                'Status': 'Enabled',
                'Filter': {
                    'Prefix': prefix, # Chỉ áp dụng cho prefix này
                },
                'Expiration': {
                    'Days': days_to_keep # Xóa sau X ngày
                }
            }
        ]
    }
    
    try:
        s3.put_bucket_lifecycle_configuration(
            Bucket=bucket_name,
            LifecycleConfiguration=lifecycle_config
        )
        print(f"Đã thiết lập tự động xóa cho {bucket_name}/{prefix} sau {days_to_keep} ngày.")
    except Exception as e:
        print(f"Lỗi cấu hình: {e}")

# Gọi hàm
# 
def minio_house_keeping():
    s3 = create_minio_client()
    for bucket_name in minio_sources:
        bucket_prefixies = minio_sources[bucket_name]
        for bucket_prefix in bucket_prefixies:
            set_minio_lifecycle(s3, bucket_name, bucket_prefix+"/", DAYS_TO_KEEP)
        

if __name__ == "__main__":
    minio_house_keeping()