
import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv

loaded = load_dotenv(verbose=True)

print(f"Loaded ENV = {loaded}")

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timezone, timedelta
from io import BytesIO
import boto3

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


VN_TZ = timezone(timedelta(hours=7))

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
    
    
def delete_folders_by_name(s3, bucket, parent_prefix):
    threshold_date = datetime.now() - timedelta(days=DAYS_TO_KEEP)
    
    # Đảm bảo prefix kết thúc bằng /
    if parent_prefix and not parent_prefix.endswith('/'):
        parent_prefix += '/'

    paginator = s3.get_paginator('list_objects_v2')
    
    # Duyệt qua các năm (category/2026/)
    year_pager = paginator.paginate(Bucket=bucket, Prefix=parent_prefix, Delimiter='/')
    for year_res in year_pager:
        for year_prefix in year_res.get('CommonPrefixes', []):
            
            # Duyệt qua các tháng (category/2026/01/)
            month_pager = paginator.paginate(Bucket=bucket, Prefix=year_prefix.get('Prefix'), Delimiter='/')
            for month_res in month_pager:
                for month_prefix in month_res.get('CommonPrefixes', []):
                    
                    # Duyệt qua các ngày (category/2026/01/01/)
                    day_pager = paginator.paginate(Bucket=bucket, Prefix=month_prefix.get('Prefix'), Delimiter='/')
                    for day_res in day_pager:
                        for day_prefix in day_res.get('CommonPrefixes', []):
                            
                            full_prefix = day_prefix.get('Prefix')
                            # full_prefix dạng: category/2026/01/01/
                            # split('/') sẽ ra ['', 'category', '2026', '01', '01', '']
                            parts = full_prefix.strip('/').split('/')
                            
                            if len(parts) >= 4:
                                # Lấy 3 phần cuối là Year, Month, Day
                                date_str = f"{parts[-3]}-{parts[-2]}-{parts[-1]}"
                                
                                try:
                                    folder_date = datetime.strptime(date_str, '%Y-%m-%d')
                                    
                                    if folder_date < threshold_date:
                                        print(f"--- Đang xóa folder cũ: {full_prefix} (Ngày: {date_str}) ---")
                                        delete_recursive(s3, bucket, full_prefix)
                                        
                                except ValueError:
                                    # Bỏ qua nếu folder không đúng định dạng ngày tháng
                                    continue

def delete_recursive(s3, bucket, prefix):
    """Hàm phụ trợ để xóa tất cả object bên trong một prefix"""
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        if 'Contents' in page:
            delete_keys = [{'Key': obj['Key']} for obj in page['Contents']]
            # Xóa tối đa 1000 objects mỗi lần (giới hạn của S3 API)
            s3.delete_objects(Bucket=bucket, Delete={'Objects': delete_keys})
            for obj in delete_keys:
                print(f"Đã xóa: {obj['Key']}")

    
def minio_house_keeping():
    s3 = create_minio_client()
    for bucket_name in minio_sources:
        bucket_prefixies = minio_sources[bucket_name]
        for bucket_prefix in bucket_prefixies:
            delete_folders_by_name(s3, bucket_name, bucket_prefix)
        

if __name__ == "__main__":
    minio_house_keeping()