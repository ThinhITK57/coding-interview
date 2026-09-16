from dotenv import load_dotenv

load_dotenv()
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import os
import sys
import json

from common.record_upload_helper import *
from resources import RESOURCE_BASE_DIR
from state import STATE_BASE_DIR
from data import DATA_BASE_DIR

from prefect import flow, get_run_logger
import requests
import pandas as pd
import re
import unidecode


def clean_column_names(df):
    def transform(column_name):
        column_name = column_name.lower()
        column_name = unidecode.unidecode(column_name)
        column_name = re.sub(r'[^a-z0-9]', '_', column_name)
        column_name = re.sub(r'_+', '_', column_name)
        column_name = column_name.strip('_')
        return column_name

    df.columns = [transform(col) for col in df.columns]
    return df


def get_nocodb_data():
    DOMAIN = os.getenv("NOCODB_URL")
    TABLE_ID = os.getenv("NOCODB_TABLE_ID")
    TOKEN = os.getenv("NOCODB_TOKEN")
    PAGE_SIZE = 1000
    
    url = f"{DOMAIN}api/v2/tables/{TABLE_ID}/records"
    headers = {
        "Accept": "application/json",
        "xc-token": TOKEN
    }
    
    all_records = []
    offset = 0
    
    while True:
        params = {
            "limit": PAGE_SIZE,
            "offset": offset
        }
        
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status() # Kiểm tra lỗi HTTP
        
        data = response.json().get('list', [])
        
        if not data:
            break
            
        all_records.extend(data)
        offset += PAGE_SIZE
        
        # Nếu số lượng bản ghi lấy về ít hơn PAGE_SIZE nghĩa là đã hết dữ liệu
        if len(data) < PAGE_SIZE:
            break

    # Tạo DataFrame
    df = pd.DataFrame(all_records)
    
    if df.empty:
        return df

    # ===== EXPAND COLUMNS (Xử lý các cột lồng nhau) =====
    # Tương đương Table.ExpandRecordColumn trong Power BI
    if 'dws_kpi_criterias' in df.columns:
        df['dws_kpi_criterias.Name'] = df['dws_kpi_criterias'].apply(lambda x: x.get('Name') if isinstance(x, dict) else None)
        
    if 'dws_kpi_systems' in df.columns:
        df['dws_kpi_systems.Name'] = df['dws_kpi_systems'].apply(lambda x: x.get('Name') if isinstance(x, dict) else None)

    # ===== CLEANING (Làm sạch dữ liệu) =====
    def clean_system_name(text):
        if not isinstance(text, str):
            return text
        
        # 1. Bỏ phần trong ngoặc [ ] và cả dấu ngoặc
        # Regex này tìm nội dung từ dấu [ đến hết
        cleaned = re.sub(r'\[.*\]', '', text).strip()
        
        # 2. Thay thế "Tỉ lệ" thành "Tỷ lệ"
        cleaned = cleaned.replace("Tỉ lệ", "Tỷ lệ")
        
        return cleaned

    if 'dws_kpi_systems.Name' in df.columns:
        df['dws_kpi_systems.Name'] = df['dws_kpi_systems.Name'].apply(clean_system_name)

    return df


@flow(log_prints=True, name="[Daily] KPI KPQ NocoDBs-Data Lake")
def run_scrapping_kpi_kqi_nocodb_crawler():
    df_final = get_nocodb_data()
    df_final = clean_column_names(df_final)
    print(df_final.head(1))
    records = df_final.to_dict(orient="records")
    resource_name= "kpi_systems"
    hdfs_base = "/opt/datasets/crawlers/vcs/nocodb/data"
    hive_db = "nocodb_raw"
    crawl_mode = "static"
    upload_record_to_hdfs(resource_name,records, hdfs_base=hdfs_base, hive_db=hive_db, crawl_mode=crawl_mode)


if __name__ == "__main__":
    run_scrapping_kpi_kqi_nocodb_crawler.serve(
        name="[Daily] KPI KPQ NocoDBs - Data Lake",
        cron="0 20 * * *",
        tags=["production", "KPI/KQI Nocodb", "ingestion"],
    )