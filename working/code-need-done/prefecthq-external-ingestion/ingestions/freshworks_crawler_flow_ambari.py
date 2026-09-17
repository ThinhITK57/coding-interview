from dotenv import load_dotenv

load_dotenv()
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import os
import sys
import json

from common.freshwork_helper import *
from resources import RESOURCE_BASE_DIR
from state import STATE_BASE_DIR
from data import DATA_BASE_DIR

from prefect import flow, get_run_logger


@flow(log_prints=True, name="[Hourly]-Ambari Deals Freshwork APIs - Data Lake")
def run_daily_scrapping_freshworks_crawler():
    logger = get_run_logger()
    # API_KEY = read_hdfs_https(API_KEY_PATH).strip()
    # API_COOKIE = read_hdfs_https(API_COOKIE_PATH).strip()
    FRESHWORKS_API_KEY = os.getenv("FRESHWORKS_API_KEY")
    FRESHWORKS_API_COOKIE = os.getenv("FRESHWORKS_API_COOKIE")

    if not FRESHWORKS_API_KEY:
        raise Exception("Not found FRESHWORKS_API_KEY")

    if not FRESHWORKS_API_COOKIE:
        raise Exception("Not found FRESHWORKS_API_COOKIE")

    # =========================
    # Load config
    # =========================
    # CONFIG_PATH = "/opt/datasets/crawlers/vcs/freshworks/configs/resources.json"
    # cfg = json.loads(read_hdfs_https(CONFIG_PATH))
    filename = os.path.join(RESOURCE_BASE_DIR, "resources-crm-ambari.json")
    with open(filename, "r") as f:
        cfg = json.loads(f.read().strip())


    HEADERS = {
        "Authorization": "Token token={}".format(FRESHWORKS_API_KEY),
        "Cookie": FRESHWORKS_API_COOKIE,
        "Content-Type": "application/json",
        "X-Requested-With": "XMLHttpRequest",
    }
    TABLES = [
        "contacts", 
        "sales_accounts", 
        "cm_contracts", 
        "cm_partners",
        "deals", 
        "cm_business_rules",
        "cm_activity_history",
    ]

    data_dir = DATA_BASE_DIR + "/crm"
    os.makedirs(data_dir, exist_ok=True)
    
    state_dir = STATE_BASE_DIR + "/crm"
    os.makedirs(state_dir, exist_ok=True)
    
    for resource_name in TABLES:
        logger.info(f"Start crawl {resource_name}")
        out_of_data = False
        for j in range(20):
            NUMBER_OF_PAGE = 50
            for i in range(6):
                try:
                    out_of_data = fetch_resource_name_freshwork(resource_name, cfg, headers=HEADERS, 
                                                                max_page=(i+1) * NUMBER_OF_PAGE,
                                                                start_page=i*NUMBER_OF_PAGE +1,
                                                                resource_dir=RESOURCE_BASE_DIR,
                                                                state_dir=state_dir,
                                                                data_dir=data_dir,)
                    if out_of_data :
                        print("out_of_data at " , i)
                        break
                except Exception as e:
                    raise e
            if out_of_data :
                print("out_of_data at " , j)
                break
        
        logger.info(f"End crawl {resource_name}")


@flow(log_prints=True, name="[Weekly]-Ambari Settings Freshwork APIs - Data Lake")
def run_weekly_scrapping_freshworks_crawler():
    # API_KEY = read_hdfs_https(API_KEY_PATH).strip()
    # API_COOKIE = read_hdfs_https(API_COOKIE_PATH).strip()
    FRESHWORKS_API_KEY = os.getenv("FRESHWORKS_API_KEY")
    FRESHWORKS_API_COOKIE = os.getenv("FRESHWORKS_API_COOKIE")

    if not FRESHWORKS_API_KEY:
        raise Exception("Not found FRESHWORKS_API_KEY")

    if not FRESHWORKS_API_COOKIE:
        raise Exception("Not found FRESHWORKS_API_COOKIE")

    # =========================
    # Load config
    # =========================
    # CONFIG_PATH = "/opt/datasets/crawlers/vcs/freshworks/configs/resources.json"
    # cfg = json.loads(read_hdfs_https(CONFIG_PATH))
    filename = os.path.join(RESOURCE_BASE_DIR, "resources-crm-ambari.json")
    with open(filename, "r") as f:
        cfg = json.loads(f.read().strip())

    HEADERS = {
        "Authorization": "Token token={}".format(FRESHWORKS_API_KEY),
        "Cookie": FRESHWORKS_API_COOKIE,
        "Content-Type": "application/json",
        "X-Requested-With": "XMLHttpRequest",
    }
    TABLES = [
       "cm_pricebook",
        "cm_catalog",  
        # "users", 
        "st_users",
        "cm_kpi",
        "deal_pipelines",
        "roles",
        "currencies", 
        "deal_reasons", 
        "deal_types",
        "deal_payment_statuses", 
        "territories", 
        "business_types", 
        "industry_types", 
        "contact_statuses",
        "deal_stages", 
        "teams",
    ]

    data_dir = DATA_BASE_DIR + "/crm"
    os.makedirs(data_dir, exist_ok=True)
    
    state_dir = STATE_BASE_DIR + "/crm"
    os.makedirs(state_dir, exist_ok=True)
    
    for resource_name in TABLES:
        out_of_data = False
        for j in range(20):
            NUMBER_OF_PAGE = 50
            for i in range(6):
                try:
                    out_of_data = fetch_resource_name_freshwork(resource_name, cfg, headers=HEADERS, 
                                                                max_page=(i+1) * NUMBER_OF_PAGE,
                                                                start_page=i*NUMBER_OF_PAGE +1,
                                                                resource_dir=RESOURCE_BASE_DIR,
                                                                state_dir=state_dir,
                                                                data_dir=data_dir,)
                    if out_of_data :
                        print("out_of_data at " , i)
                        break
                except Exception as e:
                    raise e
            if out_of_data :
                print("out_of_data at " , j)
                break


@flow(log_prints=True, name="[Adhoc]-Ambari Sync freshwork table from scratch")
def run_adhoc_scrapping_freshworks_crawler_by_table(resource_name):
    logger = get_run_logger()
    logger.info(f"Manual start crawl {resource_name}")
    # API_KEY = read_hdfs_https(API_KEY_PATH).strip()
    # API_COOKIE = read_hdfs_https(API_COOKIE_PATH).strip()
    FRESHWORKS_API_KEY = os.getenv("FRESHWORKS_API_KEY")
    FRESHWORKS_API_COOKIE = os.getenv("FRESHWORKS_API_COOKIE")

    if not FRESHWORKS_API_KEY:
        raise Exception("Not found FRESHWORKS_API_KEY")

    if not FRESHWORKS_API_COOKIE:
        raise Exception("Not found FRESHWORKS_API_COOKIE")

    # =========================
    # Load config
    # =========================
    # CONFIG_PATH = "/opt/datasets/crawlers/vcs/freshworks/configs/resources.json"
    # cfg = json.loads(read_hdfs_https(CONFIG_PATH))
    filename = os.path.join(RESOURCE_BASE_DIR, "resources-crm-ambari.json")
    with open(filename, "r") as f:
        cfg = json.loads(f.read().strip())

    HEADERS = {
        "Authorization": "Token token={}".format(FRESHWORKS_API_KEY),
        "Cookie": FRESHWORKS_API_COOKIE,
        "Content-Type": "application/json",
        "X-Requested-With": "XMLHttpRequest",
    }

    data_dir = DATA_BASE_DIR + "/crm"
    os.makedirs(data_dir, exist_ok=True)
    
    state_dir = STATE_BASE_DIR + "/crm"
    os.makedirs(state_dir, exist_ok=True)
    
    cfg[resource_name]["crawl_mode"] = "static"
    
    out_of_data = False
    for j in range(20):
        NUMBER_OF_PAGE = 50
        for i in range(6):
            try:
                out_of_data = fetch_resource_name_freshwork(resource_name, cfg, headers=HEADERS, 
                                                            max_page=(i+1) * NUMBER_OF_PAGE,
                                                            start_page=i*NUMBER_OF_PAGE +1,
                                                            resource_dir=RESOURCE_BASE_DIR,
                                                            state_dir=state_dir,
                                                            data_dir=data_dir,)
                if out_of_data :
                    print("out_of_data at " , i)
                    break
            except Exception as e:
                raise e
        if out_of_data :
            print("out_of_data at " , j)
            break
    logger.info(f"Manual end crawl {resource_name}")
    
    
if __name__ == "__main__":
    d1 = run_daily_scrapping_freshworks_crawler.to_deployment(
        name="[Hourly]-Ambari Deals Freshwork APIs - Data Lake",
        cron="10 * * * *",
        tags=["production", "Freshworks", "Deals", "ingestion"],
    )
    d2 = run_weekly_scrapping_freshworks_crawler.to_deployment(
        name="[Weekly]-Ambari Settings Freshwork APIs - Data Lake",
        cron="0 18 * * 1", # 01:00 sáng, Thứ Hai hằng tuần
        tags=["production", "Freshworks", "Settings", "ingestion"],
    )
    
    d3 = run_adhoc_scrapping_freshworks_crawler_by_table.to_deployment(
        name="[Weekly]-Ambari Settings Freshwork APIs - Data Lake",
        tags=["production", "Freshworks", "Settings", "ingestion"],
    )
    from prefect import serve
    serve(d1, d2, d3)