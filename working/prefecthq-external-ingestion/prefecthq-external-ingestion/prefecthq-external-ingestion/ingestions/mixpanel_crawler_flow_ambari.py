from dotenv import load_dotenv

load_dotenv()
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import os
import sys
import json
from datetime import datetime, timedelta
import time
from common.mixpanel_cx_helper import  push_mixpanel_file
from common.mixpanel.events import fetch_mixpanel_data
from common.date_util import date_range_generator

from resources import RESOURCE_BASE_DIR
from state import STATE_BASE_DIR
from data import DATA_BASE_DIR

from prefect import flow, get_run_logger


@flow(log_prints=True, name="[Hourly] CX-Ambari Mixpanel APIs Crawler")
def run_daily_scrapping_mixpanel_crawler():
    logger = get_run_logger()

    # =========================
    # Load config
    # =========================
    # CONFIG_PATH = "/opt/datasets/crawlers/vcs/freshworks/configs/resources.json"
    # cfg = json.loads(read_hdfs_https(CONFIG_PATH))

    filename = os.path.join(RESOURCE_BASE_DIR, "resources-cx-mixpanel-ambari.json")
    with open(filename, "r") as f:
        cfg = json.loads(f.read().strip())

    data_dir = DATA_BASE_DIR + "/mixpanel"
    os.makedirs(data_dir, exist_ok=True)
    
    state_dir = STATE_BASE_DIR + "/cx_cso"
    os.makedirs(state_dir, exist_ok=True)
    
    now = datetime.now()
    day_to = "{}-{:02d}-{:02d}".format(now.year, now.month, now.day)
    now += timedelta(days= -3)
    day_from = "{}-{:02d}-{:02d}".format(now.year, now.month, now.day)
    
    
    base_temp_dir = "./tmp/data/cx_mixpanel_raw"
    
    for resource_name in cfg:
        rc = cfg[resource_name]
        api_secret_env = rc["api_secret_env"]
        api_secret_env = os.getenv(api_secret_env)
        if not api_secret_env:
            logger.warning(f"Not found ENV for {resource_name}")
            continue
        
        for target_date in date_range_generator(day_from, day_to):
            logger.info(f"[{target_date}] {resource_name}: Fetching...")
            try:
                local_filename = fetch_mixpanel_data(project_name=resource_name, api_secret=api_secret_env, target_date=target_date, base_dir=base_temp_dir)
                push_mixpanel_file(resource_name=resource_name, cfg=cfg, local_filename=local_filename, target_date=target_date)
            except Exception as e:
                logger.error(str(e))
            time.sleep(1)
        



@flow(log_prints=True, name="[Manual] CX-Ambari Mixpanel APIs Crawler")
def run_adhoc_scrapping_mixpanel_crawler_by_day(resource_name, day_from, day_to):
    logger = get_run_logger()

    # =========================
    # Load config
    # =========================
    # CONFIG_PATH = "/opt/datasets/crawlers/vcs/freshworks/configs/resources.json"
    # cfg = json.loads(read_hdfs_https(CONFIG_PATH))

    filename = os.path.join(RESOURCE_BASE_DIR, "resources-cx-mixpanel-ambari.json")
    with open(filename, "r") as f:
        cfg = json.loads(f.read().strip())

    data_dir = DATA_BASE_DIR + "/mixpanel"
    os.makedirs(data_dir, exist_ok=True)
    
    state_dir = STATE_BASE_DIR + "/cx_cso"
    os.makedirs(state_dir, exist_ok=True)
    
    now = datetime.now()
    target_date = "{}-{:02d}-{:02d}".format(now.year, now.month, now.day)
    
    base_temp_dir = "./tmp/data/cx_mixpanel_raw"
    
    for target_date in date_range_generator(day_from, day_to):
        rc = cfg[resource_name]
        api_secret_env = rc["api_secret_env"]
        api_secret_env = os.getenv(api_secret_env)
        if not api_secret_env:
            logger.warning(f"[{target_date}] {resource_name}: Not found ENV for {api_secret_env}")
            continue
        
        logger.info(f"[{target_date}] {resource_name}: Fetching...")
        try:
            local_filename = fetch_mixpanel_data(project_name=resource_name, api_secret=api_secret_env, target_date=target_date, base_dir=base_temp_dir)
            push_mixpanel_file(resource_name=resource_name, cfg=cfg, local_filename=local_filename, target_date=target_date)
            logger.info(f"[{target_date}] {resource_name}: Done {local_filename}!")
        except Exception as e:
            logger.error(str(e))
        time.sleep(1)
    
    
if __name__ == "__main__":
    d1 = run_daily_scrapping_mixpanel_crawler.to_deployment(
        name="[Hourly] CX-Ambari Mixpanel APIs Crawler",
        cron="0 22 * * *",
        tags=["production", "CX", "Mixpanel", "ingestion"],
    )

    d3 = run_adhoc_scrapping_mixpanel_crawler_by_day.to_deployment(
        name="[Manual] CX-Ambari Mixpanel APIs Crawler",
        tags=["production", "CX", "Mixpanel", "ingestion"],
    )
    from prefect import serve
    serve(d1, d3)