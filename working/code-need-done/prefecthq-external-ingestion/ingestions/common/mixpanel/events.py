import requests
from requests.auth import HTTPBasicAuth
import os
# import json
import gzip
# import orjson
from ..proxy_util import build_request_proxies
import time


def normalize_keys(obj):
    if isinstance(obj, dict):
        result = {}

        for k, v in obj.items():
            new_key = k

            if k.startswith("$"):
                new_key = "c_" + k[1:]

            result[new_key] = normalize_keys(v)

        return result

    elif isinstance(obj, list):
        return [normalize_keys(x) for x in obj]

    return obj


def fetch_mixpanel_data(
    project_name: str,
    api_secret: str,
    target_date: str,
    base_dir: str
):
    # print(f"[{target_date}] {project_name}: Fetching...")

    filename = f"{base_dir}/{project_name}/{project_name}_{target_date}.jsonl.gz"
    os.makedirs(os.path.dirname(filename), exist_ok=True)

    url = "https://data.mixpanel.com/api/2.0/export"

    params = {
        "from_date": target_date,
        "to_date": target_date,
        # "limit": 100000,
    }
    #from datetime import datetime, timedelta

    #today = datetime.strptime(target_date, "%Y-%m-%d")
    #watermark = 0
    #params = {
    #"from_date": (today - timedelta(days=2)).strftime("%Y-%m-%d"),
    #"to_date": today.strftime("%Y-%m-%d"),
    #"where": (
    #    f'properties["mp_processing_time_ms"] >= {watermark - 60000}'
    #),
    #"limit": 100000,
    #}

    try:
        # 60 queries / giờ
        # 3 queries / giây
        # 100 concurrent queries
        
        MAX_RETRY = 3
        attempt = 0
        while attempt < MAX_RETRY:
            response = requests.get(
                url,
                headers={
                    "Accept-Encoding": "gzip",
                    'Cookie': 'mp__origin=""; mp__origin_referrer=""',
                },
                params=params,
                auth=HTTPBasicAuth(api_secret, ""),
                stream=True,
                proxies=build_request_proxies(),
                verify=False,
                timeout=600
            )
            if response.status_code == 429:
                print(f"Wait for 429 {project_name} with attempt = {attempt}")
                attempt += 1
                time.sleep(10)
                continue
            
            if response.status_code != 200:
                raise Exception(str(response.status_code) + " - " + str(response.text))
            
            break


        with gzip.open(filename, "wb", compresslevel=6) as f:
            for chunk in response.iter_content(1024 * 1024):
                f.write(chunk)

        # with gzip.open(
        #     filename,
        #     "wt",
        #     encoding="utf-8",
        #     compresslevel=6
        # ) as f:

        #     for line in response.iter_lines(decode_unicode=True):

        #         if not line:
        #             continue

        #         try:
        #             obj = normalize_keys(orjson.loads(line))

        #             f.write(
        #                 orjson.dumps(
        #                     obj,
        #                     ensure_ascii=False,
        #                     separators=(",", ":")
        #                 ) + "\n"
        #             )

        #         except orjson.JSONDecodeError:
        #             pass
        file_size = os.path.getsize(filename)
        if file_size <=0 :
            raise Exception(f"Not found data for [{target_date}] {project_name}")
        
        file_size = file_size/(1024*1024)
        print(f"Saved {filename} : { file_size :.1f} MB")
        return filename
    except Exception as e:
        raise e
        