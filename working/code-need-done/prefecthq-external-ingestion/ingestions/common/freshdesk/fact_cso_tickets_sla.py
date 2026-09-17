#!/usr/bin/env python3

import logging
import os
from pathlib import Path
from urllib.parse import urlparse
import io
import re
import unicodedata
import pandas as pd
import requests

# =====================================================
# LOGGING
# =====================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)

# =====================================================
# HELPERS
# =====================================================

def build_proxies():
    proxies = {}
    
    CRAWLER_HTTP_PROXY = os.getenv("CRAWLER_HTTP_PROXY")
    CRAWLER_HTTPS_PROXY = os.getenv("CRAWLER_HTTPS_PROXY")

    if CRAWLER_HTTP_PROXY:
        proxy = CRAWLER_HTTP_PROXY

        if not proxy.startswith(("http://", "https://")):
            proxy = f"http://{proxy}"

        proxies["http"] = proxy

    if CRAWLER_HTTPS_PROXY:
        proxy = CRAWLER_HTTPS_PROXY

        if not proxy.startswith(("http://", "https://")):
            proxy = f"http://{proxy}"

        proxies["https"] = proxy

    return proxies if proxies else None


def extract_filename(download_url: str) -> str:
    parsed = urlparse(download_url)

    filename = Path(parsed.path).name

    if not filename:
        filename = "freshdesk_export.csv"

    if not filename.endswith(".csv"):
        filename = f"{filename}.csv"

    return filename


# =====================================================
# FRESHDESK
# =====================================================
def get_export_url():
    """
    Gọi endpoint schedule report để lấy URL export thực tế.
    """
    SCHEDULE_URL = "https://vcs-care.freshdesk.com/reports/schedule/download_file.json?uuid=9d3c7cb1-0046-47c7-aff0-200cef91f7db"

    FRESHDESK_API_KEY= os.getenv("FRESHDESK_API_KEY")

    if not FRESHDESK_API_KEY:
        raise Exception("Not found FRESHDESK_API_KEY")


    logger.info("Getting export url...")

    response = requests.get(
        SCHEDULE_URL,
        auth=(FRESHDESK_API_KEY, "X"),
        proxies=build_proxies(),
        timeout=60,
        verify=False,
    )

    response.raise_for_status()

    payload = response.json()

    export_url = (
        payload.get("export", {})
        .get("url")
    )

    if not export_url:
        raise RuntimeError(
            f"Cannot find export.url in response: {payload}"
        )

    logger.info(f"Export URL found {export_url}")

    return export_url


def normalize_column_name(col: str) -> str:
    """Chuẩn hóa tên cột."""
    col = str(col).strip()

    # bỏ dấu tiếng Việt
    col = unicodedata.normalize("NFD", col)
    col = "".join(c for c in col if unicodedata.category(c) != "Mn")

    # lowercase
    col = col.lower()

    # space -> _
    col = re.sub(r"\s+", "_", col)

    # bỏ ký tự đặc biệt
    col = re.sub(r"[^a-z0-9_]", "", col)

    # tránh ____
    col = re.sub(r"_+", "_", col)

    return col.strip("_")


def download_cso_sla_csv_to_records() -> list[dict]:
    export_url: str = get_export_url()
    logger.info("Downloading CSV into memory")

    response = requests.get(
        export_url,
        proxies=build_proxies(),
        timeout=300,verify=False,
    )
    response.raise_for_status()

    # đọc CSV trực tiếp từ RAM
    csv_buffer = io.StringIO(response.text)

    df = pd.read_csv(csv_buffer)

    # đổi tên cột
    df.columns = [normalize_column_name(col) for col in df.columns]

    records = df.to_dict(orient="records")

    logger.info(
        "Loaded %s records, %s columns",
        len(records),
        len(df.columns),
    )

    return records

if __name__ == "__main__":
    records = download_cso_sla_csv_to_records()
    logger.info("Job completed successfully")