"""
Ambari HDFS Client Adapter for Knox Gateway
Ported & Adapted from prefecthq-external-ingestion for crawler-prefecthq-02.

Enables headless upload, download, and directory management on Ambari HDFS
via Knox Gateway REST API without requiring raw NameNode RPC port 8020 access.
"""

import os
import time
import logging
import requests
from typing import List, Optional, Dict, Any

logger = logging.getLogger(__name__)


def ambari_get_cookie_text(
    username: Optional[str] = None,
    password: Optional[str] = None,
    timeout: int = 150,
) -> str:
    """
    Authenticates with Ambari via Knox Gateway and returns the cookie header string.
    """
    username = username or os.getenv("AMBARI_USERNAME", "")
    password = password or os.getenv("AMBARI_PASSWORD", "")
    url = os.getenv(
        "AMBARI_LOGIN_URL",
        "https://datalake.viettelcyber.com/gateway/ui/ambari/api/v1/clusters",
    )
    headers = {"X-Requested-By": "ambari"}

    if not username or not password:
        logger.warning("AMBARI_USERNAME or AMBARI_PASSWORD not set. Returning empty cookie.")
        return ""

    try:
        resp = requests.get(
            url,
            headers=headers,
            auth=(username, password),
            timeout=timeout,
            verify=False
        )
        if resp.status_code != 200:
            raise RuntimeError(f"Ambari login failed: {resp.status_code} - {resp.text}")

        unix_ms = int(time.time() * 1000)
        test_url = os.getenv(
            "AMBARI_TEST_URL",
            "https://datalake.viettelcyber.com/gateway/ui/ambari/api/v1/clusters?fields=Clusters/provisioning_state,Clusters/security_type,Clusters/version,Clusters/cluster_id&_=",
        ) + str(unix_ms)

        resp2 = requests.get(
            test_url,
            headers=headers,
            auth=(username, password),
            timeout=timeout,
            verify=False
        )
        if resp2.status_code != 200:
            raise RuntimeError(f"Ambari verification failed: {resp2.status_code} - {resp2.text}")

        cookies = resp2.cookies.get_dict()
        cookie_text = "; ".join([f"{k}={v}" for k, v in cookies.items()])
        return cookie_text
    except Exception as e:
        logger.warning(f"Could not authenticate with Ambari: {e}")
        return ""


class AmbariHDFSClient:
    """
    Client for interacting with HDFS through Ambari Files View REST API.
    """
    def __init__(
        self,
        files_api: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        request_timeout: int = 150,
    ):
        self.files_api = (files_api or os.getenv(
            "AMBARI_FILES_API",
            "https://datalake.viettelcyber.com/gateway/ui/ambari/api/v1/views/FILES/versions/1.0.0/instances/FILES/resources/files",
        )).rstrip("/")
        self.request_timeout = request_timeout

        self.username = username or os.getenv("AMBARI_USERNAME", "")
        self.password = password or os.getenv("AMBARI_PASSWORD", "")
        self.session_cookie = ambari_get_cookie_text(self.username, self.password, timeout=self.request_timeout)

        self.headers = {
            "User-Agent": "AmbariHDFSClient/2.0 (Prefect-ETL)",
            "X-Requested-By": "ambari",
            "X-Requested-With": "XMLHttpRequest",
            "Content-Type": "application/json",
            "Cookie": self.session_cookie,
        }

    def exists(self, hdfs_path: str) -> bool:
        url = f"{self.files_api}/fileops/isfile"
        params = {"path": hdfs_path}
        try:
            r = requests.get(url, headers=self.headers, params=params, timeout=self.request_timeout, verify=False)
            return r.status_code == 200
        except Exception as e:
            logger.warning(f"Ambari exists check failed for {hdfs_path}: {e}")
            return False

    def mkdirs(self, hdfs_path: str):
        url = f"{self.files_api}/fileops/mkdir"
        data = {"path": hdfs_path}
        headers = dict(self.headers)
        try:
            r = requests.put(url, headers=headers, json=data, timeout=self.request_timeout, verify=False)
            if r.status_code not in (200, 201):
                logger.warning(f"Ambari mkdirs warning: {r.status_code} - {r.text}")
        except Exception as e:
            logger.error(f"Ambari mkdirs error for {hdfs_path}: {e}")

    def upload(self, hdfs_path: str, local_file: str):
        url = f"{self.files_api}/upload"
        upload_headers = {
            "User-Agent": "AmbariHDFSClient/2.0",
            "X-Requested-By": "ambari",
            "X-Requested-With": "XMLHttpRequest",
            "Cookie": self.session_cookie,
        }
        with open(local_file, "rb") as f:
            files = {
                "file": (os.path.basename(local_file), f),
                "path": hdfs_path,
            }
            r = requests.put(url, headers=upload_headers, files=files, timeout=self.request_timeout, verify=False)
            if r.status_code not in (200, 201):
                raise RuntimeError(f"Ambari upload failed: {r.status_code} - {r.text}")

    def upload_dir(self, local_dir: str, hdfs_dir: str):
        """
        Recursively uploads an entire local directory (e.g. Parquet partitions) into Ambari HDFS.
        """
        local_dir = os.path.abspath(local_dir)
        for root, _, files in os.walk(local_dir):
            rel_path = os.path.relpath(root, local_dir)
            target_hdfs_dir = hdfs_dir if rel_path == "." else f"{hdfs_dir}/{rel_path}".replace("\\", "/")
            self.mkdirs(target_hdfs_dir)
            for file_name in files:
                local_file_path = os.path.join(root, file_name)
                logger.info(f"Uploading {file_name} -> {target_hdfs_dir}")
                self.upload(target_hdfs_dir, local_file_path)

    def remove(self, hdfs_path: str):
        url = f"{self.files_api}/fileops/remove"
        data = {"paths": [{"path": hdfs_path, "recursive": True}]}
        r = requests.post(url, headers=self.headers, json=data, timeout=self.request_timeout, verify=False)
        if r.status_code not in (200, 204):
            logger.warning(f"Ambari remove warning for {hdfs_path}: {r.text}")
