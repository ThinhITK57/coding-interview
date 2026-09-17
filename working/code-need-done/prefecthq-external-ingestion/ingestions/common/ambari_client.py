import os
import requests
from .config import *
from .http_util import *
import time
from .base_hdf_client import BaseHDFSClient
from typing import List


def ambari_get_cookie_text(
    username: str,
    password: str,
    timeout: int = 10,
) -> str:
    url = os.getenv(
        "AMBARI_LOGIN_URL",
        "https://datalake.viettelcyber.com/gateway/ui/ambari/api/v1/clusters",
    )
    headers = {"X-Requested-By": "ambari"}

    resp = requests.get(
        url,
        headers=headers,
        auth=(username, password),
        timeout=timeout,
    )

    if resp.status_code != 200:
        raise RuntimeError(f"Ambari login failed: {resp.status_code} - {resp.text}")

    unix_ms = int(time.time() * 1000)
    AMBARI_TEST_URL = os.getenv(
        "AMBARI_TEST_URL",
        "https://datalake.viettelcyber.com/gateway/ui/ambari/api/v1/clusters?fields=Clusters/provisioning_state,Clusters/security_type,Clusters/version,Clusters/cluster_id&_=",
    ) + str(unix_ms)
    resp = requests.get(
        AMBARI_TEST_URL,
        headers=headers,
        auth=(username, password),
        timeout=timeout,
    )
    if resp.status_code != 200:
        raise RuntimeError(f"Ambari login failed: {resp.status_code} - {resp.text}")

    # Ghép toàn bộ cookie thành chuỗi text
    cookie_text = "; ".join([f"{k}={v}" for k, v in resp.cookies.get_dict().items()])

    if not cookie_text:
        raise RuntimeError("No cookies returned from Ambari")

    return cookie_text


def expired_session(session):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
        "X-Requested-By": "ambari",
        "X-Requested-With": "XMLHttpRequest",
        "Content-Type": "application/json",
        "Cookie": session,
    }
    unix_ms = int(time.time() * 1000)
    AMBARI_TEST_URL = os.getenv(
        "AMBARI_TEST_URL",
        "https://datalake.viettelcyber.com/gateway/ui/ambari/api/v1/clusters?fields=Clusters/provisioning_state,Clusters/security_type,Clusters/version,Clusters/cluster_id&_=",
    ) + str(unix_ms)
    resp = requests.get(
        AMBARI_TEST_URL,
        headers=headers,
        timeout=10,
    )
    if resp.status_code != 200:
        return False
    return True


# =========================
# HDFS HTTPS helpers
# =========================


class AmbariHDFSClient(BaseHDFSClient):
    def __init__(
        self,
        files_api: str = None,
        request_timeout: int = 150,
    ):
        """
        :param files_api: base URL của Ambari Files API
                          ví dụ: https://ambari-host/api/v1/views/FILES/versions/1.0.0/instances/FILES1
        """
        if files_api is None:
            files_api = os.getenv(
                "AMBARI_FILES_API",
                "https://datalake.viettelcyber.com/gateway/ui/ambari/api/v1/views/FILES/versions/1.0.0/instances/FILES/resources/files",
            )
        self.files_api = files_api.rstrip("/")
        self.request_timeout = request_timeout
        self._login()

    def _login(self):
        AMBARI_USERNAME = os.getenv("AMBARI_USERNAME")
        AMBARI_PASSWORD = os.getenv("AMBARI_PASSWORD")
        AMBARI_SESSION = ambari_get_cookie_text(AMBARI_USERNAME, AMBARI_PASSWORD)

        self.hdfs_headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
            "X-Requested-By": "ambari",
            "X-Requested-With": "XMLHttpRequest",
            "Content-Type": "application/json",
            "Cookie": AMBARI_SESSION,
        }

        self.hdfs_upload_headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
            "X-Requested-By": "ambari",
            "X-Requested-With": "XMLHttpRequest",
            "Cookie": AMBARI_SESSION,
        }

        self.hdfs_mkdir_headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
            "X-Requested-By": "ambari",
            "X-Requested-With": "XMLHttpRequest",
            "Content-Type": "application/json",
            "Cookie": AMBARI_SESSION,
        }

    def read(self, hdfs_path: str) -> str:
        url = f"{self.files_api}/download/browse"
        params = {
            "download": "true",
            "path": hdfs_path,
        }

        r = requests.get(
            url,
            headers=self.hdfs_headers,
            params=params,
            timeout=self.request_timeout,
        )
        if r.status_code == 403:
            self._login()
            r = requests.get(
                url,
                headers=self.hdfs_headers,
                params=params,
                timeout=self.request_timeout,
            )
        r.raise_for_status()
        return r.text

    def mkdirs(self, hdfs_path: str):
        url = f"{self.files_api}/fileops/mkdir"
        data = {"path": hdfs_path}

        r = safe_put_request(
            url,
            headers=self.hdfs_mkdir_headers,
            max_retries=5,
            timeout=self.request_timeout,
            json_data=data,
        )
        if r.status_code == 403:
            self._login()
            r = safe_put_request(
                url,
                headers=self.hdfs_mkdir_headers,
                max_retries=5,
                timeout=self.request_timeout,
                json_data=data,
            )

        if r.status_code != 200:
            raise RuntimeError(r.text)

    def upload(self, hdfs_path: str, local_file: str):
        url = f"{self.files_api}/upload"

        files = {
            "file": (
                os.path.basename(local_file),
                open(local_file, "rb"),
            ),
            "path": hdfs_path,
        }

        r = safe_put_request(
            url,
            headers=self.hdfs_upload_headers,
            max_retries=5,
            timeout=self.request_timeout,
            files=files,
            data=None,
        )
        if r.status_code == 403:
            self._login()
            r = safe_put_request(
                url,
                headers=self.hdfs_upload_headers,
                max_retries=5,
                timeout=self.request_timeout,
                files=files,
                data=None,
            )

        if r.status_code != 200:
            raise RuntimeError(r.text)

    def remove(
        self,
        hdfs_path: str,
        save_folders: List[str] = ("crawlers", "vcs", "data"),
    ):
        for folder in save_folders:
            if folder not in hdfs_path:
                raise ValueError(f"Invalid path: {hdfs_path}")
            if hdfs_path.endswith(folder):
                raise ValueError(f"Invalid path: {hdfs_path}")

        url = f"{self.files_api}/fileops/remove"
        data = {
            "paths": [
                {
                    "path": hdfs_path,
                    "recursive": True,
                }
            ]
        }

        r = safe_post_request(
            url,
            headers=self.hdfs_mkdir_headers,
            max_retries=5,
            timeout=self.request_timeout,
            json_data=data,
        )
        if r.status_code == 403:
            self._login()
            r = safe_post_request(
                url,
                headers=self.hdfs_mkdir_headers,
                max_retries=5,
                timeout=self.request_timeout,
                json_data=data,
            )

        if r.status_code != 200:
            raise RuntimeError(r.text)

    def replace(self, hdfs_path: str, local_file: str):
        try:
            self.remove(hdfs_path)
        except Exception:
            pass

        self.mkdirs(hdfs_path)
        self.upload(hdfs_path, local_file)

    def upload_dir(self, local_dir: str, hdfs_dir: str):
        local_dir = os.path.abspath(local_dir)

        for root, _, files in os.walk(local_dir):
            rel_path = os.path.relpath(root, local_dir)
            target_dir = hdfs_dir if rel_path == "." else f"{hdfs_dir}/{rel_path}"

            self.mkdirs(target_dir)

            for f in files:
                self.upload(
                    target_dir,
                    os.path.join(root, f),
                )

    def exists(self, hdfs_path: str) -> bool:
        url = f"{self.files_api}/fileops/isfile"
        params = {"path": hdfs_path}
        r = requests.get(
            url,
            headers=self.hdfs_headers,
            params=params,
        )
        if r.status_code == 403:
            self._login()
            r = requests.get(
                url,
                headers=self.hdfs_headers,
                params=params,
            )
        return r.status_code == 200
