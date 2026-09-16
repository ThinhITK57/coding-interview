# | Ambari Files API   | WebHDFS                    |
# | ------------------ | -------------------------- |
# | `/download/browse` | `op=OPEN`                  |
# | `/fileops/mkdir`   | `op=MKDIRS`                |
# | `/fileops/remove`  | `op=DELETE&recursive=true` |
# | `/upload`          | `op=CREATE` + redirect     |

import os
import requests
from typing import List
from requests_kerberos import HTTPKerberosAuth, OPTIONAL
from .base_hdf_client import BaseHDFSClient


class KerberosWebHDFSClient(BaseHDFSClient):
    def __init__(
        self,
        timeout: int = 150,
        verify_ssl: bool = False,
    ):
        """
        :param namenodes: list of NameNode hosts (HA)
        :param port: WebHDFS port (https=9871, http=9870)
        :param scheme: http | https
        """
        txt = os.getenv("NAMENODES_HOST", "namenode1.local,namenode2.local")
        namenodes = txt.split(",")
        self.namenodes = namenodes
        self.port = int(os.getenv("NAMENODES_PORT", "9871"))
        self.scheme = os.getenv("NAMENODES_SCHEME", "https")
        self.timeout = timeout
        self.verify_ssl = verify_ssl

        self.auth = HTTPKerberosAuth(
            mutual_authentication=OPTIONAL,
            sanitize_mutual_error_response=False,
        )

        self.active_nn = self._detect_active_namenode()
        self.base_url = f"{self.scheme}://{self.active_nn}:{self.port}/webhdfs/v1"

    def _detect_active_namenode(self) -> str:
        last_error = None
        for nn in self.namenodes:
            url = f"{self.scheme}://{nn}:{self.port}/webhdfs/v1/?op=GETHOMEDIRECTORY"
            try:
                r = requests.get(
                    url,
                    auth=self.auth,
                    timeout=self.timeout,
                    verify=self.verify_ssl,
                )
                if r.status_code == 200:
                    return nn
            except Exception as e:
                last_error = e

        raise RuntimeError(f"No active NameNode found. Last error: {last_error}")

    def read(self, hdfs_path: str) -> str:
        url = f"{self.base_url}{hdfs_path}"
        params = {"op": "OPEN"}

        r = requests.get(
            url,
            params=params,
            auth=self.auth,
            timeout=self.timeout,
            verify=self.verify_ssl,
        )
        r.raise_for_status()
        return r.text

    def mkdirs(self, hdfs_path: str):
        url = f"{self.base_url}{hdfs_path}"
        params = {"op": "MKDIRS"}

        r = requests.put(
            url,
            params=params,
            auth=self.auth,
            timeout=self.timeout,
            verify=self.verify_ssl,
        )
        r.raise_for_status()

    def delete(self, hdfs_path: str, recursive=True):
        url = f"{self.base_url}{hdfs_path}"
        params = {
            "op": "DELETE",
            "recursive": "true" if recursive else "false",
        }

        r = requests.delete(
            url,
            params=params,
            auth=self.auth,
            timeout=self.timeout,
            verify=self.verify_ssl,
        )
        r.raise_for_status()

    def upload_file(self, hdfs_path: str, local_file: str, overwrite=True):
        url = f"{self.base_url}{hdfs_path}"
        params = {
            "op": "CREATE",
            "overwrite": "true" if overwrite else "false",
        }

        r1 = requests.put(
            url,
            params=params,
            auth=self.auth,
            allow_redirects=False,
            timeout=self.timeout,
            verify=self.verify_ssl,
        )

        if r1.status_code != 307:
            raise RuntimeError(f"CREATE failed: {r1.text}")

        datanode_url = r1.headers["Location"]

        with open(local_file, "rb") as f:
            r2 = requests.put(
                datanode_url,
                data=f,
                auth=self.auth,
                timeout=self.timeout,
                verify=self.verify_ssl,
            )
            r2.raise_for_status()

    def upload_dir(
        self,
        local_dir: str,
        hdfs_dir: str,
        overwrite=True,
    ):
        local_dir = os.path.abspath(local_dir)

        for root, _, files in os.walk(local_dir):
            rel_path = os.path.relpath(root, local_dir)
            hdfs_target_dir = hdfs_dir if rel_path == "." else f"{hdfs_dir}/{rel_path}"

            self.mkdirs(hdfs_target_dir)

            for file in files:
                local_file = os.path.join(root, file)
                hdfs_file = f"{hdfs_target_dir}/{file}"
                self.upload_file(hdfs_file, local_file, overwrite=overwrite)

    def replace(self, hdfs_path: str, local_file: str):
        try:
            self.delete(hdfs_path)
        except Exception:
            pass

        parent = os.path.dirname(hdfs_path)
        if parent:
            self.mkdirs(parent)

        self.upload_file(hdfs_path, local_file, overwrite=True)

    def exists(self, hdfs_path: str) -> bool:
        url = f"{self.base_url}{hdfs_path}"
        params = {"op": "GETFILESTATUS"}
        r = requests.get(
            url,
            params=params,
            auth=self.auth,
            timeout=self.timeout,
            verify=self.verify_ssl,
        )
        return r.status_code == 200
