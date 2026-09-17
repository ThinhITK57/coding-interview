# from dotenv import load_dotenv

# load_dotenv()

import os
import requests
from .config import *
from .http_util import *
from .crawler_util import (
    read_file_text,
    write_file_text,
    read_file_json,
    write_file_json,
)
import time
from pathlib import Path
from .base_hdf_client import BaseHDFSClient
from .ambari_client import AmbariHDFSClient
from .minio_file_client import MinioS3Client


def create_hdfs_client(kind="ambari") -> BaseHDFSClient:
    if kind == "ambari":
        return AmbariHDFSClient()
    if kind == "minio":
        return MinioS3Client()
    raise ValueError("Unknown HDFS client type - " + str(kind))


hdfs_client = create_hdfs_client(os.getenv("HDFS_CLIENT", "ambari"))


# =========================
# HDFS HTTPS helpers
# =========================
def read_hdfs_https(path):
    return hdfs_client.read(path)


def upload_hdfs_https(hdfs_path, local_file):
    print("Add Upload ", hdfs_path, local_file)
    # mkdirs_hdfs_https(hdfs_path)
    try:
        hdfs_client.upload(hdfs_path, local_file)
    except Exception as e:
        print(e)
        raise e


def mkdirs_hdfs_https(hdfs_path):
    try:
        hdfs_client.mkdirs(hdfs_path)
    except Exception as e:
        print(e)
        raise e


def remove_hdfs_https(hdfs_path: str, save_folders=["crawlers", "vcs", "data"]):
    for save_folder in save_folders:
        if hdfs_path.find(save_folder) == -1:
            print("Invalid path ", hdfs_path)
            return
        if hdfs_path.endswith(save_folder):
            print("Invalid path ", hdfs_path)
            return
    try:
        hdfs_client.remove(hdfs_path)
    except Exception as e:
        print(e)
        raise e


def replace_hdfs_https(hdfs_path, local_file):
    print("Replace Upload ", hdfs_path, local_file)
    try:
        hdfs_client.replace(hdfs_path, local_file)
    except Exception as e:
        print(e)
        raise e


# =========================
# State handling (HTTPS)
# =========================
def read_last_state(resoure_name, state_path=None):
    try:
        # return read_hdfs_https(
        #     "{}/state-{}.txt".format(state_path, resoure_name)
        # )
        tmp = "./tmp/state-{}.txt".format(resoure_name)
        with open(tmp, "r") as f:
            return f.read().strip()
    except Exception:
        return None


def write_last_state(ts, resoure_name):
    tmp = "./tmp/state-{}.txt".format(resoure_name)
    with open(tmp, "w") as f:
        f.write(str(ts))

    # remove_hdfs_https("{}/state-{}.txt".format(state_path, resoure_name))

    # upload_hdfs_https(
    #     "{}".format(state_path),
    #     tmp
    # )
