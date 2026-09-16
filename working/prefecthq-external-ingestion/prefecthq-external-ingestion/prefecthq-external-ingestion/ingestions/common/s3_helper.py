import boto3

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def list_s3_files(s3: boto3.Session, bucket: str, prefix: str = "") -> list[str]:
    paginator = s3.get_paginator("list_objects_v2")
    keys = []

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])

    return keys

def read_s3_file(s3: boto3.Session, bucket: str, key: str) -> bytes:
    print(f"Load file {bucket}/{key}")
    response = s3.get_object(Bucket=bucket, Key=key)
    return response["Body"].read()

