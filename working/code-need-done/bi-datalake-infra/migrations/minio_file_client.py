import os
from urllib.parse import urlparse
import boto3
import warnings
from urllib3.exceptions import InsecureRequestWarning
warnings.simplefilter('ignore', InsecureRequestWarning)
from typing import Optional



class MinioS3Client:
    def __init__(self, endpoint=None, access_key=None, secret_key=None, verify=False):
        """_summary_
            client = MinioS3Client(
                endpoint=MINIO_ENDPOINT,
                access_key=MINIO_ACCESS_KEY,
                secret_key=MINIO_SECRET_KEY
            )
            client.upload(
                "s3://my-bucket/data/file.parquet",
                "./local/file.parquet"
            )
            client.upload_dir(
                "./data",
                "s3://my-bucket/data/"
            )
            content = client.read("s3://my-bucket/data/file.txt")
            client.remove("s3://my-bucket/data/")
            client.exists("s3://my-bucket/data/file.parquet")
        Args:
            endpoint (_type_, optional): _description_. Defaults to None.
            access_key (_type_, optional): _description_. Defaults to None.
            secret_key (_type_, optional): _description_. Defaults to None.
            verify (bool, optional): _description_. Defaults to False.
        """
        if not endpoint:
            endpoint = os.getenv("MINIO_ENDPOINT")
        if not access_key:
            access_key = os.getenv("MINIO_ACCESS_KEY")
        if not secret_key:
            secret_key = os.getenv("MINIO_SECRET_KEY")

        self.s3 = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            verify=verify,
        )

    # ========================
    # Utils
    # ========================
    def _parse_path(self, path: str):
        """
        s3://bucket/path/to/file -> (bucket, key)
        """
        if path.startswith("s3a://"):
            path = path.replace("s3a://", "s3://")
            
        if not path.startswith("s3://"):
            path = "s3://" +  path.lstrip("/")
            
        if path.startswith("s3://"):
            parsed = urlparse(path)
            bucket = parsed.netloc
            key = parsed.path.lstrip("/")
            print(f"bucket={bucket} , key={key}")
        else:
            raise ValueError(f"Invalid S3 path: {path}")

        return bucket, key

    def _list_objects(self, bucket, prefix):
        paginator = self.s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                yield obj["Key"]

    def parse_s3_path(self, s3_url):
        parsed = urlparse(s3_url.replace("s3a://", "s3://"))
        return parsed.netloc, parsed.path.lstrip("/")

    # ========================
    # Implement methods
    # ========================
    def list_s3_objects(self, s3_url):
        bucket, prefix = self.parse_s3_path(s3_url)
        paginator = self.s3.get_paginator("list_objects_v2")
        files = []
        parent_key = ""
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if str(key).endswith("/"):
                    parent_key = key
                    continue
                files.append(key)
        return bucket, files, parent_key
    
    def upload_s3_file(self, local_file, bucket, s3_key):
        self.s3.upload_file(local_file, bucket, s3_key)


    def download_s3_objects(self, bucket, keys, local_dir):
        os.makedirs(local_dir, exist_ok=True)
        local_files = []
        for _, key in enumerate(keys):
            local_file = os.path.join(local_dir, os.path.basename(key))
            print(bucket, key, local_file)
            
            if str(key).endswith("/"):
                continue
            
            self.s3.download_file(bucket, key, local_file)
            local_files.append(local_file)
        return local_files
    
    
    def read(self, s3_path: str) -> str:
        bucket, key = self._parse_path(s3_path)

        obj = self.s3.get_object(Bucket=bucket, Key=key)
        return obj["Body"].read().decode("utf-8")


    def upload(self, s3_path: str, local_file: str):
        basename = os.path.basename(local_file)
        if not s3_path.endswith("/"):
            s3_path = s3_path+"/"
        s3_path = s3_path + basename
        
        bucket, key = self._parse_path(s3_path)

        if os.path.isdir(local_file):
            raise ValueError("upload() expects file, got directory")

        self.s3.upload_file(local_file, bucket, key)

    def upload_dir(self, local_dir: str, hdfs_dir: str):
        
        # if not hdfs_dir.endswith("/"):
            # hdfs_dir = hdfs_dir+"/"
            
        bucket, prefix = self._parse_path(hdfs_dir)

        for root, _, files in os.walk(local_dir):
            for file in files:
                local_path = os.path.join(root, file)

                rel_path = os.path.relpath(local_path, local_dir)
                s3_key = os.path.join(prefix, rel_path).replace("\\", "/")

                self.s3.upload_file(local_path, bucket, s3_key)

    def mkdirs(self, s3_path: str):
        """
        S3 không có folder thật → tạo object dummy
        """
        bucket, key = self._parse_path(s3_path)

        if not key.endswith("/"):
            key += "/"

        self.s3.put_object(Bucket=bucket, Key=key)

    def remove(self, s3_path: str):
        bucket, prefix = self._parse_path(s3_path)
        if not bucket or not prefix:
            raise Exception(f"Invalid bucket = {bucket} , prefix={prefix}")

        objects_to_delete = [{"Key": key} for key in self._list_objects(bucket, prefix) if not key.endswith("/")]
        print(f"objects_to_delete={objects_to_delete}")

        if objects_to_delete:
            self.s3.delete_objects(Bucket=bucket, Delete={"Objects": objects_to_delete})
            
            
    def remove_dir(self, s3_path: str):
        bucket, prefix = self._parse_path(s3_path)
        if not bucket or not prefix:
            raise Exception(f"Invalid bucket = {bucket} , prefix={prefix}")

        objects_to_delete = [{"Key": key} for key in self._list_objects(bucket, prefix)]
        print(f"objects_to_delete={objects_to_delete}")

        if objects_to_delete:
            self.s3.delete_objects(Bucket=bucket, Delete={"Objects": objects_to_delete})
        

    def replace(self, s3_path: str, local_file: str):
        """
        Xóa rồi upload lại
        """
        self.remove(s3_path)
        self.upload(s3_path, local_file)

    def exists(self, s3_path: str) -> bool:
        bucket, key = self._parse_path(s3_path)

        try:
            self.s3.head_object(Bucket=bucket, Key=key)
            return True
        except:
            # check prefix
            objs = list(self._list_objects(bucket, key))
            return len(objs) > 0

    def rename_prefix(self,
        bucket: str,
        old_prefix: str,
        new_prefix: str,
        delete_batch_size: int = 1000,
        dry_run: bool = False,
    ):
        """
        Rename prefix trong MinIO/S3 bằng cách copy + delete.

        Params:
            bucket: tên bucket
            old_prefix: prefix cũ (vd: data/old/)
            new_prefix: prefix mới (vd: data/new/)
            delete_batch_size: batch size khi delete (max 1000)
            dry_run: True = chỉ log, không thực thi

        Returns:
            total_files_moved
        """
        paginator = self.s3.get_paginator("list_objects_v2")

        total = 0
        delete_buffer = []

        for page in paginator.paginate(Bucket=bucket, Prefix=old_prefix):
            contents = page.get("Contents", [])
            if not contents:
                continue

            for obj in contents:
                old_key = obj["Key"]
                if str(old_key).endswith("/"):
                    continue
                new_key = old_key.replace(old_prefix, new_prefix, 1)

                print(f"➡️ {old_key} -> {new_key}")

                if not dry_run:
                    # copy
                    self.s3.copy_object(
                        Bucket=bucket,
                        CopySource={"Bucket": bucket, "Key": old_key},
                        Key=new_key
                    )

                    delete_buffer.append({"Key": old_key})

                total += 1

                # batch delete
                if not dry_run and len(delete_buffer) >= delete_batch_size:
                    self.s3.delete_objects(
                        Bucket=bucket,
                        Delete={"Objects": delete_buffer}
                    )
                    delete_buffer = []

        # delete phần còn lại
        if not dry_run and delete_buffer:
            self.s3.delete_objects(
                Bucket=bucket,
                Delete={"Objects": delete_buffer}
            )

        print(f"✅ Done. Total files moved: {total}")
        return total
    
    def clone_prefix(self,
        src_bucket: str,
        src_prefix: str,
        dst_bucket: str,
        dst_prefix: str,
        dry_run: bool = False,
    ):
        """
        Clone prefix trong MinIO/S3 bằng cách copy

        Params:
            src_bucket: tên bucket
            src_prefix: prefix cũ (vd: data/old/)
            dst_bucket: 
            dst_prefix: prefix mới (vd: data/new/)
            dry_run: True = chỉ log, không thực thi

        Returns:
            total_files_moved
        """
        paginator = self.s3.get_paginator("list_objects_v2")

        total = 0

        for page in paginator.paginate(Bucket=src_bucket, Prefix=src_prefix):
            contents = page.get("Contents", [])
            if not contents:
                continue

            for obj in contents:
                old_key = obj["Key"]
                if str(old_key).endswith("/"):
                    continue
                new_key = old_key.replace(src_prefix, dst_prefix, 1)

                print(f"➡️ {old_key} -> {new_key}")

                if not dry_run:
                    # copy
                    self.s3.copy_object(
                        Bucket=dst_bucket,
                        CopySource={"Bucket": src_bucket, "Key": old_key},
                        Key=new_key
                    )

                total += 1

        print(f"✅ Done. Total files moved: {total}")
        return total
