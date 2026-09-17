import os
import time
import uuid

from .crawler_util import load_pyarrow_schema_from_json
from .pd_schema import ddl_to_arrow_schema, write_parquet_file
from .minio_file_client import MinioS3Client

from common.trino_utils.trino_config import (
    get_clone_config,
    get_src_config,
    get_dst_config,
)
from common.trino_utils.schema_utils import (
    extract_s3_location,
    normalize_ddl,
)
from common.trino_utils.trino_client import TrinoClient



class TrinoCloneService:

    def __init__(
        self,
        schema: str,
        table: str,
        resource_dir: str = None,
    ):
        self.schema = schema
        self.table = table

        self.full_src = f"hive.{schema}.{table}"
        self.full_dst = f"hive.{schema}.{table}"

        self.src_config = get_src_config()
        self.dst_config = get_dst_config()
        self.config = get_clone_config()
        if resource_dir:
            self.config.parquet_schema_dir = resource_dir

        self.src = TrinoClient(
            self.src_config,
            schema,
        )

        self.dst = TrinoClient(
            self.dst_config,
            schema,
        )

        self.minio_client = MinioS3Client()

        os.makedirs(
            self.config.tmp_dir,
            exist_ok=True,
        )

    def close(self):
        self.src.close()
        self.dst.close()

    def run(
        self,
        truncate: bool = False,
        clone_schema: bool = False,
    ):
        print(
            f"🚀 Clone {self.full_src} → {self.full_dst}"
        )

        self._configure_target()

        self._validate_source()

        target_exists = self._target_exists()

        self.clone_table(
            truncate=truncate,
            is_clone_schema=(
                clone_schema or not target_exists
            ),
        )

    def _configure_target(self):
        self.dst.execute(
            "SET SESSION task_max_writer_count = "
            f"{self.config.task_max_writer_count}"
        )

        self.dst.execute(
            "SET SESSION hive.target_max_file_size = "
            f"'{self.config.target_max_file_size}'"
        )

    def _validate_source(self):
        if not self.src.table_exists(
            "hive",
            self.schema,
            self.table,
        ):
            raise ValueError(
                f"Source table does not exist: "
                f"{self.full_src}"
            )

    def _target_exists(self):
        return self.dst.table_exists(
            "hive",
            self.schema,
            self.table,
        )

    def clone_table(
        self,
        truncate: bool = False,
        is_clone_schema: bool = False,
    ):
        ddl_src = self.src.get_create_table_ddl(
            self.full_src
        )

        schema_pd = self._get_pyarrow_schema(
            ddl_src
        )

        if is_clone_schema:
            self._recreate_target_table(
                ddl_src,
                schema_pd,
            )

        ddl_dst = self.dst.get_create_table_ddl(
            self.full_dst
        )

        s3_location = extract_s3_location(
            ddl_dst
        )

        print(
            f"🚀 Cloning "
            f"{self.full_src} → {s3_location}"
        )

        if truncate:
            self.minio_client.remove(
                s3_location
            )

        self._copy_data(
            s3_location,
            schema_pd,
        )

    def _get_pyarrow_schema(self, ddl_src):

        schema_path = os.path.join(
            self.config.parquet_schema_dir,
            self.schema,
            f"{self.table}.json",
        )

        if os.path.exists(schema_path):
            schema_pd = load_pyarrow_schema_from_json(
                schema_path
            )

            if schema_pd:
                return schema_pd

        return ddl_to_arrow_schema(ddl_src)

    def _recreate_target_table(
        self,
        ddl_src,
        schema_pd,
    ):
        print(
            f"🧨 Recreate table {self.full_dst}"
        )

        existed = self._target_exists()

        if existed:
            ddl_dst = self.dst.get_create_table_ddl(
                self.full_dst
            )

            s3_location = extract_s3_location(
                ddl_dst
            )

            print(
                f"🚀 Remove Folder {s3_location}"
            )

            self.minio_client.remove_dir(
                s3_location
            )

        self.dst.execute(
            f"DROP TABLE IF EXISTS {self.full_dst}"
        )

        ddl_dst = normalize_ddl(
            ddl_src,
            tgt_schema=f"hive.{self.schema}",
            table=self.table,
        )

        ddl_dst = ddl_dst.replace(
            "csv_escape = '\\',",
            "",
        )

        ddl_dst = ddl_dst.replace(
            "csv_quote = '\"',",
            "",
        )

        ddl_dst = ddl_dst.replace(
            "csv_separator = ',',",
            "",
        )

        ddl_dst = ddl_dst.replace(
            "format = 'CSV',",
            "",
        )

        ddl_dst = ddl_dst.replace(
            "skip_header_line_count = 1",
            "format = 'PARQUET'",
        )

        print(ddl_dst)

        self.dst.execute(ddl_dst)

        for field in schema_pd:
            print(
                f"{field.name}: {field.type}"
            )

        time.sleep(2)

    def _copy_data(
        self,
        s3_location,
        schema_pd,
    ):
        self.src.execute(
            f"SELECT * FROM {self.full_src}"
        )

        columns = [
            col[0]
            for col in self.src.cursor.description
        ]

        total_rows = 0
        file_count = 0

        while True:
            rows = self.src.cursor.fetchmany(
                self.config.batch_size
            )

            if not rows:
                break

            local_file = os.path.join(
                self.config.tmp_dir,
                f"{self.schema}_{self.table}_"
                f"{uuid.uuid4().hex}.parquet",
            )

            try:
                write_parquet_file(
                    rows,
                    schema_pd,
                    local_file,
                    columns,
                )

                self.minio_client.upload(
                    s3_location,
                    local_file,
                )

            finally:
                if os.path.exists(local_file):
                    os.remove(local_file)

            total_rows += len(rows)
            file_count += 1

            print(
                f"   {total_rows} rows, "
                f"{file_count} files uploaded"
            )

        print(
            f"✅ DONE {self.full_src}: "
            f"{total_rows} rows, "
            f"{file_count} files"
        )