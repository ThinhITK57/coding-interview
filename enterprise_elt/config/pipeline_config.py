import argparse
import sys
from datetime import datetime

class PipelineConfig:
    """
    Quản lý tập trung toàn bộ cấu hình, tham số môi trường và ngưỡng hệ thống.
    Được inject vào các component khác theo nguyên lý Dependency Injection.
    """
    def __init__(self, args_list=None):
        self.parser = argparse.ArgumentParser(
            description="Enterprise Production ELT Pipeline Engine (Spark 2.3.2 Compatible)"
        )
        self._setup_arguments()
        self.args = self.parser.parse_args(args_list if args_list is not None else sys.argv[1:])
        self._validate_and_derive()

    def _setup_arguments(self):
        # Môi trường chạy
        self.parser.add_argument("--env", type=str, choices=["dev", "staging", "prod"], default="dev",
                                 help="Môi trường triển khai: dev | staging | prod")
        
        # Nhận diện mục tiêu & phân vùng
        self.parser.add_argument("--domain-target", type=str, required=True,
                                 choices=["muc-1", "muc-2", "muc-3", "muc-4"],
                                 help="Mục tiêu nghiệp vụ cần nạp (muc-1, muc-2, muc-3, muc-4)")
        self.parser.add_argument("--batch-id", type=str, default=None,
                                 help="Mã định danh batch (nếu rỗng sẽ tự sinh theo timestamp)")
        self.parser.add_argument("--execution-date", type=str, default=datetime.utcnow().strftime("%Y-%m-%d"),
                                 help="Ngày xử lý dữ liệu YYYY-MM-DD")

        # Ngưỡng Quality & Rate limit
        self.parser.add_argument("--dlq-threshold-ratio", type=float, default=0.02,
                                 help="Tỷ lệ bản ghi lỗi tối đa cho phép trước khi cảnh báo (ví dụ 0.02 = 2%)")
        self.parser.add_argument("--rate-limit-rps", type=int, default=10,
                                 help="Giới hạn số request/giây khi gọi API (tránh HTTP 429)")

        # Cấu hình MinIO & Storage
        self.parser.add_argument("--minio-endpoint", type=str, default="http://minio:9000")
        self.parser.add_argument("--minio-access-key", type=str, default="minioadmin")
        self.parser.add_argument("--minio-secret-key", type=str, default="minioadmin")
        self.parser.add_argument("--minio-raw-bucket", type=str, default="backup-raw")

        # Cấu hình Trino Catalogs
        self.parser.add_argument("--trino-dev-catalog", type=str, default="iceberg_dev",
                                 help="Catalog database cá nhân cho dev/thử nghiệm")
        self.parser.add_argument("--trino-prod-catalog", type=str, default="iceberg_prod",
                                 help="Catalog database tổng đã làm sạch")
        self.parser.add_argument("--user-name", type=str, default="engineer_dev",
                                 help="Tên user phục vụ tạo sandbox database cá nhân")

    def _validate_and_derive(self):
        if not self.args.batch_id:
            self.args.batch_id = f"batch_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        
        # Đường dẫn raw backup trên MinIO S3A
        self.raw_backup_path = (
            f"s3a://{self.args.minio_raw_bucket}/{self.args.domain_target}/"
            f"date={self.args.execution_date}/batch_id={self.args.batch_id}"
        )
        
        # Tên bảng đích
        domain_normalized = self.args.domain_target.replace('-', '_')
        self.dev_table_name = f"{self.args.trino_dev_catalog}.sandbox_{self.args.user_name}.raw_{domain_normalized}"
        self.prod_table_name = f"{self.args.trino_prod_catalog}.cleaned.{domain_normalized}"
        self.dlq_table_name = f"{self.args.trino_prod_catalog}.monitoring.dlq_{domain_normalized}"

    def get(self, key):
        return getattr(self.args, key, None)
