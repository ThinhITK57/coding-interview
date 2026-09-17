import json
import logging 
from datetime import datetime


logger = logging.getLogger(__name__)



class QualityReport:
    def __init__(
            self,
            endpoint_name,
            total_records=0,
            valid_records=0,
            dlq_records=0,
            column_stats=None,
            validation_errors=None,
            batch_id=""
        ):
        self.endpoint_name = endpoint_name
        self.total_records = total_records
        self.valid_records = valid_records
        self.dlq_records = dlq_records
        self.column_stats = column_stats
        self.validation_errors = validation_errors or []
        self.batch_id = batch_id
        self.timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


    @classmethod
    def from_results(cls, endpoint_name, profile_result, valid_count, dlq_count, batch_id=""):
        return cls(
            endpoint_name=endpoint_name,
            total_records=profile_result.get("total_records", 0),
            valid_records=valid_count,
            dlq_records=dlq_count,
            column_stats=profile_result.get("columns", {}),
            batch_id=batch_id
        )

    def to_markdown(self):
        pass

    def to_dict(self):
        return {
            "endpoint_name": self.endpoint_name,
            "batch_id": self.batch_id,
            "timestamp": self.timestamp,
            "total_records": self.total_records,
            "valid_records": self.valid_records,
            "dlq_records": self.dlq_records,
            "column_stats": self.column_stats,
            "validation_errors": self.validation_errors
        }

    def log_summary(self):
        logger.info(json.dumps({
            "event": "quality_report",
            "endpoint": self.endpoint_name,
            "total_records": self.total_records,
            "valid_records": self.valid_records,
            "dlq_records": self.dlq_records,
            "batch_id": self.batch_id
        }))

