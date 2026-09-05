import json
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class QualityReport:
    """
    Data quality report combining validation results and statistical profiles.

    Deep module design:
        Interface (small): from_results(...), to_markdown(), to_dict()
        Implementation (deep): Aggregates validation counts, column statistics,
        and DLQ information into a structured report. Generates markdown
        tables for Prefect artifacts and logging.
    """

    def __init__(
        self,
        endpoint_name,
        total_records=0,
        valid_records=0,
        dlq_records=0,
        column_stats=None,
        validation_errors=None,
        batch_id="",
    ):
        """Initialize quality report.

        Args:
            endpoint_name: API endpoint that was ingested.
            total_records: Total records processed.
            valid_records: Records passing all validation rules.
            dlq_records: Records sent to Dead Letter Queue.
            column_stats: Dict of per-column statistics from StatisticsProfiler.
            validation_errors: List of validation error descriptions.
            batch_id: Unique batch identifier.
        """
        self.endpoint_name = endpoint_name
        self.total_records = total_records
        self.valid_records = valid_records
        self.dlq_records = dlq_records
        self.column_stats = column_stats or {}
        self.validation_errors = validation_errors or []
        self.batch_id = batch_id
        self.timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    @classmethod
    def from_results(cls, endpoint_name, profile_result, valid_count, dlq_count, batch_id=""):
        """Create report from profiler results and validation counts.

        Args:
            endpoint_name: API endpoint name.
            profile_result: Dict from StatisticsProfiler.profile().
            valid_count: Number of valid records.
            dlq_count: Number of DLQ records.
            batch_id: Batch identifier.

        Returns:
            QualityReport instance.
        """
        return cls(
            endpoint_name=endpoint_name,
            total_records=profile_result.get("total_records", 0),
            valid_records=valid_count,
            dlq_records=dlq_count,
            column_stats=profile_result.get("columns", {}),
            batch_id=batch_id,
        )

    def to_markdown(self):
        """Generate markdown report for Prefect artifacts or logging.

        Returns:
            str: Formatted markdown string with summary table and column stats.
        """
        lines = []
        lines.append(f"## \U0001f4ca Data Quality Report — {self.endpoint_name}")
        lines.append("")

        # Summary table
        lines.append("| Metric | Value |")
        lines.append("|:---|:---|")
        lines.append(f"| **Endpoint** | `{self.endpoint_name}` |")
        lines.append(f"| **Batch ID** | `{self.batch_id}` |")
        lines.append(f"| **Timestamp** | {self.timestamp} |")
        lines.append(f"| **Total Records** | {self.total_records:,} |")
        lines.append(f"| **Valid Records** | {self.valid_records:,} |")
        lines.append(f"| **DLQ Records** | {self.dlq_records:,} |")

        if self.total_records > 0:
            dlq_ratio = (self.dlq_records / self.total_records) * 100
            lines.append(f"| **DLQ Ratio** | {dlq_ratio:.2f}% |")

        lines.append("")

        # Column statistics table
        if self.column_stats:
            lines.append("### Column Statistics")
            lines.append("")
            lines.append("| Column | Null % | Distinct | Min | Max | Mean |")
            lines.append("|:---|:---|:---|:---|:---|:---|")

            for col_name, stats in self.column_stats.items():
                null_pct = f"{stats.get('null_ratio', 0) * 100:.2f}%"
                distinct = str(stats.get("distinct_count", "—"))
                min_val = str(stats.get("min", "—")) if stats.get("min") is not None else "—"
                max_val = str(stats.get("max", "—")) if stats.get("max") is not None else "—"
                mean_val = str(stats.get("mean", "—")) if stats.get("mean") is not None else "—"

                # Truncate long values
                min_val = min_val[:20] + "..." if len(min_val) > 20 else min_val
                max_val = max_val[:20] + "..." if len(max_val) > 20 else max_val

                lines.append(f"| {col_name} | {null_pct} | {distinct} | {min_val} | {max_val} | {mean_val} |")

        lines.append("")

        # Log summary line
        summary = (
            f"Ingested {self.total_records:,} records. "
            f"Valid: {self.valid_records:,}. "
            f"DLQ: {self.dlq_records:,}."
        )
        lines.append(f"> {summary}")

        return "\n".join(lines)

    def to_dict(self):
        """Serialize report to dictionary."""
        return {
            "endpoint_name": self.endpoint_name,
            "batch_id": self.batch_id,
            "timestamp": self.timestamp,
            "total_records": self.total_records,
            "valid_records": self.valid_records,
            "dlq_records": self.dlq_records,
            "column_stats": self.column_stats,
            "validation_errors": self.validation_errors,
        }

    def log_summary(self):
        """Log a structured JSON summary of the quality report."""
        logger.info(json.dumps({
            "event": "quality_report",
            "endpoint": self.endpoint_name,
            "total_records": self.total_records,
            "valid_records": self.valid_records,
            "dlq_records": self.dlq_records,
            "batch_id": self.batch_id,
        }))
