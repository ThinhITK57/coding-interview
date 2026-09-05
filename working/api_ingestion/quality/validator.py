import logging
import json

logger = logging.getLogger(__name__)


class SchemaValidator:
    """
    Validates Spark DataFrame records against configurable schema rules.
    Compatible with Spark 2.3.2.

    Deep module design:
        Interface (small): validate(df) -> (valid_df, dlq_df)
        Implementation (deep): Rule engine with null checks, type conformance,
        range validation. Single-pass error tagging using concat_ws (no array_compact).
        Splits into valid and DLQ DataFrames without double-scanning.

    Rules format:
        [
            {"column": "sysid", "rule": "not_null"},
            {"column": "percent_completed", "rule": "range", "min": 0, "max": 100},
            {"column": "state", "rule": "in_set", "values": ["Active", "Completed"]},
        ]
    """

    def __init__(self, rules=None):
        """Initialize validator with quality rules.

        Args:
            rules: List of rule dicts. Each must have 'column' and 'rule' keys.
                Supported rules: 'not_null', 'range' (min/max), 'in_set' (values).
        """
        self._rules = rules or []

    def add_rule(self, column, rule_type, **kwargs):
        """Add a validation rule.

        Args:
            column: Column name to validate.
            rule_type: One of 'not_null', 'range', 'in_set'.
            **kwargs: Rule parameters (min, max for range; values for in_set).
        """
        rule = {"column": column, "rule": rule_type}
        rule.update(kwargs)
        self._rules.append(rule)

    def validate(self, df):
        """Validate DataFrame and split into valid and DLQ records.

        Uses single-pass error tagging via F.concat_ws() to avoid
        double-scanning the DataFrame. Compatible with Spark 2.3.2
        (no F.array_compact).

        Args:
            df: Input Spark DataFrame to validate.

        Returns:
            tuple: (valid_df, dlq_df) where:
                - valid_df: Records passing all rules (no _error_tags column)
                - dlq_df: Records failing at least one rule (with _error_tags column)
        """
        from pyspark.sql import functions as F

        if not self._rules:
            logger.info(json.dumps({
                "event": "validation_skipped",
                "reason": "no_rules_configured",
            }))
            return df, None

        # Build error expressions for each rule
        # Each expression returns either an error description or empty string
        error_expressions = []

        for rule in self._rules:
            col_name = rule["column"]
            rule_type = rule["rule"]

            if col_name not in df.columns:
                logger.warning(json.dumps({
                    "event": "validation_column_missing",
                    "column": col_name,
                    "rule": rule_type,
                }))
                continue

            if rule_type == "not_null":
                expr = F.when(
                    F.col(col_name).isNull(),
                    F.lit(f"{col_name}:null")
                ).otherwise(F.lit(""))
                error_expressions.append(expr)

            elif rule_type == "range":
                min_val = rule.get("min")
                max_val = rule.get("max")
                conditions = []
                if min_val is not None:
                    conditions.append(F.col(col_name) < min_val)
                if max_val is not None:
                    conditions.append(F.col(col_name) > max_val)
                if conditions:
                    combined = conditions[0]
                    for c in conditions[1:]:
                        combined = combined | c
                    expr = F.when(
                        combined,
                        F.lit(f"{col_name}:out_of_range")
                    ).otherwise(F.lit(""))
                    error_expressions.append(expr)

            elif rule_type == "in_set":
                valid_values = rule.get("values", [])
                if valid_values:
                    expr = F.when(
                        ~F.col(col_name).isin(valid_values) & F.col(col_name).isNotNull(),
                        F.lit(f"{col_name}:invalid_value")
                    ).otherwise(F.lit(""))
                    error_expressions.append(expr)

        if not error_expressions:
            return df, None

        # Single-pass: concat all error tags with semicolon separator
        # Empty strings are automatically omitted by concat_ws
        df_tagged = df.withColumn(
            "_error_tags",
            F.concat_ws(";", *error_expressions)
        )

        # Split: valid records have empty error tags
        valid_df = (
            df_tagged
            .filter(F.length(F.col("_error_tags")) == 0)
            .drop("_error_tags")
        )

        dlq_df = df_tagged.filter(F.length(F.col("_error_tags")) > 0)

        # Log counts (Spark 2.3.2: use count() directly)
        valid_count = valid_df.count()
        dlq_count = dlq_df.count()

        logger.info(json.dumps({
            "event": "validation_complete",
            "total_records": valid_count + dlq_count,
            "valid_records": valid_count,
            "dlq_records": dlq_count,
            "rules_applied": len(error_expressions),
        }))

        # Return None for dlq_df if empty (Spark 2.3.2: use rdd.isEmpty())
        if dlq_df.rdd.isEmpty():
            dlq_df = None

        return valid_df, dlq_df
