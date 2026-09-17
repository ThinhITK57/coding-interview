import logging
import json


logger = logging.getLogger(__name__)

class SchemaValidator:
    def __init__(self, rules=None):
        self._rules = rules or []

    def add_rule(self, column, rule_type, **kwargs):
        rule = {"column": column, "rule": rule_type}
        rule.update(kwargs)
        self._rules.append(rule)

    def validate(self, df):
        from pyspark.sql import functions as F

        if not self._rules:
            logger.info(json.dumps({
                "event": "validation_skipped",
                "reason": "no_rules_configured"
            }))
            return df, None

        error_expressions = []

        for rule in self._rules:
            col_name = rule["column"]
            rule_type = rule["rule"]

            if col_name not in df.columns:
                logger.warning(json.dumps({
                    "event": "validation_column_missing",
                    "column": col_name,
                    "rule": rule_type
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
                        ~F.col(col_name).isin(valid_values) & F.col(col_name).isNotNull(), F.lit(f"{col_name}:invalid_value")
                    ).otherwise(F.lit(""))
                    error_expressions.append(expr)

        if not error_expressions:
            return df, None

        df_tagged = df.withColumn(
            "_error_tags",
            F.concat_ws(";", *error_expressions)
        )

        valid_df = (
            df_tagged
            .filter(F.length(F.col("_error_tags")) == 0)
            .drop("_error_tags")
        )
        dlq_df = df_tagged.filter(F.length(F.col("_error_tags")) > 0)

        valid_count = valid_df.count()
        dlq_count = dlq_df.count()

        logger.info(json.dumps({
            "event": "validation_complete",
            "total_records": valid_count + dlq_count,
            "valid_records": valid_count,
            "dlq_records": dlq_count,
            "rules_applied": len(error_expressions)
        }))

        if dlq_df.rdd.isEmpty():
            dlq_df = None
        return valid_df, dlq_df
        