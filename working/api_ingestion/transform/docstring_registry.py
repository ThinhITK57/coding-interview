import os
import json
import logging
from collections import OrderedDict

logger = logging.getLogger(__name__)


class DocstringRegistry:
    """
    Column metadata registry that generates dbt schema.yml for genBI.

    Deep module design:
        Interface (small): register_columns(df, flatten_map), export_dbt_schema(path)
        Implementation (deep): Auto-detects columns from DataFrame schema,
        merges with flatten_map from JSONFlattener, generates dbt-compatible
        YAML with column descriptions and meta tags, warns on unknown columns.

    Output format: dbt schema.yml with columns[].description + columns[].meta
    consumed by genBI system.
    """

    def __init__(self, source_name, endpoint_name, api_version=""):
        """Initialize registry.

        Args:
            source_name: Data source name (e.g. "clarizen").
            endpoint_name: API endpoint name (e.g. "tasks").
            api_version: API version string (e.g. "v2.0").
        """
        self._source_name = source_name
        self._endpoint_name = endpoint_name
        self._api_version = api_version
        self._columns = OrderedDict()  # column_name -> metadata dict

    def register_columns(self, df, flatten_map=None):
        """Register columns from a Spark DataFrame schema.

        Auto-detects data types from the DataFrame schema and merges
        with flatten_map to record original JSON source paths.

        Args:
            df: Spark DataFrame with flattened columns.
            flatten_map: Optional dict mapping {col_name: original_json_path}.
        """
        flatten_map = flatten_map or {}

        for field in df.schema.fields:
            col_name = field.name

            if col_name in self._columns:
                continue  # Already registered

            source_field = flatten_map.get(col_name, col_name)
            flatten_path = None
            if "." in source_field or "[" in source_field:
                flatten_path = source_field

            self._columns[col_name] = {
                "description": self._generate_description(col_name, source_field),
                "data_type": str(field.dataType.simpleString()),
                "nullable": field.nullable,
                "source_field": source_field,
                "api_endpoint": self._endpoint_name,
                "flatten_path": flatten_path,
                "business_meaning": "",  # To be filled by data team
            }

        new_columns = [
            name for name in df.schema.fieldNames()
            if name not in self._columns
        ]
        if new_columns:
            logger.warning(json.dumps({
                "event": "docstring_unknown_columns",
                "columns": new_columns,
                "endpoint": self._endpoint_name,
            }))

        logger.info(json.dumps({
            "event": "docstring_columns_registered",
            "total_columns": len(self._columns),
            "endpoint": self._endpoint_name,
        }))

    def update_column(self, col_name, description=None, business_meaning=None):
        """Manually update a column's description or business meaning.

        Args:
            col_name: Column name to update.
            description: Human-readable column description.
            business_meaning: Business context for genBI.
        """
        if col_name not in self._columns:
            self._columns[col_name] = {
                "description": description or "",
                "data_type": "unknown",
                "nullable": True,
                "source_field": col_name,
                "api_endpoint": self._endpoint_name,
                "flatten_path": None,
                "business_meaning": business_meaning or "",
            }
        else:
            if description is not None:
                self._columns[col_name]["description"] = description
            if business_meaning is not None:
                self._columns[col_name]["business_meaning"] = business_meaning

    def export_dbt_schema(self, output_path):
        """Export column metadata as dbt schema.yml file.

        Generates a valid dbt schema.yml with:
        - Model name: bronze_{source}_{endpoint}
        - Model description with source and API version
        - Column descriptions
        - Column meta tags (source_field, data_type, nullable, etc.)

        Args:
            output_path: File path to write the schema.yml.
        """
        model_name = f"bronze_{self._source_name}_{self._endpoint_name}"

        columns_yaml = []
        for col_name, meta in self._columns.items():
            col_entry = {
                "name": col_name,
                "description": meta["description"],
                "meta": {
                    "source_field": meta["source_field"],
                    "api_endpoint": meta["api_endpoint"],
                    "data_type": meta["data_type"],
                    "nullable": meta["nullable"],
                },
            }
            if meta.get("flatten_path"):
                col_entry["meta"]["flatten_path"] = meta["flatten_path"]
            if meta.get("business_meaning"):
                col_entry["meta"]["business_meaning"] = meta["business_meaning"]
            columns_yaml.append(col_entry)

        schema = {
            "version": 2,
            "models": [
                {
                    "name": model_name,
                    "description": (
                        f"Raw flattened data from {self._source_name} "
                        f"{self._endpoint_name} API endpoint "
                        f"(API {self._api_version})"
                    ),
                    "columns": columns_yaml,
                }
            ],
        }

        # Write YAML manually to avoid PyYAML dependency
        # (stdlib only per project rules)
        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
        yaml_content = self._dict_to_yaml(schema)

        with open(output_path, "w", encoding="utf-8") as f:
            f.write(yaml_content)

        logger.info(json.dumps({
            "event": "dbt_schema_exported",
            "path": output_path,
            "model_name": model_name,
            "total_columns": len(columns_yaml),
        }))

    def _generate_description(self, col_name, source_field):
        """Auto-generate a human-readable description from column name."""
        # Convert snake_case to readable words
        words = col_name.replace("_", " ").title()
        if source_field != col_name:
            return f"{words} (from API field: {source_field})"
        return words

    def _dict_to_yaml(self, data, indent=0):
        """Convert Python dict/list to YAML string without PyYAML dependency.

        Handles nested dicts, lists, strings, booleans, and None values.
        Produces valid YAML parseable by dbt.
        """
        lines = []
        prefix = "  " * indent

        if isinstance(data, dict):
            for key, value in data.items():
                if isinstance(value, dict):
                    lines.append(f"{prefix}{key}:")
                    lines.append(self._dict_to_yaml(value, indent + 1))
                elif isinstance(value, list):
                    lines.append(f"{prefix}{key}:")
                    for item in value:
                        if isinstance(item, dict):
                            # First key on same line as dash
                            items_iter = iter(item.items())
                            first_key, first_val = next(items_iter)
                            if isinstance(first_val, (dict, list)):
                                lines.append(f"{prefix}  - {first_key}:")
                                lines.append(self._dict_to_yaml(first_val, indent + 3))
                            else:
                                lines.append(f"{prefix}  - {first_key}: {self._format_yaml_value(first_val)}")
                            for k, v in items_iter:
                                if isinstance(v, (dict, list)):
                                    lines.append(f"{prefix}    {k}:")
                                    lines.append(self._dict_to_yaml(v, indent + 3))
                                else:
                                    lines.append(f"{prefix}    {k}: {self._format_yaml_value(v)}")
                        else:
                            lines.append(f"{prefix}  - {self._format_yaml_value(item)}")
                else:
                    lines.append(f"{prefix}{key}: {self._format_yaml_value(value)}")

        return "\n".join(lines)

    def _format_yaml_value(self, value):
        """Format a scalar value for YAML output."""
        if value is None:
            return "null"
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, (int, float)):
            return str(value)
        # String: quote if contains special chars
        s = str(value)
        if any(c in s for c in ':{}[]&*?|-><!%@`#,') or s.startswith(('"', "'")):
            # Escape double quotes inside and wrap
            escaped = s.replace('"', '\\"')
            return f'"{escaped}"'
        return f'"{s}"'

    def get_columns_metadata(self):
        """Return all registered column metadata.

        Returns:
            dict: {col_name: {description, data_type, ...}, ...}
        """
        return dict(self._columns)
