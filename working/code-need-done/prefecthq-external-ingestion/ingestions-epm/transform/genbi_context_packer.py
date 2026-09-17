import os
import json
import logging

from typing import Any, Dict, List, Optional


logger = logging.getLogger(__name__)


class GenBIContextPacker:
    def __init__(self, trino_catalog: str = "hive", clean_schema: str = "global_clean"):
        self.catalog = trino_catalog
        self.schema = clean_schema

    def build_context_pack(
            self, table_name: str, columns_meta: Dict[str, Dict[str, Any]], 
            table_description: Optional[str] = None, sample_rows: Optional[List[Dict[str, Any]]] = None
    )-> Dict[str, Any]:
        full_table_name = f"{self.catalog}.{self.schema}.{table_name.lower()}"

        dimensions = []
        metrics = []
        time_dimensions = []

        for col_name, meta in columns_meta.items():
            if col_name.startswith("_") and col_name not in ["_batch_id"]:
                continue
            chart_role = meta.get("chart_role", "dimension")
            entry = {
                "name": col_name,
                "data_type": meta.get("data_type", "varchar"),
                "description": meta.get("description", col_name),
                "synonyms": meta.get("synonyms", [col_name])
            }

            if chart_role == "y_axis" or meta.get("aggregation_type") in ["sum", "avg"]:
                entry["aggregation_type"] = meta.get("aggregation_type", "sum")
                entry["chart_role"] = "y_axis"
                metrics.append(entry)

            elif chart_role == "x_axis" and any(k in col_name.lower() for k in ["date", "time"]):
                entry["chart_role"] = "x_axis"
                time_dimensions.append(entry)

            else:
                entry["chart_role"] = "x_axis" if chart_role == "x_axis" else "dimension"
                dimensions.append(entry)

        few_shots = self._build_few_shot_examples(table_name, full_table_name, dimensions, metrics)
        chart_recommendations = self._build_chart_recommendations(table_name, dimensions, metrics, time_dimensions)

        context_pack = {
            "metadata_version": "2.0", 
            "table_name": table_name,
            "trino_table_ref": full_table_name,
            "table_description": "",
            "semantic_layer": {
                "metrics": metrics,
                "dimensions": dimensions,
                "time_dimensions": time_dimensions
            }
        }

        return context_pack

    def _build_few_shot_examples(
            self, table_name, full_table_name: str, dimensions: List[Dict[str, Any]], metrics: List[Dict[str, Any]]
    )-> List[Dict[str, Any]]:
        examples = []

        # make code for creating sample 
        return examples

    def _build_chart_recommendations(
            self, table_name: str, dimensions: List[Dict[str, Any]], metrics: List[Dict[str, Any]], 
            time_dimensions: List[Dict[str, Any]]
    )-> List[Dict[str, Any]]:
        charts = []

        ## make some code for creating sample chart
        return charts

    def export_pack(
            self, 
            output_path: str,
            table_name: str,
            columns_meta: Dict[str, Dict[str, Any]],
            table_description: Optional[str] = None,
            sample_rows: Optional[List[Dict[str, Any]]] = None
    ) -> str:
        """return path to generated JSON context pack"""

        if os.path.isdir(output_path) or not output_path.endswith("*.json"):
            os.makedirs(output_path, exist_ok=True)
            file_path = os.path.join(output_path, f"genbi_context_pack_{table_name.lower()}.json")
        else:
            os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
            file_path = output_path

        pack = self.build_context_pack(
            table_name=table_name,
            columns_meta=columns_meta,
            table_description=table_description, 
            sample_rows=sample_rows
        )

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(pack, f, indent=2, ensure_ascii=False)

        logger.info(json.dumps({
            "event": "genbi_context_pack_exported",
            "file_path": file_path,
            "table_name": table_name,
            "metrics_count": len(pack["semantic_layer"]["dimensions"])
        }))

        return file_path