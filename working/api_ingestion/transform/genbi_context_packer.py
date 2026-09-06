import os
import json
import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class GenBIContextPacker:
    """
    GenBI Knowledge Context Packer for AI LLM Semantic Reasoning.

    Transforms dbt / Lakehouse table metadata into a structured JSON knowledge pack
    specifically designed to be fed into LLM prompts (OpenAI, Gemini, Claude, Local LLMs).

    Enables the LLM to generate dual-output answers:
        1. Executable SQL queries on Trino / Hive.
        2. Natural language business text insights.
        3. Dynamic visualization specifications (Apache ECharts / Chart.js options).

    Deep module design:
        Interface: build_context_pack(table_name, columns_meta, ...), export_pack(path, ...)
        Implementation: Categorizes dimensions vs metrics, maps synonyms, builds few-shot
        SQL examples, and synthesizes ready-to-render ECharts templates.
    """

    def __init__(
        self,
        trino_catalog: str = "hive",
        clean_schema: str = "global_clean",
    ):
        """Initialize GenBI context packer.

        Args:
            trino_catalog: Trino catalog name (default: "hive").
            clean_schema: Trino schema for analytics-ready data (default: "global_clean").
        """
        self.catalog = trino_catalog
        self.schema = clean_schema

    def build_context_pack(
        self,
        table_name: str,
        columns_meta: Dict[str, Dict[str, Any]],
        table_description: Optional[str] = None,
        sample_rows: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """Build full semantic knowledge pack from registered columns metadata.

        Args:
            table_name: Target table name (e.g. "tasks", "projects").
            columns_meta: Dict from DocstringRegistry._columns containing rich meta tags.
            table_description: Business description of the table.
            sample_rows: Optional sample records to give the LLM concrete value examples.

        Returns:
            dict: Structured GenBI Context Pack ready for AI ingestion.
        """
        full_table_name = f"{self.catalog}.{self.schema}.{table_name.lower()}"

        dimensions = []
        metrics = []
        time_dimensions = []

        for col_name, meta in columns_meta.items():
            # Skip internal metadata columns from user queries
            if col_name.startswith("_") and col_name not in ["_batch_id"]:
                continue

            chart_role = meta.get("chart_role", "dimension")
            entry = {
                "name": col_name,
                "data_type": meta.get("data_type", "varchar"),
                "description": meta.get("description", col_name),
                "synonyms": meta.get("synonyms", [col_name]),
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

        # Build few-shot examples for LLM prompt grounding
        few_shots = self._build_few_shot_examples(table_name, full_table_name, dimensions, metrics)

        # Build recommended visualization templates
        chart_recommendations = self._build_chart_recommendations(table_name, dimensions, metrics, time_dimensions)

        context_pack = {
            "metadata_version": "2.0",
            "table_name": table_name,
            "trino_table_ref": full_table_name,
            "table_description": table_description or f"Curated analytics table for {table_name}",
            "semantic_layer": {
                "metrics": metrics,
                "dimensions": dimensions,
                "time_dimensions": time_dimensions,
            },
            "sample_data": sample_rows[:3] if sample_rows else [],
            "chart_recommendations": chart_recommendations,
            "few_shot_prompt_examples": few_shots,
            "llm_system_instructions": {
                "role": "Expert Data Analyst & BI Specialist",
                "rules": [
                    f"Always query from table {full_table_name}",
                    "Use Trino/Presto compatible SQL syntax",
                    "Return response in JSON with keys: 'sql', 'text_insights', 'visualization'",
                    "In 'visualization', provide valid ECharts option JSON matching user intent",
                ],
                "expected_response_format": {
                    "sql": "SELECT ... FROM ... GROUP BY ...",
                    "text_insights": "Phân tích số liệu tổng hợp...",
                    "visualization": {
                        "type": "bar | line | pie",
                        "title": "Tiêu đề biểu đồ",
                        "echarts_option": {
                            "tooltip": {"trigger": "axis"},
                            "xAxis": {"type": "category", "data": []},
                            "yAxis": {"type": "value"},
                            "series": [{"data": [], "type": "bar"}]
                        }
                    }
                }
            }
        }

        return context_pack

    def _build_few_shot_examples(
        self,
        table_name: str,
        full_table_name: str,
        dimensions: List[Dict[str, Any]],
        metrics: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """Generate representative few-shot question-SQL-chart pairs."""
        examples = []

        dim_name = dimensions[0]["name"] if dimensions else "name"
        metric_name = metrics[0]["name"] if metrics else "sysid"
        metric_agg = metrics[0].get("aggregation_type", "count") if metrics else "count"

        # Example 1: Grouped metric
        examples.append({
            "user_query": f"Thống kê {metric_name} theo từng {dim_name}",
            "expected_sql": f"SELECT {dim_name}, {metric_agg.upper()}({metric_name}) AS metric_val FROM {full_table_name} GROUP BY {dim_name} ORDER BY metric_val DESC LIMIT 10",
            "text_summary": f"Biểu đồ phân bổ {metric_name} theo {dim_name}",
            "chart_type": "bar",
        })

        # Example 2: Overall count
        examples.append({
            "user_query": f"Hiện tại có bao nhiêu {table_name} trong hệ thống?",
            "expected_sql": f"SELECT COUNT(*) AS total_count FROM {full_table_name}",
            "text_summary": f"Tổng số lượng bản ghi {table_name} đang được ghi nhận.",
            "chart_type": "kpi_card",
        })

        return examples

    def _build_chart_recommendations(
        self,
        table_name: str,
        dimensions: List[Dict[str, Any]],
        metrics: List[Dict[str, Any]],
        time_dimensions: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """Generate ECharts templates for standard BI questions."""
        charts = []

        # 1. Bar Chart (Categorical)
        if dimensions and metrics:
            dim_col = dimensions[0]["name"]
            metric_col = metrics[0]["name"]
            charts.append({
                "chart_type": "bar",
                "title": f"Phân bổ {metrics[0]['description']} theo {dimensions[0]['description']}",
                "x_axis_field": dim_col,
                "y_axis_field": metric_col,
                "echarts_template": {
                    "title": {"text": f"{table_name.title()} Distribution"},
                    "tooltip": {"trigger": "axis"},
                    "xAxis": {"type": "category", "data": "$X_VALUES"},
                    "yAxis": {"type": "value"},
                    "series": [{"name": metric_col, "type": "bar", "data": "$Y_VALUES"}]
                }
            })

        # 2. Line Chart (Temporal Trend)
        if time_dimensions and metrics:
            time_col = time_dimensions[0]["name"]
            metric_col = metrics[0]["name"]
            charts.append({
                "chart_type": "line",
                "title": f"Xu hướng {metrics[0]['description']} theo thời gian ({time_dimensions[0]['description']})",
                "x_axis_field": time_col,
                "y_axis_field": metric_col,
                "echarts_template": {
                    "title": {"text": f"{table_name.title()} Trend Over Time"},
                    "tooltip": {"trigger": "axis"},
                    "xAxis": {"type": "category", "data": "$X_VALUES"},
                    "yAxis": {"type": "value"},
                    "series": [{"name": metric_col, "type": "line", "smooth": True, "data": "$Y_VALUES"}]
                }
            })

        # 3. Pie Chart (Status Distribution)
        for dim in dimensions:
            if any(k in dim["name"].lower() for k in ["state", "status", "phase", "type"]):
                charts.append({
                    "chart_type": "pie",
                    "title": f"Tỷ lệ {table_name} theo {dim['description']}",
                    "category_field": dim["name"],
                    "echarts_template": {
                        "title": {"text": f"{dim['name'].title()} Breakdown"},
                        "tooltip": {"trigger": "item"},
                        "series": [{"type": "pie", "radius": "60%", "data": "$PIE_SERIES_DATA"}]
                    }
                })
                break

        return charts

    def export_pack(
        self,
        output_path: str,
        table_name: str,
        columns_meta: Dict[str, Dict[str, Any]],
        table_description: Optional[str] = None,
        sample_rows: Optional[List[Dict[str, Any]]] = None,
    ) -> str:
        """Build and write genbi_context_pack.json to disk.

        Args:
            output_path: Target directory or file path.
            table_name: Table name.
            columns_meta: Columns metadata dict from DocstringRegistry.
            table_description: Table description.
            sample_rows: Sample rows for context grounding.

        Returns:
            str: Path to the generated JSON context pack.
        """
        if os.path.isdir(output_path) or not output_path.endswith(".json"):
            os.makedirs(output_path, exist_ok=True)
            file_path = os.path.join(output_path, f"genbi_context_pack_{table_name.lower()}.json")
        else:
            os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
            file_path = output_path

        pack = self.build_context_pack(
            table_name=table_name,
            columns_meta=columns_meta,
            table_description=table_description,
            sample_rows=sample_rows,
        )

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(pack, f, indent=2, ensure_ascii=False)

        logger.info(json.dumps({
            "event": "genbi_context_pack_exported",
            "file_path": file_path,
            "table_name": table_name,
            "metrics_count": len(pack["semantic_layer"]["metrics"]),
            "dimensions_count": len(pack["semantic_layer"]["dimensions"]),
        }))

        return file_path
