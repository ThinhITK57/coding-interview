import logging
import json
from typing import Dict, Any

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from transform.marts.base_mart import BaseMart

logger = logging.getLogger(__name__)


class MartKPIGapAnalysis(BaseMart):
    """Semantic Mart 5: Phân tích Khoảng cách Đạt Target Đa Kịch bản (KPI Gap Analysis).
    
    Phục vụ Ban Chiến lược & Quản lý KPI:
      - Chi tiết khoảng cách đạt mục tiêu (Gap Value, Gap %) theo từng kịch bản (M, N, S).
      - Đánh giá khả năng về đích, tự động phân nhóm rủi ro (`risk_category`).
      - Tính tỷ lệ cần nỗ lực cải thiện (`improvement_needed_pct`) và đối sánh chéo các kịch bản.
    """

    def __init__(self, spark_session, output_base: str = "./data/warehouse/marts"):
        super().__init__(spark_session, mart_name="mart_kpi_gap_analysis", output_base=output_base)

    def extract(self, dim_tables: Dict[str, Any], fact_tables: Dict[str, Any]):
        return {
            "dim_objective": dim_tables.get("dim_objective"),
            "dim_project": dim_tables.get("dim_project"),
            "dim_department": dim_tables.get("dim_department"),
            "dim_resource": dim_tables.get("dim_resource"),
            "fact_target_snapshot": fact_tables.get("fact_target_snapshot")
        }

    def build_mart(self, data: Dict[str, Any]):
        fact_tgt = data.get("fact_target_snapshot")
        dim_obj = data.get("dim_objective")
        dim_proj = data.get("dim_project")
        dim_dept = data.get("dim_department")
        dim_res = data.get("dim_resource")

        if fact_tgt is None:
            logger.warning("Không có fact_target_snapshot để build mart_kpi_gap_analysis")
            return None

        # 1. Base dữ liệu từ Fact Target
        base = fact_tgt

        # 2. Join Tên Mục tiêu BSC
        if dim_obj is not None and "objective_id" in dim_obj.columns:
            base = base.join(
                dim_obj.select("objective_id", "objective_name"),
                "objective_id",
                "left"
            )
        else:
            base = base.withColumn("objective_name", F.col("objective_id"))

        # 3. Join Tên Dự án
        if dim_proj is not None and "project_id" in dim_proj.columns:
            curr_proj = dim_proj.filter(F.col("is_current") == True) if "is_current" in dim_proj.columns else dim_proj
            base = base.join(
                curr_proj.select("project_id", "project_name"),
                "project_id",
                "left"
            )
        else:
            base = base.withColumn("project_name", F.col("project_id"))

        # 4. Join Tên Phòng ban & Người phụ trách
        if dim_dept is not None and "department_id" in dim_dept.columns:
            base = base.join(
                dim_dept.select("department_id", "department_name"),
                "department_id",
                "left"
            )
        else:
            base = base.withColumn("department_name", F.col("department_id"))

        if dim_res is not None and "resource_id" in dim_res.columns:
            base = base.join(
                dim_res.select(F.col("resource_id").alias("assignee_id"), F.col("resource_name").alias("assignee_name")),
                "assignee_id",
                "left"
            )
        else:
            base = base.withColumn("assignee_name", F.col("assignee_id"))

        # 5. Điền mặc định
        filled = base.fillna({
            "objective_name": "Chưa gắn mục tiêu",
            "project_name": "Chưa gắn dự án",
            "department_name": "Chưa xác định",
            "assignee_name": "Chưa phân công",
            "achievement_pct": 0.0,
            "gap_value": 0.0,
            "gap_pct": 0.0,
            "days_to_deadline": 999
        })

        # 6. Pre-computed Intelligence: Phân loại rủi ro & Mức độ cần cải thiện
        with_risk = filled.withColumn(
            # PHÂN LOẠI RỦI RO (Risk Category)
            "risk_category",
            F.when(F.col("is_achieved") == True, F.lit("ACHIEVED"))
             .when((F.col("achievement_pct") >= 80.0) & (F.col("days_to_deadline") > 14), F.lit("ON_TRACK"))
             .when((F.col("achievement_pct") >= 50.0) | (F.col("days_to_deadline") > 7), F.lit("AT_RISK"))
             .otherwise(F.lit("CRITICAL"))
        ).withColumn(
            # Tỷ lệ % cần nỗ lực thêm để đạt mục tiêu
            "improvement_needed_pct",
            F.when(
                F.col("is_achieved") == True, F.lit(0.0)
            ).otherwise(
                F.round(F.greatest(F.lit(0.0), F.lit(100.0) - F.col("achievement_pct")), 2)
            )
        )

        # 7. Window Functions: Thứ hạng gap trong cùng objective và trong department
        obj_gap_window = Window.partitionBy("objective_id", "scenario").orderBy(F.col("gap_value").desc())
        dept_achieve_window = Window.partitionBy("department_id", "scenario").orderBy(F.col("achievement_pct").desc())

        final_df = with_risk.withColumn(
            "gap_rank_in_objective", F.rank().over(obj_gap_window)
        ).withColumn(
            "dept_achievement_rank", F.rank().over(dept_achieve_window)
        ).withColumn(
            "_materialized_at", F.current_timestamp()
        )

        logger.info(json.dumps({
            "event": "mart_kpi_gap_analysis_built",
            "mart": self.mart_name
        }))
        return final_df
