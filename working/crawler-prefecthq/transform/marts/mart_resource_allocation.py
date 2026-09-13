import logging
import json
from typing import Dict, Any

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from transform.marts.base_mart import BaseMart

logger = logging.getLogger(__name__)


class MartResourceAllocation(BaseMart):
    """Semantic Mart 4: Ma trận Phân bổ Nguồn lực & Tải Nhân sự (Workload Matrix).
    
    Phục vụ HR, PMO & Giám đốc Khối:
      - Đo lường mức độ quá tải / non tải của từng nhân sự (`workload_score`, `workload_flag`).
      - Tính toán hiệu suất sử dụng công sức (`work_utilization_pct`), tỷ lệ trễ hạn cá nhân,
        và độ dàn trải qua nhiều dự án (`projects_involved`).
    """

    def __init__(self, spark_session, output_base: str = "./data/warehouse/marts"):
        super().__init__(spark_session, mart_name="mart_resource_allocation", output_base=output_base)

    def extract(self, dim_tables: Dict[str, Any], fact_tables: Dict[str, Any]):
        return {
            "dim_resource": dim_tables.get("dim_resource"),
            "dim_department": dim_tables.get("dim_department"),
            "fact_task_execution": fact_tables.get("fact_task_execution"),
            "fact_target_snapshot": fact_tables.get("fact_target_snapshot")
        }

    def build_mart(self, data: Dict[str, Any]):
        dim_res = data.get("dim_resource")
        dim_dept = data.get("dim_department")
        fact_t = data.get("fact_task_execution")
        fact_tgt = data.get("fact_target_snapshot")

        if dim_res is None:
            logger.warning("Không có dim_resource để build mart_resource_allocation")
            return None

        # 1. Tổng hợp số liệu Task theo assignee_id
        if fact_t is not None and "assignee_id" in fact_t.columns:
            task_agg = fact_t.filter(
                F.col("assignee_id").isNotNull() & (F.col("assignee_id") != "UNKNOWN")
            ).groupBy("assignee_id").agg(
                F.count(F.lit(1)).alias("total_assigned_tasks"),
                F.sum(F.when(F.col("percent_completed") < 100.0, 1).otherwise(0)).alias("active_tasks"),
                F.sum(F.when(F.col("percent_completed") >= 100.0, 1).otherwise(0)).alias("completed_tasks"),
                F.sum(F.when(F.col("is_overdue") == True, 1).otherwise(0)).alias("overdue_tasks"),
                F.round(F.sum("work_planned"), 2).alias("total_work_planned"),
                F.round(F.sum("actual_effort"), 2).alias("total_work_actual"),
                F.countDistinct("project_id").alias("projects_involved")
            )
        else:
            task_agg = dim_res.select(F.col("resource_id").alias("assignee_id")).withColumn(
                "total_assigned_tasks", F.lit(0)
            ).withColumn(
                "active_tasks", F.lit(0)
            ).withColumn(
                "completed_tasks", F.lit(0)
            ).withColumn(
                "overdue_tasks", F.lit(0)
            ).withColumn(
                "total_work_planned", F.lit(0.0)
            ).withColumn(
                "total_work_actual", F.lit(0.0)
            ).withColumn(
                "projects_involved", F.lit(0)
            )

        # 2. Tổng hợp số liệu Target theo assignee_id
        if fact_tgt is not None and "assignee_id" in fact_tgt.columns:
            tgt_agg = fact_tgt.filter(
                F.col("assignee_id").isNotNull() & (F.col("assignee_id") != "UNKNOWN")
            ).groupBy("assignee_id").agg(
                F.countDistinct("target_id").alias("targets_assigned"),
                F.sum(F.when((F.col("scenario") == "M") & (F.col("is_achieved") == True), 1).otherwise(0)).alias("achieved_targets_m")
            )
        else:
            tgt_agg = dim_res.select(F.col("resource_id").alias("assignee_id")).withColumn(
                "targets_assigned", F.lit(0)
            ).withColumn(
                "achieved_targets_m", F.lit(0)
            )

        # 3. Join với dim_resource
        joined = dim_res.join(
            task_agg,
            dim_res["resource_id"] == task_agg["assignee_id"],
            "left"
        ).join(
            tgt_agg,
            "assignee_id",
            "left"
        )

        # 4. Điền mặc định
        filled = joined.fillna({
            "total_assigned_tasks": 0, "active_tasks": 0, "completed_tasks": 0,
            "overdue_tasks": 0, "total_work_planned": 0.0, "total_work_actual": 0.0,
            "projects_involved": 0, "targets_assigned": 0, "achieved_targets_m": 0
        })

        # 5. Pre-computed Metrics
        with_metrics = filled.withColumn(
            # Tỷ lệ tiêu hao công sức (Work Utilization %)
            "work_utilization_pct",
            F.when(
                F.col("total_work_planned") > 0.0,
                F.round((F.col("total_work_actual") / F.col("total_work_planned")) * 100.0, 2)
            ).otherwise(F.lit(100.0))
        ).withColumn(
            # Tỷ lệ trễ hạn cá nhân
            "overdue_ratio",
            F.when(
                F.col("total_assigned_tasks") > 0,
                F.round(F.col("overdue_tasks") / F.col("total_assigned_tasks"), 4)
            ).otherwise(F.lit(0.0))
        ).withColumn(
            # Tỷ lệ đạt KPI
            "target_achievement_rate",
            F.when(
                F.col("targets_assigned") > 0,
                F.round((F.col("achieved_targets_m") / F.col("targets_assigned")) * 100.0, 2)
            ).otherwise(F.lit(100.0))
        ).withColumn(
            # WORKLOAD SCORE (0-100): 35% Active Tasks + 25% Công sức + 20% Trễ hạn + 20% Dàn trải nhiều dự án
            "workload_score",
            F.round(
                (F.least(F.col("active_tasks"), F.lit(10)) * 10.0 * 0.35) +
                (F.least(F.col("work_utilization_pct"), F.lit(150.0)) * (100.0 / 150.0) * 0.25) +
                (F.col("overdue_ratio") * 100.0 * 0.20) +
                (F.least(F.col("projects_involved"), F.lit(5)) * 20.0 * 0.20),
                2
            )
        ).withColumn(
            # CỜ TẢI NHÂN SỰ
            "workload_flag",
            F.when(F.col("workload_score") > 75.0, F.lit("OVERLOADED"))
             .when(F.col("workload_score") >= 35.0, F.lit("BALANCED"))
             .otherwise(F.lit("UNDERUTILIZED"))
        )

        # 6. Window Ranking toàn công ty
        company_window = Window.orderBy(F.col("workload_score").desc())
        final_df = with_metrics.withColumn(
            "company_workload_rank", F.rank().over(company_window)
        ).withColumn(
            "_materialized_at", F.current_timestamp()
        )

        logger.info(json.dumps({
            "event": "mart_resource_allocation_built",
            "mart": self.mart_name
        }))
        return final_df
