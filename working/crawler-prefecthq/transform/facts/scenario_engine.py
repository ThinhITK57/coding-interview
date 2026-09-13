import logging
import json
from typing import List, Tuple, Any

from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


class ScenarioEngine:
    """Bộ máy xử lý Đa kịch bản (Scenario Engine) cho Target / KPI trong Clarizen.
    
    Trong Clarizen, mỗi Target được khai báo theo 3 kịch bản theo chiều ngang (Wide format):
      - Kịch bản M (Must-have / Bắt buộc đạt)
      - Kịch bản N (Normal / Dự kiến thông thường)
      - Kịch bản S/C (Stretch, Challenge / Kịch bản thách thức)
      
    ScenarioEngine thực hiện:
      1. Unpivot dạng ngang (wide) sang dạng dọc (long) tương thích PySpark 2.3.2.
      2. Tính toán tự động các chỉ số khoảng cách (Gap Analysis), tỷ lệ đạt, và cờ rủi ro.
    """

    SCENARIO_SPECS: List[Tuple[str, str, str, str]] = [
        ("M", "c_target_value_m", "c_target_result_value_m", "c_target_date_m"),
        ("N", "c_target_value_n", "c_target_result_value_n", "c_target_date_n"),
        ("S", "c_target_value_s", "c_target_result_value_s", "c_target_date_s"),
    ]

    def unpivot(self, df, common_cols: List[str]):
        """Chuyển đổi các cột kịch bản M/N/S thành các dòng riêng biệt.
        
        Sử dụng kỹ thuật Union các tập con có lọc dữ liệu (PySpark 2.3.2 compatible),
        loại bỏ hoàn toàn các dòng kịch bản rỗng (cả target_value và result đều NULL).
        """
        scenario_dfs = []

        for code, val_col, res_col, date_col in self.SCENARIO_SPECS:
            # Kiểm tra sự tồn tại của các cột trong batch
            val_expr = F.col(val_col) if val_col in df.columns else F.lit(None).cast("double")
            res_expr = F.col(res_col) if res_col in df.columns else F.lit(None).cast("double")
            date_expr = F.col(date_col) if date_col in df.columns else F.lit(None).cast("date")

            selected_cols = [F.col(c) for c in common_cols if c in df.columns]
            scenario_df = df.select(
                *selected_cols,
                F.lit(code).alias("scenario"),
                val_expr.alias("target_value"),
                res_expr.alias("target_result"),
                date_expr.alias("target_date")
            ).filter(
                F.col("target_value").isNotNull() | F.col("target_result").isNotNull()
            )

            scenario_dfs.append(scenario_df)

        if not scenario_dfs:
            return df

        unioned = scenario_dfs[0]
        for s_df in scenario_dfs[1:]:
            unioned = unioned.union(s_df)

        return unioned

    def compute_gap_analysis(self, unpivoted_df, snapshot_date_col: str = "snapshot_date"):
        """Tính toán các chỉ số phân tích khoảng cách mục tiêu (Pre-computed Gap Metrics)."""
        snap_col = F.col(snapshot_date_col) if snapshot_date_col in unpivoted_df.columns else F.current_date()

        analyzed = unpivoted_df.withColumn(
            # Tỷ lệ đạt (%)
            "achievement_pct",
            F.when(
                F.col("target_value") > 0,
                F.round((F.coalesce(F.col("target_result"), F.lit(0.0)) / F.col("target_value")) * 100.0, 2)
            ).otherwise(F.lit(None).cast("double"))
        ).withColumn(
            # Khoảng cách số tuyệt đối (Gap Value: dương = còn thiếu, âm = vượt mức)
            "gap_value",
            F.round(F.col("target_value") - F.coalesce(F.col("target_result"), F.lit(0.0)), 2)
        ).withColumn(
            # Khoảng cách % so với mục tiêu
            "gap_pct",
            F.when(
                F.col("target_value") > 0,
                F.round(((F.col("target_value") - F.coalesce(F.col("target_result"), F.lit(0.0))) / F.col("target_value")) * 100.0, 2)
            ).otherwise(F.lit(0.0))
        ).withColumn(
            # Đã đạt mục tiêu chưa
            "is_achieved",
            F.when(
                F.col("target_result").isNotNull() & (F.col("target_result") >= F.col("target_value")),
                F.lit(True)
            ).otherwise(F.lit(False))
        ).withColumn(
            # Số ngày còn lại đến hạn chót (Days to deadline)
            "days_to_deadline",
            F.when(
                F.col("target_date").isNotNull(),
                F.datediff(F.col("target_date"), snap_col)
            ).otherwise(F.lit(None).cast("integer"))
        ).withColumn(
            # Cờ cảnh báo nguy cơ trễ hạn (chưa đạt và hạn chót còn <= 7 ngày)
            "is_at_risk",
            F.when(
                (~F.col("is_achieved")) & (F.col("days_to_deadline") <= 7) & (F.col("days_to_deadline") >= 0),
                F.lit(True)
            ).otherwise(F.lit(False))
        ).withColumn(
            # Điểm đạt có trọng số BSC
            "weighted_achievement",
            F.when(
                F.col("achievement_pct").isNotNull(),
                F.round((F.col("achievement_pct") * F.coalesce(F.col("weight"), F.lit(1.0))) / 100.0, 2)
            ).otherwise(F.lit(0.0))
        )

        return analyzed
