"""
Mockup Data Generator for Planview / Clarizen AdaptiveWork EPM Tasks.
Sinh bộ dữ liệu giả lập phong phú bám sát 186 trường trong task_data_raw.txt:
- Phân cấp WBS & Cây công việc
- Lịch trình, Baseline & Variance
- Giờ công, Timesheet & Nguồn lực
- Tài chính CAPEX / OPEX, Labor / Non-Labor Rate
- Quản lý giá trị thu được EVM (PV, EV, AC, CPI, SPI, EAC)
- Trạng thái, Đường găng (Critical Path), Sức khỏe
- Tham chiếu thực thể lồng nhau: State, Project, Parent, User
"""

import json
import random
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional


TASK_TITLES = [
    "Khảo sát yêu cầu kiến trúc hệ thống EPM",
    "Thiết kế ERD và mô hình dữ liệu DWH",
    "Xây dựng API Client kết nối Planview Clarizen",
    "Cài đặt bộ ngắt mạch Circuit Breaker và Rate Limiter",
    "Xây dựng cơ chế phân trang Offset Paginator lồng nhau",
    "Kiểm thử tải và đo đạc độ trễ mạng API",
    "Cấu hình luồng lập lịch Prefect Flow remote worker",
    "Làm phẳng JSON đệ quy với Apache Spark 2.3.2",
    "Cài đặt thuật toán Dedup Idempotent bằng Window Ranking",
    "Tạo stub record cho Kimball Inferred Dimensions",
    "Định tuyến bản ghi lỗi sang Dead Letter Queue (DLQ)",
    "Sinh mã DDL cho Trino Hive Metastore",
    "Cấu hình dbt staging bóc tách trường lồng nhau",
    "Viết model dbt intermediate tính toán chỉ số EVM",
    "Xây dựng bảng Marts Fact ảnh chụp tiến độ hàng ngày",
    "Thiết kế Dashboard trực quan hóa đường cong chữ S",
    "Tích hợp AI GenBI Semantic Context Pack",
    "Kiểm thử hồi quy trên Apache Zeppelin Livy",
    "Bàn giao tài liệu hướng dẫn vận hành cho Leader",
    "Tối ưu hóa bộ nhớ worker chống tràn RAM OOM",
]

DELIVERABLE_TYPES = ["Software", "Architecture_Doc", "Test_Report", "Data_Pipeline", "Dashboard"]
TASK_TYPES = ["Execution", "Review", "Milestone", "Research"]
TRACK_STATUSES = ["On Track", "At Risk", "Off Track"]
STATES = [
    {"id": "/State/Draft"},
    {"id": "/State/Active"},
    {"id": "/State/Active"},
    {"id": "/State/Completed"},
]


class MockDataGenerator:
    """Sinh bộ dữ liệu giả lập chuẩn Clarizen/Planview EPM."""

    def __init__(self, seed: int = 42):
        random.seed(seed)
        self.projects = [f"/Project/PRJ-VCS-{i:03d}" for i in range(1, 11)]
        self.phases = [f"/Phase/PHASE-{i:02d}" for i in range(1, 6)]
        self.users = [f"/User/USR-VCS-{i:03d}" for i in range(1, 21)]

    def generate_task(
        self,
        task_seq: int,
        parent_id: Optional[str] = None,
        is_duplicate: bool = False,
        duplicate_id: Optional[str] = None,
        is_corrupt: bool = False,
    ) -> Dict[str, Any]:
        """Sinh 1 bản ghi Task chi tiết."""
        if is_corrupt:
            # Bản ghi lỗi không có ID để test DLQ Router
            return {
                "id": None,
                "SYSID": None,
                "Name": "CORRUPT_TASK_WITHOUT_ID",
                "State": {"id": "/State/Draft"},
                "LastUpdatedOn": datetime.utcnow().isoformat(),
            }

        task_id = duplicate_id if is_duplicate else f"/Task/{uuid.uuid4().hex[:28]}"
        sysid = f"T-{task_seq:05d}"
        name = random.choice(TASK_TITLES) + f" (Đợt {task_seq})"
        project_id = random.choice(self.projects)
        phase_id = random.choice(self.phases)
        manager_id = random.choice(self.users)
        creator_id = random.choice(self.users)

        is_milestone = (task_seq % 10 == 0)
        on_critical_path = (random.random() < 0.35)
        deliverable_type = random.choice(DELIVERABLE_TYPES)
        task_type = "Milestone" if is_milestone else random.choice(TASK_TYPES)

        # Dates
        base_date = datetime(2026, 6, 1) + timedelta(days=(task_seq % 60))
        planned_duration = 0.0 if is_milestone else float(random.choice([3, 5, 10, 15, 20]))
        planned_start = base_date
        planned_due = base_date + timedelta(days=int(planned_duration))

        baseline_duration = planned_duration
        baseline_start = planned_start - timedelta(days=random.choice([0, 1, 2]))
        baseline_due = baseline_start + timedelta(days=int(baseline_duration))
        baseline_work = 0.0 if is_milestone else float(baseline_duration * 8)

        # Progress & Status
        state = random.choice(STATES)
        if state["id"] == "/State/Completed":
            percent_completed = 100.0
            actual_start = planned_start + timedelta(days=random.choice([-1, 0, 1]))
            actual_end = planned_due + timedelta(days=random.choice([-2, 0, 3]))
            actual_duration = max(1.0, (actual_end - actual_start).days)
        elif state["id"] == "/State/Active":
            percent_completed = float(random.choice([25.0, 50.0, 75.0, 90.0]))
            actual_start = planned_start
            actual_end = None
            actual_duration = float(random.randint(1, int(max(1, planned_duration))))
        else:
            percent_completed = 0.0
            actual_start = None
            actual_end = None
            actual_duration = 0.0

        # Hours
        budgeted_hours = baseline_work
        actual_effort = round(budgeted_hours * (percent_completed / 100.0) * random.uniform(0.85, 1.25), 1)
        remaining_effort = max(0.0, round(budgeted_hours - actual_effort, 1))
        actual_billable_hours = round(actual_effort * 0.8, 1)
        actual_non_billable_hours = round(actual_effort * 0.2, 1)

        # Financials
        labor_rate_hourly = 45.0  # USD / hour
        budget_cost_lr = round(budgeted_hours * labor_rate_hourly, 2)
        budget_cost_nlr = round(budget_cost_lr * 0.15, 2)
        budget_cost_capex = round(budget_cost_lr * 0.6, 2)
        budget_cost_opex = round(budget_cost_lr * 0.4, 2)
        planned_budget = budget_cost_lr + budget_cost_nlr
        planned_revenue = round(planned_budget * 1.35, 2)

        actual_cost_lr = round(actual_effort * labor_rate_hourly, 2)
        actual_cost_nlr = round(budget_cost_nlr * (percent_completed / 100.0), 2)
        actual_cost = actual_cost_lr + actual_cost_nlr
        actual_cost_capex = round(actual_cost * 0.6, 2)
        actual_cost_opex = round(actual_cost * 0.4, 2)
        actual_revenue = round(actual_cost * 1.35, 2)

        # EVM
        expected_progress = float(min(100.0, percent_completed + random.choice([-10.0, 0.0, 10.0])))
        earned_value = round(planned_budget * (percent_completed / 100.0), 2)
        cpi = round(earned_value / actual_cost, 3) if actual_cost > 0 else 1.0
        pv = planned_budget * (expected_progress / 100.0)
        spi = round(earned_value / pv, 3) if pv > 0 else 1.0
        currency_eac = round(planned_budget / cpi, 2) if (cpi > 0 and planned_budget > 0) else planned_budget
        currency_etc = round(currency_eac - actual_cost, 2)

        # Audit dates
        created_on = (base_date - timedelta(days=10)).strftime("%Y-%m-%dT%H:%M:%S.0000000")
        if is_duplicate:
            # Bản ghi update mới hơn
            last_updated_on = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.0000000")
        else:
            last_updated_on = base_date.strftime("%Y-%m-%dT%H:%M:%S.0000000")

        return {
            # 1. Định danh & Phân cấp WBS
            "id": task_id,
            "SYSID": sysid,
            "Name": name,
            "Description": f"Mô tả công việc chi tiết cho task {sysid}",
            "Parent": {"id": parent_id} if parent_id else None,
            "Project": {"id": project_id},
            "ParentProject": {"id": project_id},
            "Phase": {"id": phase_id},
            "Milestone": is_milestone,
            "OnCriticalPath": on_critical_path,
            "Deliverable": f"Deliverable_{sysid}",
            "DeliverableType": deliverable_type,
            "TaskType": task_type,
            "ChildrenCount": 0 if parent_id else random.randint(0, 3),
            "PredecessorsCount": random.randint(0, 2),
            "SuccessorsCount": random.randint(0, 2),

            # 2. Lịch trình & Variance
            "StartDate": planned_start.strftime("%Y-%m-%dT%H:%M:%S.0000000"),
            "DueDate": planned_due.strftime("%Y-%m-%dT%H:%M:%S.0000000"),
            "Duration": planned_duration,
            "BaselineStartDate": baseline_start.strftime("%Y-%m-%dT%H:%M:%S.0000000"),
            "BaselineDueDate": baseline_due.strftime("%Y-%m-%dT%H:%M:%S.0000000"),
            "BaselineDuration": baseline_duration,
            "BaselineWork": baseline_work,
            "ActualStartDate": actual_start.strftime("%Y-%m-%dT%H:%M:%S.0000000") if actual_start else None,
            "ActualEndDate": actual_end.strftime("%Y-%m-%dT%H:%M:%S.0000000") if actual_end else None,
            "ActualDuration": actual_duration,
            "StartDateVariance": (actual_start - planned_start).days if actual_start else 0.0,
            "DueDateVariance": (actual_end - planned_due).days if actual_end else 0.0,
            "DurationVariance": round(actual_duration - planned_duration, 1),
            "WorkVariance": round(actual_effort - budgeted_hours, 1),

            # 3. Giờ công & Phân bổ
            "Work": budgeted_hours,
            "BudgetedHours": budgeted_hours,
            "ActualEffort": actual_effort,
            "RemainingEffort": remaining_effort,
            "ActualBillableHours": actual_billable_hours,
            "ActualNonBillableHours": actual_non_billable_hours,
            "Allocation": 100.0,
            "UserResourcesCount": random.randint(1, 4),

            # 4. Tài chính
            "PlannedBudget": planned_budget,
            "PlannedAmount": planned_budget,
            "BudgetCostLR": budget_cost_lr,
            "BudgetCostNLR": budget_cost_nlr,
            "BudgetCostCAPEX": budget_cost_capex,
            "BudgetCostOPEX": budget_cost_opex,
            "PlannedRevenue": planned_revenue,
            "ActualCost": actual_cost,
            "ActualCostLR": actual_cost_lr,
            "ActualCostNLR": actual_cost_nlr,
            "ActualCostCAPEX": actual_cost_capex,
            "ActualCostOPEX": actual_cost_opex,
            "ActualRevenue": actual_revenue,

            # 5. EVM
            "EarnedValue": earned_value,
            "CPI": cpi,
            "SPI": spi,
            "CurrencyEAC": currency_eac,
            "CurrencyETC": currency_etc,

            # 6. Trạng thái & Sức khỏe
            "State": state,
            "TrackStatus": random.choice(TRACK_STATUSES),
            "PercentCompleted": percent_completed,
            "LaborCostPercentComplete": percent_completed,
            "ExpectedProgress": expected_progress,
            "IssuesCount": random.choice([0, 0, 0, 1, 2]),

            # 7. Audit & Users
            "Manager": {"id": manager_id},
            "CreatedBy": {"id": creator_id},
            "EntityOwner": {"id": manager_id},
            "CreatedOn": created_on,
            "LastUpdatedOn": last_updated_on,
        }

    def generate_dataset(
        self,
        total_records: int = 1000,
        duplicate_rate: float = 0.05,
        corrupt_rate: float = 0.02,
    ) -> List[Dict[str, Any]]:
        """Sinh toàn bộ dataset có cấu trúc phân cấp, trùng lặp và bản ghi lỗi."""
        records = []
        created_ids = []

        corrupt_count = int(total_records * corrupt_rate)
        duplicate_count = int(total_records * duplicate_rate)
        clean_count = total_records - corrupt_count - duplicate_count

        # 1. Sinh bản ghi chuẩn (có WBS cha-con)
        parent_id = None
        for seq in range(1, clean_count + 1):
            if seq % 4 == 0 and len(created_ids) > 0:
                parent = random.choice(created_ids)
            else:
                parent = None

            record = self.generate_task(seq, parent_id=parent)
            records.append(record)
            created_ids.append(record["id"])

        # 2. Sinh bản ghi trùng lặp (Duplicate with updated timestamp & higher progress)
        for seq in range(1, duplicate_count + 1):
            target_id = random.choice(created_ids)
            dup_record = self.generate_task(
                seq + clean_count,
                is_duplicate=True,
                duplicate_id=target_id,
            )
            dup_record["PercentCompleted"] = 100.0  # Cập nhật tiến độ hoàn thành
            dup_record["State"] = {"id": "/State/Completed"}
            records.append(dup_record)

        # 3. Sinh bản ghi lỗi (Corrupt records for DLQ)
        for seq in range(1, corrupt_count + 1):
            records.append(self.generate_task(seq, is_corrupt=True))

        # Trộn ngẫu nhiên thứ tự các bản ghi
        random.shuffle(records)
        return records


if __name__ == "__main__":
    gen = MockDataGenerator()
    sample = gen.generate_dataset(total_records=100)
    print(f"✅ Generated {len(sample)} sample records successfully!")
    print(f"Sample Task Keys ({len(sample[0].keys())} fields):")
    print(json.dumps(list(sample[0].keys())[:15], indent=2))
