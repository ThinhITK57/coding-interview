# Bo dashboard goi y theo domain

Tai lieu nay de xuat nhanh 4 dashboard mau cho Lightdash:
- CRM Pipeline
- Finance Plan vs Actual
- CX SLA/NPS
- HR Attrition

Muc tieu:
- Tao nhanh dashboard co gia tri van hanh.
- Dung lai duoc tren du lieu staging hien tai.
- Dong bo naming/description metrics theo chuan song ngu.

## 1) CRM Pipeline

### KPI cards
- Tong deal: `deal_count`
- Tong gia tri deal (VND): `total_deal_amount`
- Weighted pipeline: `weighted_pipeline`
- Win rate: `win_rate`
- So deal da won: `won_deal_count`
- So deal da lost: `lost_deal_count`
- Pipeline stale >=30 ngay: `stale_pipeline_amount`

### Charts de xuat
1. Pipeline theo thoi gian (Line)
- Time: `created_at` (month)
- Metrics: `total_deal_amount`, `weighted_pipeline`
- Breakdown tuy chon: `territory_name`, `customer_segment_l1`

2. Funnel theo stage (Bar)
- Dimension: `deal_stage_name`
- Metrics: `deal_count`, `total_deal_amount`

3. Win/Loss theo AM (Bar ngang)
- Dimension: `am_username`
- Metrics: `won_deal_count`, `lost_deal_count`, `win_rate`
- Sort: `total_deal_amount` desc

4. Top stale pipeline (Table)
- Dimensions: `deal_name`, `am_username`, `deal_stage_name`, `days_since_last_update`
- Metrics: `stale_pipeline_amount`, `total_deal_amount`
- Filter: `days_since_last_update >= 30`

### Filters global
- `territory_name`
- `customer_segment_l1`
- `deal_stage_name`
- `deal_status`

---

## 2) Finance Plan vs Actual

### KPI cards
- Actual revenue: `total_revenue_amount` (from allocated revenue)
- Forecast revenue: `forecast_revenue_amount`
- Actual revenue share: `actual_revenue_share`
- Total cost actual: `total_actual_cost`
- Plan MUST: `total_plan_must_amount`
- Plan NICE: `total_plan_nice_amount`
- Plan target: `total_plan_target_amount`

### Charts de xuat
1. Actual vs Plan theo thang (Combo)
- Time: `report_date` or `plan_date` (month)
- Metrics: `total_revenue_amount`, `total_plan_must_amount`, `total_plan_target_amount`

2. Co cau doanh thu actual/forecast (Stacked bar)
- Dimension: `product_category`
- Metrics: `actual_revenue_amount`, `forecast_revenue_amount`

3. Cost trend (Line)
- Time: `report_date` (month)
- Metrics: `total_actual_cost`, `avg_actual_cost`
- Breakdown: `cost_group` hoac `business_unit_level_1`

4. Cong no va thu no (Bar + Table)
- Metrics: `total_initial_receivable_amount`, `total_received_amount`, `total_remaining_balance_amount`
- Ratios: `collection_ratio`, `outstanding_ratio`, `overdue_ratio`
- Breakdown: `am_username` hoac `sales_channel`

### Filters global
- `business_unit_level_1`
- `product_category`
- `territory_name`
- `customer_segment_l1`

---

## 3) CX SLA/NPS

### KPI cards
- So ticket: `ticket_count`
- Ticket overdue: `overdue_ticket_count`
- SLA breach rate: `sla_breach_rate`
- Avg first response (phut): `avg_first_response_minutes`
- Avg resolution (phut): `avg_resolution_time_minutes`
- NPS: `nps_score`

### Charts de xuat
1. SLA trend theo thoi gian (Line)
- Time: `created_at` (day/week)
- Metrics: `ticket_count`, `sla_breach_count`, `sla_breach_rate`

2. Ticket theo severity va support category (Heatmap/Bar)
- Dimensions: `severity_level`, `support_category`
- Metrics: `ticket_count`, `overdue_ticket_count`

3. NPS theo product/journey (Bar)
- Dimension: `product_category` hoac `journey`
- Metrics: `nps_score`, `promoter_count`, `detractor_count`

4. Ticket tags phu bien (Table)
- Dimension: `tag`
- Metrics: `tag_record_count`, `tagged_ticket_count`, `avg_tags_per_ticket_proxy`

### Filters global
- `product_category`
- `customer_segment_l1`
- `severity_level`
- `assigned_agent_stage`

---

## 4) HR Attrition

### KPI cards
- Headcount hien tai: `headcount_employee_count`
- So nhan su nghi viec: `resigned_employee_count`
- So key employee nghi viec: `resigned_key_employee_count`
- Ty le key employee nghi viec: `resigned_key_employee_rate`
- So nhan su moi: `new_hire_count`
- Ty le nhan su moi: `new_hire_rate`

### Charts de xuat
1. Onboard vs Resigned theo thang (Line)
- Time: `hire_date` va `termination_date` (month)
- Metrics: `new_hire_count`, `resigned_employee_count`

2. Attrition theo don vi (Bar)
- Dimension: `business_unit_level_1` hoac `division`
- Metrics: `resigned_employee_count`, `resigned_key_employee_count`

3. Co cau nhan su strategic (Donut)
- Metrics: `ai_org_employee_count`, `ai_group_employee_count`, `rnd_employee_count`, `intl_business_employee_count`

4. Age va nhan su key (Table)
- Dimensions: `job_level`, `management_level`
- Metrics: `avg_age`, `key_employee_count`

### Filters global
- `business_unit_level_1`
- `division`
- `job_level`
- `management_level`

---

## Chuan naming/description metrics (VI/EN)

Quy uoc de dong bo trong Lightdash:
- Metric key: snake_case, tien to theo domain neu can (`crm_`, `finance_`, `cx_`, `hr_`).
- Display naming: uu tien tieng Viet cho business user, giu key ky thuat de truy vet.
- Description: song ngu theo format:
  - `VI: ... | EN: ...`
- Don vi hien thi:
  - So tien: ghi ro `VND` neu da quy doi.
  - Ty le: mo ta ro mau so/tu so.
  - Thoi gian: ghi ro don vi (phut/ngay/thang).

Mau description:
- `VI: Ty le ticket vi pham SLA. | EN: Ratio of tickets that breached SLA.`
- `VI: Tong gia tri deal theo VND. | EN: Total deal value in VND.`

## Go-live checklist
- Xac nhan metric trong Explore hien dung label/description.
- Kiem tra filters global hoat dong tren tat ca tile.
- Kiem tra bo loc thoi gian mac dinh (30/90 ngay) theo tung dashboard.
- Chot owner dashboard theo domain (CRM/Finance/CX/HR).
- Dat lich review hang tuan cho KPI bat thuong.
