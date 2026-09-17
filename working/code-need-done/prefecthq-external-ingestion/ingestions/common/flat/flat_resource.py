from .post_processing import *

# ---------------------------------------------------------------------------
# Resource catalog
# ---------------------------------------------------------------------------

HR_FILE_2025 = "hr-data/xlsx/VCS_Workforce Report 2025_31122025.xlsx"
HR_FILE_2026 = "hr-data/xlsx/VCS_Workforce Report 2026_09032026.xlsx"

RESOURCES: list = [
    # ── CX / CSO ─────────────────────────────────────────────────────────────
    {
        "domain": "cx_cso_raw",
        "resource_name": "cx_dim_journey",
        "object_key": "CX_def_kpi/CX_def_kpi/cx_dim_journey.xlsx",
        "sheet_name": "cx_dim_journey",
        "header_row": 0,
        "drop_rows": 1,
        "crawl_mode": "static",
    },
    {
        "domain": "cx_cso_raw",
        "resource_name": "dim_question_customer_journey",
        "object_key": "CX_def_kpi/CX_def_kpi/dim_question_customer_journey.xlsx",
        "sheet_name": "dim_question_customer_journey",
        "header_row": 0,
        "drop_rows": 1,
        "crawl_mode": "static",
    },
    {
        "domain": "cx_cso_raw",
        "resource_name": "dim_surveys_customer_journey",
        "object_key": "CX_def_kpi/CX_def_kpi/dim_surveys_customer_journey.xlsx",
        "sheet_name": "dim_surveys_customer_journey",
        "header_row": 0,
        "drop_rows": 1,
        "crawl_mode": "static",
    },
    # -- Ambari resource START --#
    {
        "domain": "cx_cso_raw",
        "resource_name": "excel_surveys",
        "object_key": "cx-cso/survicate_export v2.xlsx",
        "sheet_name": "Surveys",
        "header_row": 0,
        "drop_rows": 1,
        "crawl_mode": "static",
    },
    {
        "domain": "cx_cso_raw",
        "resource_name": "excel_survey_questions",
        "object_key": "cx-cso/survicate_export v2.xlsx",
        "sheet_name": "Survey_Questions (2)",
        "header_row": 0,
        "drop_rows": 1,
        "crawl_mode": "static",
    },
    # -- Ambari resource END --#
    # # ── CX: cx_product_category ───────────────────────────────────────────────
    # {
    #     "domain": "cx_cso_raw",
    #     "resource_name": "cx_product_category",
    #     "object_key": "CSO_keymap.xlsx", -- chưa có file này
    #     "sheet_name": "CX_Product_Category",
    #     "header_row": 0,
    #     "drop_rows": 1,
    #     "crawl_mode": "static",
    # },
    # # ── CX: cx_company ────────────────────────────────────────────────────────
    # {
    #     "domain": "cx_cso_raw",
    #     "resource_name": "cx_company",
    #     "object_key": "CSO_keymap.xlsx", -- chưa có file này
    #     "sheet_name": "CX_Company",
    #     "header_row": 0,
    #     "drop_rows": 1,
    #     "crawl_mode": "static",
    # },
    # ── HR onboard ────────────────────────────────────────────────────────────
    {
        "domain": "hr_raw",
        "resource_name": "hr_employee_onboard",
        "object_key": HR_FILE_2025,
        "sheet_name": "Data_MHTC Mới",
        "header_row": 1,
        "drop_rows": 2,
        "crawl_mode": "static",
        "post_process": dedup_by_employee_id,
    },
    {
        "domain": "hr_raw",
        "resource_name": "hr_employee_onboard_logs",
        "object_key": HR_FILE_2026,
        "sheet_name": "Active_2026",
        "header_row": 1,
        "drop_rows": 2,
        "crawl_mode": "modified_and_new",
    },
    # ── HR resigned ───────────────────────────────────────────────────────────
    {
        "domain": "hr_raw",
        "resource_name": "hr_employee_resigned",
        "object_key": HR_FILE_2025,
        "sheet_name": "Out_2025",
        "header_row": 1,
        "drop_rows": 2,
        "crawl_mode": "static",
        "post_process": dedup_by_employee_code,
    },
    {
        "domain": "hr_raw",
        "resource_name": "hr_employee_resigned_logs",
        "object_key": HR_FILE_2025,
        "sheet_name": "Out_2025",
        "header_row": 1,
        "drop_rows": 2,
        "crawl_mode": "modified_and_new",
    },
    # ── HR headcount ──────────────────────────────────────────────────────────
    {
        "domain": "hr_raw",
        "resource_name": "hr_employee_headcount",
        "object_key": HR_FILE_2026,
        "sheet_name": "NhuCau_2026",
        "header_row": 1,
        "drop_rows": 2,
        "crawl_mode": "static",
    },
    {
        "domain": "hr_raw",
        "resource_name": "hr_employee_headcount_logs",
        "object_key": HR_FILE_2026,
        "sheet_name": "NhuCau_2026",
        "header_row": 1,
        "drop_rows": 2,
        "crawl_mode": "static",
    },
    # ── Finance: actual_cost ──────────────────────────────────────────────────
    # The 2025 file is the static baseline loaded once.
    # Add new monthly files as additional entries with crawl_mode=modified_and_new.
    # enrich_actual_cost derives report_year, amount*1M, territory_name, etc.
    {
        "domain": "finance_raw",
        "resource_name": "actual_cost",
        "crawl_mode": "static",
        "post_process": enrich_actual_cost,
        "sources": [
            {
                "object_key": "chiphi_spdv/actual_cost_2025.xlsx",
                "file_type": "excel",
                "sheet_name": "Sheet1",
                "header_row": 0,
                "drop_rows": 1,
                "add_columns": {"source_file": "actual_cost_2025.xlsx"},
            },
        ],
    },
    # Uncomment and extend when new monthly files arrive:
    {
        "domain": "finance_raw",
        "resource_name": "actual_cost",
        "crawl_mode": "modified_and_new",
        "post_process": enrich_actual_cost,
        "sources": [
            {
                "object_key": "chiphi_spdv/chi_phi_spdv_T12_2025.xlsx",
                "file_type": "excel",
                "sheet_name": "Sheet1",
                "header_row": 0,
                "drop_rows": 1,
                "add_columns": {"source_file": "chi_phi_spdv_T12_2025.xlsx"},
            },
            {
                "object_key": "chiphi_spdv/chi_phi_spdv_T01_2026.xlsx",
                "file_type": "excel",
                "sheet_name": "Sheet1",
                "header_row": 0,
                "drop_rows": 1,
                "add_columns": {"source_file": "chi_phi_spdv_T01_2026.xlsx"},
            },
            {
                "object_key": "chiphi_spdv/chi_phi_spdv_T02_2026.xlsx",
                "file_type": "excel",
                "sheet_name": "Sheet1",
                "header_row": 0,
                "drop_rows": 1,
                "add_columns": {"source_file": "chi_phi_spdv_T02_2026.xlsx"},
            },
        ],
    },
    # ── Finance: cost_items ───────────────────────────────────────────────────
    # 2025 baseline: all CSVs in the folder, tagged data_type=CP.
    {
        "domain": "finance_raw",
        "resource_name": "cost_items",
        "crawl_mode": "static",
        "sources": [
            {
                "object_key": "cost-2025/chi_phi_2025",
                "file_type": "csv_folder",
                "header_row": 0,
                "drop_rows": 1,
                "add_columns": {"data_type": "CP"},
            },
        ],
    },
    # 2026 monthly files: each xlsx has a DT sheet and a CP sheet.
    {
        "domain": "finance_raw",
        "resource_name": "cost_items",
        "crawl_mode": "modified_and_new",
        "sources": [
            {
                "object_key": "finance-data/data/doanh_thu_chi_phi_cong_ty_T01_2026.xlsx",
                "file_type": "excel",
                "sheet_name": "DT",
                "header_row": 0,
                "drop_rows": 1,
                "add_columns": {
                    "data_type": "DT",
                    "source_file": "doanh_thu_chi_phi_cong_ty_T01_2026.xlsx",
                },
            },
            {
                "object_key": "finance-data/data/doanh_thu_chi_phi_cong_ty_T01_2026.xlsx",
                "file_type": "excel",
                "sheet_name": "CP",
                "header_row": 0,
                "drop_rows": 1,
                "add_columns": {
                    "data_type": "CP",
                    "source_file": "doanh_thu_chi_phi_cong_ty_T01_2026.xlsx",
                },
            },
            {
                "object_key": "finance-data/data/doanh_thu_chi_phi_cong_ty_T02_2026.xlsx",
                "file_type": "excel",
                "sheet_name": "CP",
                "header_row": 0,
                "drop_rows": 1,
                "add_columns": {
                    "data_type": "CP",
                    "source_file": "doanh_thu_chi_phi_cong_ty_T02_2026.xlsx",
                },
            },
            {
                "object_key": "finance-data/data/doanh_thu_chi_phi_cong_ty_T02_2026.xlsx",
                "file_type": "excel",
                "sheet_name": "DT",
                "header_row": 0,
                "drop_rows": 1,
                "add_columns": {
                    "data_type": "DT",
                    "source_file": "doanh_thu_chi_phi_cong_ty_T02_2026.xlsx",
                },
            },
            {
                "object_key": "finance-data/data/doanh_thu_chi_phi_cong_ty_T03_2026.xlsx",
                "file_type": "excel",
                "sheet_name": "DT",
                "header_row": 0,
                "drop_rows": 1,
                "add_columns": {
                    "data_type": "DT",
                    "source_file": "doanh_thu_chi_phi_cong_ty_T03_2026.xlsx",
                },
            },
            {
                "object_key": "finance-data/data/doanh_thu_chi_phi_cong_ty_T03_2026.xlsx",
                "file_type": "excel",
                "sheet_name": "CP",
                "header_row": 0,
                "drop_rows": 1,
                "add_columns": {
                    "data_type": "CP",
                    "source_file": "doanh_thu_chi_phi_cong_ty_T03_2026.xlsx",
                },
            },
        ],
    },
    # ── Finance: cost_plan ────────────────────────────────────────────────────
    # 2025 baseline: CSV folder.
    {
        "domain": "finance_raw",
        "resource_name": "cost_plan",
        "crawl_mode": "static",
        "sources": [
            {
                "object_key": "cost-2025/ke_hoach_chi_phi",
                "file_type": "csv_folder",
                "header_row": 0,
                "drop_rows": 1,
            },
        ],
    },
    # 2026 plan: single xlsx.
    {
        "domain": "finance_raw",
        "resource_name": "cost_plan",
        "crawl_mode": "modified_and_new",
        "sources": [
            {
                "object_key": "cost-plan/ke_hoach_cong_ty_2026.xlsx",
                "file_type": "excel",
                "sheet_name": "Chi phí",
                "header_row": 0,
                "drop_rows": 1,
                "add_columns": {"source_file": "ke_hoach_cong_ty_2026.xlsx"},
            },
        ],
    },
    # ── Finance: financial_ratio ──────────────────────────────────────────────
    {
        "domain": "finance_raw",
        "resource_name": "financial_ratio",
        "crawl_mode": "static",
        "sources": [
            {
                "object_key": "finance-data/data/cong_ty_Financial_ratio_T01_2026.xlsx",
                "file_type": "excel",
                "sheet_name": "Sheet1",
                "header_row": 0,
                "drop_rows": 1,
                "add_columns": {"source_file": "cong_ty_Financial_ratio_T01_2026.xlsx"},
            },
        ],
    },
    {
        "domain": "finance_raw",
        "resource_name": "financial_ratio",
        "crawl_mode": "modified_and_new",
        "sources": [
            {
                "object_key": "finance-data/data/cong_ty_Financial_ratio_T02_2026.xlsx",
                "file_type": "excel",
                "sheet_name": "Sheet1",
                "header_row": 0,
                "drop_rows": 1,
                "add_columns": {"source_file": "cong_ty_Financial_ratio_T02_2026.xlsx"},
            },
        ],
    },
    # ── Finance: production_cost_allocation ───────────────────────────────────
    {
        "domain": "finance_raw",
        "resource_name": "production_cost_allocation",
        "crawl_mode": "static",
        "sources": [
            {
                "object_key": "cost-2025/chi_phi_spdv_2025",
                "file_type": "csv_folder",
                "header_row": 0,
                "drop_rows": 1,
            },
        ],
    },
    # ── Finance: debt_report ──────────────────────────────────────────────────
    {
        "domain": "finance_raw",
        "resource_name": "debt_report",
        "crawl_mode": "static",
        "sources": [
            {
                "object_key": "cost-2025/cong_no_2025",
                "file_type": "csv_folder",
                "header_row": 0,
                "drop_rows": 1,
            },
        ],
    },
    # ── Finance: actual_revenue ───────────────────────────────────────────────
    {
        "domain": "finance_raw",
        "resource_name": "actual_revenue",
        "crawl_mode": "static",
        "sources": [
            {
                "object_key": "csv-revenue-finance-pbi- 2025/csv-revenue-finance-pbi- 2025/positive",
                "file_type": "csv_folder",
                "header_row": 0,
                "drop_rows": 1,
            },
        ],
    },
    # ── Finance: provisional_revenue ──────────────────────────────────────────
    {
        "domain": "finance_raw",
        "resource_name": "provisional_revenue",
        "crawl_mode": "static",
        "sources": [
            {
                "object_key": "csv-revenue-finance-pbi- 2025/csv-revenue-finance-pbi- 2025/negative",
                "file_type": "csv_folder",
                "header_row": 0,
                "drop_rows": 1,
            },
        ],
    },
    # ── Finance: finance_plan ─────────────────────────────────────────────────
    # 2025 baseline: CSV folder (only has "segment" column mapped, rest snake_case).
    # The csv reader adds subgroup="" via add_columns.
    {
        "domain": "finance_raw",
        "resource_name": "finance_plan",
        "crawl_mode": "static",
        "sources": [
            {
                "object_key": "taichinh/raw",
                "file_type": "csv_folder",
                "header_row": 0,
                "drop_rows": 1,
                "add_columns": {"subgroup": ""},
            },
        ],
    },
    # 2026 plan xlsx: adds company=VCS, subgroup handled by mapping.
    {
        "domain": "finance_raw",
        "resource_name": "finance_plan",
        "crawl_mode": "modified_and_new",
        "sources": [
            {
                "object_key": "plan-kinh-doanh-2026/Kế hoạch Nhóm KH_2026.xlsx",
                "file_type": "excel",
                "sheet_name": 0,
                "header_row": 0,
                "drop_rows": 1,
                "add_columns": {
                    "source_file": "Kế hoạch Nhóm KH_2026.xlsx",
                    "company": "VCS",
                },
            },
        ],
    },
    # ── Finance: product_revenue_plan (2025, wide→long pivot) ────────────────
    # Multi-level headers require special flattening + melt in post_process.
    {
        "domain": "finance_raw",
        "resource_name": "product_revenue_plan",
        "crawl_mode": "static",
        "post_process": transform_product_revenue_plan,
        "sources": [
            {
                "object_key": "plan_revenue/RAW_Dieu chinh Tach KH tung thang_N2025.xlsx",
                "file_type": "excel",
                "sheet_name": "KH N2025 (theo nhóm SPDV)",  # fix diacritic
                "header_row": 0,
                "drop_rows": 0,  # pass ALL rows including headers to post_process
            },
        ],
    },
    # ── Finance: product_revenue_plan_2026 ────────────────────────────────────
    {
        "domain": "finance_raw",
        "resource_name": "product_revenue_plan_2026",
        "crawl_mode": "static",
        "sources": [
            {
                "object_key": "kinhdoanh-03-18-2026-18-57-47_files_list/Plan_SPDV_2026.xlsx",
                "file_type": "excel",
                "sheet_name": "product_revenue_plan",
                "header_row": 0,
                "drop_rows": 1,
                "add_columns": {"source_file": "Plan_SPDV_2026.xlsx"},
            },
        ],
    },
    # ── Finance: dim_product_category_code ────────────────────────────────────
    # Headers are already clean English; no mapping needed, snake_case fallback applies.
    {
        "domain": "finance_raw",
        "resource_name": "dim_product_category_code",
        "crawl_mode": "static",
        "sources": [
            {
                "object_key": "dim_product_category_code.xlsx",
                "file_type": "excel",
                "sheet_name": 0,
                "header_row": 0,
                "drop_rows": 1,
                "add_columns": {"source_file": "dim_product_category_code.xlsx"},
            },
        ],
    },
    # ── Finance: sales_revenue ────────────────────────────────────────────────
    # T1 + T2 2026 concatenated into a single static load.
    {
        "domain": "finance_raw",
        "resource_name": "sales_revenue",
        "crawl_mode": "static",
        "sources": [
            {
                "object_key": "kinhdoanh-03-18-2026-18-57-47_files_list/Dữ liệu dashboard kinh doanh tháng 1_2026/Doanh_thu_cong_ty_t1_2026.xlsx",
                "file_type": "excel",
                "sheet_name": "Doanh_thu_cong_ty_t1_2026",
                "header_row": 0,
                "drop_rows": 1,
                "add_columns": {"source_file": "Doanh_thu_cong_ty_t1_2026.xlsx"},
            },
            {
                "object_key": "kinhdoanh-03-18-2026-18-57-47_files_list/Dữ liệu dashboard kinh doanh tháng 2_2026/Doanh_thu_cong_ty_t2_2026.xlsx",
                "file_type": "excel",
                "sheet_name": "Doanh_thu_cong_ty_t2_2026",
                "header_row": 0,
                "drop_rows": 1,
                "add_columns": {"source_file": "Doanh_thu_cong_ty_t2_2026.xlsx"},
            },
        ],
    },
    # ── Finance: sales_revenue_share_all ──────────────────────────────────────
    {
        "domain": "finance_raw",
        "resource_name": "sales_revenue_share_all",
        "crawl_mode": "static",
        "sources": [
            {
                "object_key": "kinhdoanh-03-18-2026-18-57-47_files_list/Dữ liệu dashboard kinh doanh tháng 1_2026/Doanh_thu_cong_ty_t1_2026_shared_all.xlsx",
                "file_type": "excel",
                "sheet_name": "Sheet1",
                "header_row": 0,
                "drop_rows": 1,
                "add_columns": {
                    "source_file": "Doanh_thu_cong_ty_t1_2026_shared_all.xlsx"
                },
            },
            {
                "object_key": "kinhdoanh-03-18-2026-18-57-47_files_list/Dữ liệu dashboard kinh doanh tháng 2_2026/Doanh_thu_cong_ty_t2_2026_shared_all.xlsx",
                "file_type": "excel",
                "sheet_name": "Sheet1",
                "header_row": 0,
                "drop_rows": 1,
                "add_columns": {
                    "source_file": "Doanh_thu_cong_ty_t2_2026_shared_all.xlsx"
                },
            },
        ],
    },
    # ── Finance: sales_revenue_share_soc_product_category ────────────────────
    {
        "domain": "finance_raw",
        "resource_name": "sales_revenue_share_soc_product_category",
        "crawl_mode": "static",
        "sources": [
            {
                "object_key": "kinhdoanh-03-18-2026-18-57-47_files_list/Dữ liệu dashboard kinh doanh tháng 1_2026/Doanh_thu_cong_ty_t1_2026_SOC_chia_se_cho_SPDV.xlsx",
                "file_type": "excel",
                "sheet_name": "Sheet1",
                "header_row": 0,
                "drop_rows": 1,
                "add_columns": {
                    "source_file": "Doanh_thu_cong_ty_t1_2026_SOC_chia_se_cho_SPDV.xlsx"
                },
            },
            {
                "object_key": "kinhdoanh-03-18-2026-18-57-47_files_list/Dữ liệu dashboard kinh doanh tháng 2_2026/Doanh_thu_cong_ty_t2_2026_SOC_chia_se_cho_SPDV.xlsx",
                "file_type": "excel",
                "sheet_name": "Sheet1",
                "header_row": 0,
                "drop_rows": 1,
                "add_columns": {
                    "source_file": "Doanh_thu_cong_ty_t2_2026_SOC_chia_se_cho_SPDV.xlsx"
                },
            },
        ],
    },
    # ── Finance: contract_collected_invoices ──────────────────────────────────
    # Notebook used column-selection before rename; our normalize handles extra
    # columns fine (they get snake_cased but don't break anything).
    {
        "domain": "finance_raw",
        "resource_name": "contract_collected_invoices",
        "crawl_mode": "static",
        "sources": [
            {
                "object_key": "financial-data/File theo doi cong no 25-03-2026.xlsx",
                "file_type": "excel",
                "sheet_name": "All contract",
                "header_row": 0,
                "drop_rows": 1,
                "add_columns": {"source_file": "File theo doi cong no 25-03-2026.xlsx"},
            },
        ],
    },
    {
        "domain": "finance_raw",
        "resource_name": "contract_collected_invoices",
        "crawl_mode": "modified_and_new",
        "sources": [
            {
                "object_key": "financial-data/File theo doi cong no 31-03-2026 (1).xlsx",
                "file_type": "excel",
                "sheet_name": "All contract",
                "header_row": 0,
                "drop_rows": 1,
                "add_columns": {
                    "source_file": "File theo doi cong no 31-03-2026 (1).xlsx"
                },
            },
        ],
    },
]
