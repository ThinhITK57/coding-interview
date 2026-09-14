import sys
from libs.glossary_task import sync_excel_glossary

if __name__ == "__main__":
    sync_excel_glossary(
        excel_path="../dbt-agent/dg-terms-resources/KD_DataGovernance_Metrics.xlsx"
    )
    