#!/bin/bash

python clone-verify.py --schemas bi_silver
python clone-verify.py --schemas bitu_schema
python clone-verify.py --schemas crm_raw
python clone-verify.py --schemas crm_silver
python clone-verify.py --schemas cx_cso_raw
python clone-verify.py --schemas cx_cso_silver
python clone-verify.py --schemas cx_survicate_bronze
python clone-verify.py --schemas cx_survicate_raw
python clone-verify.py --schemas cx_survicate_silver
python clone-verify.py --schemas finance_raw
python clone-verify.py --schemas hr_raw
python clone-verify.py --schemas jira_raw
python clone-verify.py --schemas jira_silver
# python clone-verify.py --schemas noc_metrics_raw 
# python clone-verify.py --schemas noc_metrics_silver 
# python clone-verify.py --schemas nocodb_raw 
