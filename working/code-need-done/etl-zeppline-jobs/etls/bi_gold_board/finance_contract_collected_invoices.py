%livy.pyspark

# spark.sql("DROP VIEW IF EXISTS bi_gold.finance_contract_collected_invoices")

spark.sql("""
CREATE OR REPLACE VIEW bi_gold.finance_contract_collected_invoices  AS
SELECT 
    idd ,
   upper(sales_channel) as sales_channel ,
   upper(transaction_code) as transaction_code,
   upper(invoice_number) as invoice_number ,
   upper(contract_num) as contract_num,
   upper(customer_code) as customer_code,
   upper(contract_description) as contract_description,
   upper(company_name) as company_name,
   original_amount ,
   initial_receivable_amount ,
   total_received_amount,
   remaining_balance_amount,
   invoice_date ,
   payment_deadline ,
   payment_date ,
   overdue_months_numeric,
   upper(overdue_status_enum) as overdue_status_enum,
   upper(am_username) as am_username
FROM bi_silver.finance_contract_collected_invoices
""")
