# # %livy.pyspark

# # OLD: crm_payment_records
# # NEW: crm_payment_forecast
# # NEW: finance_allocated_revenue

# # spark.sql("DROP TABLE IF EXISTS bi_silver.crm_payment_records")
# # spark.sql("DROP TABLE IF EXISTS bi_silver.crm_allocated_revenue")
# # =========================================================
# # Revenue allocation logic UPDATE 08-May-2026
# # =========================================================
# #
# # Nếu period_number = 1:
# #   -> tạo 1 kỳ duy nhất tại first_payment_date
# #
# # Nếu period_number > 1:
# #   -> period_number là số tháng phân bổ
# #   -> nếu first_payment_date không phải ngày đầu tháng,
# #      tạo thêm 1 kỳ partial cuối
# #
# # contract_end_date =
# #   add_months(first_payment_date, period_number) - 1 day
# #
# # contract_total_days =
# #   datediff(contract_end_date, first_payment_date) + 1
# #
# # used_days =
# #   datediff(payment_period_end_date, payment_period_start_date) + 1
# #
# # daily_value =
# #   total_value / contract_total_days
# #
# # period_value =
# #   daily_value * used_days
# #
# # Kỳ cuối nhận phần còn lại:
# #   last_period_value = total_value - sum(previous_period_values)
# #
# # để đảm bảo tổng tất cả kỳ = đúng total_value.
# # =========================================================

# from pyspark.sql import functions as F
# from pyspark.sql.types import ArrayType, IntegerType
# from pyspark.sql.functions import udf
# from pyspark.sql.window import Window
# spark.catalog.clearCache()


# spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
# spark.sql("REFRESH TABLE crm_raw.deals")
# spark.sql("REFRESH TABLE crm_raw.deleted_deals")
# spark.sql("REFRESH TABLE crm_raw.deal_stages")
# spark.sql("REFRESH TABLE crm_raw.cm_contracts")
# spark.sql("REFRESH TABLE crm_raw.deal_payment_statuses")
# spark.sql("REFRESH TABLE crm_raw.currencies")
# spark.sql("REFRESH TABLE crm_raw.deal_reasons")
# spark.sql("REFRESH TABLE bi_silver.crm_contract_allocations")

# # -----------------------
# # 1) Read
# # -----------------------
# df = spark.table("bi_silver.crm_contract_allocations") \
#     .filter(F.col("period_value").isNotNull())

# # =========================================================
# # 2) Build allocation period theo logic prorate by used_days
# # =========================================================

# def gen_idx(n):
#     try:
#         if n is None:
#             return [1]
#         n = int(n)
#         if n < 1:
#             return [1]
#         return list(range(1, n + 1))
#     except:
#         return [1]

# gen_idx_udf = udf(gen_idx, ArrayType(IntegerType()))

# df2 = (
#     df
#     .withColumn(
#         "allocation_duration_fix",
#         F.when(F.col("period_number").isNull() | (F.col("period_number") < 1), F.lit(1))
#          .otherwise(F.col("period_number").cast("int"))
#     )
#     .withColumn("contract_start_date", F.to_date(F.col("first_payment_date")))

#     # Nếu count = 1 -> 1 kỳ
#     # Nếu count > 1 và start date là đầu tháng -> count kỳ
#     # Nếu count > 1 và start date không phải đầu tháng -> count + 1 kỳ
#     .withColumn(
#         "total_payment_periods_fix",
#         F.when(F.col("allocation_duration_fix") == 1, F.lit(1))
#          .when(F.dayofmonth(F.col("contract_start_date")) == 1, F.col("allocation_duration_fix"))
#          .otherwise(F.col("allocation_duration_fix") + F.lit(1))
#     )

#     .withColumn("payment_array", gen_idx_udf(F.col("total_payment_periods_fix")))
#     .withColumn("payment_index", F.explode(F.col("payment_array")))
#     .drop("payment_array")

#     # Ngày kết thúc phân bổ = first_payment_date + count tháng - 1 ngày
#     .withColumn(
#         "contract_end_date",
#         F.expr("date_sub(add_months(contract_start_date, allocation_duration_fix), 1)")
#     )

#     .withColumn(
#         "contract_total_days",
#         F.when(F.col("allocation_duration_fix") == 1, F.lit(1))
#          .otherwise(F.datediff(F.col("contract_end_date"), F.col("contract_start_date")) + F.lit(1))
#     )

#     .withColumn("update_at", F.current_timestamp())
# )
# # -----------------------
# # 3) payment_date
# # -----------------------
# df3 = (
#     df2
#     .withColumn(
#         "payment_period_start_date",
#         F.expr("""
#             CASE
#                 WHEN allocation_duration_fix = 1 THEN contract_start_date
#                 WHEN payment_index = 1 THEN contract_start_date
#                 ELSE add_months(trunc(contract_start_date, 'MM'), payment_index - 1)
#             END
#         """)
#     )
#     .withColumn(
#         "payment_period_end_date",
#         F.expr("""
#             CASE
#                 WHEN allocation_duration_fix = 1 THEN contract_start_date
#                 WHEN payment_index = total_payment_periods_fix THEN contract_end_date
#                 ELSE last_day(add_months(trunc(contract_start_date, 'MM'), payment_index - 1))
#             END
#         """)
#     )
#     .withColumn(
#         "used_days",
#         F.when(
#             F.col("payment_period_end_date") < F.col("payment_period_start_date"),
#             F.lit(0)
#         ).otherwise(
#             F.datediff(F.col("payment_period_end_date"), F.col("payment_period_start_date")) + F.lit(1)
#         )
#     )
#     .withColumn("payment_date", F.col("payment_period_start_date"))
# )

# # -----------------------
# # 4) vnd_amount + is_actual
# # -----------------------
# #v allocation_value=> period_value
# df4 = (
#     df3
#     .withColumn("currency_code_u", F.upper(F.trim(F.col("currency_code"))))

#     .withColumn(
#         "exchange_rate_fix",
#         F.when(
#             F.col("currency_code_u") == "USD",
#             F.when(
#                 F.col("usd_to_vnd").isNull() | (F.col("usd_to_vnd") == 0),
#                 F.lit(26000.0)
#             ).otherwise(F.col("usd_to_vnd").cast("double"))
#         ).otherwise(F.lit(1.0))
#     )

#     # Tổng nguyên tệ cần phân bổ
#     .withColumn(
#         "total_currency_amount",
#         F.col("period_value").cast("double") * F.col("allocation_duration_fix").cast("double")
#     )

#     # Tổng VND cần phân bổ
#     .withColumn(
#         "total_vnd_amount",
#         F.when(
#             F.col("currency_code_u") == "USD",
#             F.col("total_currency_amount") * F.col("exchange_rate_fix")
#         )
#         .when(
#             F.col("currency_code_u") == "VND",
#             F.col("total_currency_amount")
#         )
#         .otherwise(F.col("total_currency_amount"))
#     )

#     .withColumn(
#         "daily_currency_base",
#         F.col("total_currency_amount") / F.col("contract_total_days")
#     )
#     .withColumn(
#         "daily_vnd_base",
#         F.col("total_vnd_amount") / F.col("contract_total_days")
#     )

#     .withColumn(
#         "currency_amount_tmp",
#         F.round(F.col("daily_currency_base") * F.col("used_days"), 2)
#     )
#     .withColumn(
#         "vnd_amount_tmp",
#         F.round(F.col("daily_vnd_base") * F.col("used_days"), 2)
#     )
# )
# w = (
#     Window
#     .partitionBy("id")
#     .orderBy("payment_index")
#     .rowsBetween(Window.unboundedPreceding, -1)
# )

# df4 = (
#     df4
#     .withColumn("prev_currency_amount", F.sum("currency_amount_tmp").over(w))
#     .withColumn("prev_vnd_amount", F.sum("vnd_amount_tmp").over(w))

#     .withColumn(
#         "currency_amount",
#         F.when(
#             F.col("payment_index") == F.col("total_payment_periods_fix"),
#             F.col("total_currency_amount") - F.coalesce(F.col("prev_currency_amount"), F.lit(0.0))
#         ).otherwise(F.col("currency_amount_tmp"))
#     )

#     .withColumn(
#         "vnd_amount",
#         F.when(
#             F.col("payment_index") == F.col("total_payment_periods_fix"),
#             F.col("total_vnd_amount") - F.coalesce(F.col("prev_vnd_amount"), F.lit(0.0))
#         ).otherwise(F.col("vnd_amount_tmp"))
#     )

#     .withColumn(
#         "is_actual",
#         F.when(F.col("payment_date") <= F.to_date(F.col("update_at")), F.lit(True))
#          .otherwise(F.lit(False))
#     )
# )

# # -----------------------
# # 5) Output
# # -----------------------
# df_result = df4.select(
#     F.col("payment_date").cast("timestamp").alias("report_date"),
#     F.expr("year(payment_date)").alias("report_year"),
#     F.expr("month(payment_date)").alias("report_month"),
#     F.lit("N/A").alias("product_category_code"),
#     F.col("product_category"),
#     F.expr("CASE WHEN contract_type = 'New' THEN 'DT mới' WHEN contract_type = 'Renew' THEN 'DT renew' WHEN contract_type = 'Upsales' THEN 'DT Upsales' ELSE 'N/A' END").alias("product_growth_type"),
#     F.col("product_type").alias("product_service_group"),
#     F.expr("CAST( coalesce(vnd_amount, 0) AS DECIMAL(18,2))").alias("revenue_amount"),
#     F.col("usd_to_vnd").cast("double").alias("usd_to_vnd"),
#     F.expr("coalesce(company_alias, 'N/A')").alias("company_alias"),
#     F.expr("CASE WHEN contract_type = 'New' THEN 'Doanh thu mới' WHEN contract_type = 'Renew' THEN 'Doanh thu gia hạn' WHEN contract_type = 'Upsales' THEN 'Doanh thu bán gia tăng' ELSE 'N/A' END").alias("revenue_type"),
#     F.lit('N/A').alias('channel_name'),
#     F.lit(False).alias('is_soc'),
#     F.lit('N/A').alias('department_alias'),
#     F.expr("coalesce(customer_segment_l1, '-')").alias('customer_segment_l1'),
#     F.expr("coalesce(customer_segment_l2, '-')").alias('customer_segment_l2'),
#     F.expr("coalesce(customer_segment_l3, '-')").alias('customer_segment_l3'),
#     F.expr("coalesce(customer_segment_l1, '-')").alias('revenue_group'),
#     F.lit(0).alias('vat'),
#     F.lit("Tạm tính").alias("payment_stage"),
#     F.expr("coalesce(am_username, '-')").alias('am_username'),
#     F.expr("coalesce(sale_admin, '-')").alias('presale_name'),
#     F.col("currency_code_u").alias("currency_code"),
#     F.lit("N/A").alias("business_unit_level_1"),
#     F.expr("coalesce(territory_name, 'Others')").alias('territory_name'),
    
#     F.col("deal_id"),
#     F.col("update_at"),
#     F.expr("CAST( coalesce(currency_amount, 0) AS DECIMAL(18,2))").alias("currency_amount"),
    
#     F.col("is_actual"),
#     F.col("due_date").cast("timestamp").alias("due_date"),

#     F.col("deployment_type"),
#     F.col("pricebook_id"),
#     F.col("is_recurring").cast("boolean").alias("is_recurring"),
#     F.col("product_index"),

#     F.col("company_id").alias("customer_id"),
#     F.expr("coalesce(company_name, 'N/A')").alias("customer_name"),
#     F.expr("coalesce(customer_group, 'Chưa xác định')").alias('customer_group'),

#     F.col("product_category_index"),
#     F.col("contract_id"),
#     F.col("payment_index").cast("bigint").alias("payment_index"),
    
#     F.col("crm_contract_allocations.id").alias("contract_allocation_id"),
# ).filter(F.col("payment_date").isNotNull())
# # -----------------------
# # 6) Write chống DOUBLE FILE
# # -----------------------
# tgt_table = "bi_silver.finance_allocated_revenue"
# tgt_path  = "s3a://bi-silver/finance_allocated_revenue"

# df_result.repartition(1).write \
#   .mode("overwrite") \
#   .format("parquet") \
#   .option("path", tgt_path) \
#   .saveAsTable(tgt_table)

# spark.catalog.refreshTable(tgt_table)
# print(tgt_table)
# print("Rows:", df_result.count())
# # df_result.show(5, truncate=False)