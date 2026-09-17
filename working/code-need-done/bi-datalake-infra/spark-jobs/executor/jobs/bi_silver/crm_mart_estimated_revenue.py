# %livy.pyspark
tgt_table = "bi_silver.crm_mart_estimated_revenue"
tgt_path  = "s3a://bi-silver/crm_mart_estimated_revenue"

spark.catalog.clearCache()

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
spark.sql("REFRESH TABLE bi_silver.crm_committed_revenue")
spark.sql("REFRESH TABLE bi_silver.crm_expected_revenue")

allocated_table = spark.sql(
    r"""
   WITH dtcc as (
       SELECT
    CAST(payment_date AS DATE)  AS report_date,
    product_category,
    revenue_segment_l1,
    customer_segment_l1,
    
    COALESCE(customer_segment_l2, '-') AS  customer_segment_l2,
    
    am_group,
    vvip_segment,
    deal_status,
    opportunity_level,
    CASE
            WHEN customer_segment_l1 = 'Nội bộ'
              OR (
                    customer_segment_l1 = 'International'
                AND customer_segment_l2 in ('Viettel Global Partner' , 'Thị trường') 
              )
               THEN 'DT nội bộ + Thị trường (cả SI nội bộ)'
            WHEN customer_segment_l1 = 'International'  AND customer_segment_l2 in ( 'Quốc tế ngoài' , 'Direct / Local Channel' )  THEN 'DT Quốc tế (KH ngoài QT)'
            WHEN customer_segment_l1 = 'Bộ Quốc phòng'
                THEN 'DT BQP (gồm cả SI BQP)'
            WHEN customer_segment_l1 = 'Khách hàng ngoài'
                THEN 'DT ngoài trong nước (gồm cả SI ngoài)'
            ELSE 'Khác'
        END AS customer_segment,
    
    CASE WHEN product_type = 'Renew' AND is_actual = true AND is_in_year = false THEN vnd_amount  ELSE 0 END      
        AS  carryover_renew_committed_revenue_amount,
    CASE WHEN product_type = 'Renew' AND is_actual = true AND is_in_year = true  THEN vnd_amount  ELSE 0  END     
        AS  in_year_renew_committed_revenue_amount,
    CASE WHEN product_type = 'Renew' AND is_actual = false AND is_in_year = true  THEN vnd_amount  ELSE 0  END     
        AS  in_year_renew_provisional_revenue_amount,
    CASE WHEN product_type = 'Renewal' AND is_actual = true THEN vnd_amount  ELSE 0  END    
        AS  renewal_committed_revenue_amount,
    
    CASE WHEN product_type = 'New' AND is_actual = true AND is_in_year = false  THEN vnd_amount  ELSE 0    END 
        AS  carryover_new_revenue_amount,
    CASE WHEN product_type = 'New' AND is_actual = true AND is_in_year = true   THEN vnd_amount  ELSE 0    END 
        AS in_year_new_revenue_amount,
    CASE WHEN product_type = 'New' AND is_actual = true THEN vnd_amount  ELSE 0    END 
        AS new_committed_revenue_amount,
        
    CASE WHEN product_type = 'New' THEN vnd_amount  ELSE 0    END AS new_revenue_amount,
    
    CASE WHEN product_type = 'Upsales' AND is_actual = true AND is_in_year = false  THEN vnd_amount  ELSE 0    END 
        AS carryover_upsales_committed_revenue_amount,
    CASE WHEN product_type = 'Upsales' AND is_actual = true AND is_in_year = true  THEN vnd_amount  ELSE 0    END 
        AS in_year_upsales_committed_revenue_amount,
    CASE WHEN product_type = 'Upsales' AND is_actual = true  THEN vnd_amount  ELSE 0    END 
        AS upsales_committed_revenue_amount,

    CASE WHEN COALESCE(product_type, '') NOT IN ('New', 'Renew', 'Renewal', 'Upsales')
               AND is_actual = true
         THEN vnd_amount ELSE 0 END
        AS other_type_committed_revenue_amount,
    
    CASE WHEN  product_category != 'SI' AND is_actual = true THEN vnd_amount ELSE 0 END AS  committed_non_si_revenue_amount,
    CASE WHEN  product_category = 'SI' AND is_actual = true THEN vnd_amount ELSE 0 END AS  committed_si_revenue_amount,
    CASE WHEN  is_recurring = true AND is_actual = true THEN vnd_amount ELSE 0 END AS  committed_arr_amount,
    CASE WHEN  is_recurring != true AND is_actual = true THEN vnd_amount ELSE 0 END AS  committed_none_arr_amount,
    vnd_amount AS  signed_contract_committed_revenue_amount
    FROM bi_silver.crm_committed_revenue )
    
    SELECT 
    report_date,
    
    COALESCE(product_category, 'UN') as product_category,
    COALESCE(revenue_segment_l1, 'UN') as revenue_segment_l1,
    COALESCE(customer_segment_l1, 'UN') as customer_segment_l1,
    COALESCE(customer_segment_l2, 'UN') as customer_segment_l2,
    COALESCE(am_group, 'UN') as am_group,
    COALESCE(vvip_segment, 'Standard') as vvip_segment,
    COALESCE(deal_status, 'UN') AS deal_status,
    COALESCE(opportunity_level, 'UN') AS opportunity_level,
    COALESCE(customer_segment, 'UN') as customer_segment,
    
    sum(t.carryover_renew_committed_revenue_amount) as carryover_renew_committed_revenue_amount,
    sum(t.in_year_renew_committed_revenue_amount) as in_year_renew_committed_revenue_amount,
    sum(t.renewal_committed_revenue_amount) as renewal_committed_revenue_amount,
    sum(t.in_year_renew_provisional_revenue_amount) as in_year_renew_provisional_revenue_amount,
    sum(t.new_committed_revenue_amount) as new_committed_revenue_amount,
    sum(t.carryover_upsales_committed_revenue_amount) as carryover_upsales_committed_revenue_amount,
    sum(t.in_year_upsales_committed_revenue_amount) as in_year_upsales_committed_revenue_amount,
    sum(t.upsales_committed_revenue_amount) as upsales_committed_revenue_amount,
    sum(t.other_type_committed_revenue_amount) as other_type_committed_revenue_amount,
    
    sum( COALESCE(t.carryover_renew_committed_revenue_amount, 0) ) 
        + sum(COALESCE(t.in_year_renew_committed_revenue_amount, 0) ) 
        + sum(COALESCE(t.renewal_committed_revenue_amount, 0) ) 
        + sum(COALESCE(t.in_year_renew_provisional_revenue_amount, 0) ) 
            as renew_estimated_revenue_amount,
    
    sum(t.carryover_new_revenue_amount) as carryover_new_revenue_amount,
    sum(t.in_year_new_revenue_amount) as in_year_new_revenue_amount,
    sum(t.committed_non_si_revenue_amount) as committed_non_si_revenue_amount,
    sum(t.committed_si_revenue_amount) as committed_si_revenue_amount,
    sum(t.committed_arr_amount) as committed_arr_amount,
    sum(t.committed_none_arr_amount) as committed_none_arr_amount,
    
    sum(t.signed_contract_committed_revenue_amount) as signed_contract_committed_revenue_amount,
    
    sum(t.new_revenue_amount)  AS new_revenue_amount
    
    FROM dtcc t
    WHERE report_date is not null
    GROUP BY 1,2,3,4,5,6,7,8,9,10
    """
)
allocated_table.createOrReplaceTempView("allocated_table")

expected_table = spark.sql(
    r"""
    WITH dtdb AS (
    SELECT
    CAST(payment_date AS DATE)  AS report_date,
    product_category,
    revenue_segment_l1,
    customer_segment_l1,
    COALESCE(customer_segment_l2, '-') AS  customer_segment_l2,
    am_group,
    vvip_segment,
    deal_status,
    opportunity_level,
    CASE
            WHEN customer_segment_l1 = 'Nội bộ'
              OR (
                    customer_segment_l1 = 'International'
                AND customer_segment_l2 in ('Viettel Global Partner' , 'Thị trường') 
              )
               THEN 'DT nội bộ + Thị trường (cả SI nội bộ)'
            WHEN customer_segment_l1 = 'International'  AND customer_segment_l2 in ( 'Quốc tế ngoài' , 'Direct / Local Channel' )  THEN 'DT Quốc tế (KH ngoài QT)'
            WHEN customer_segment_l1 = 'Bộ Quốc phòng'
                THEN 'DT BQP (gồm cả SI BQP)'
            WHEN customer_segment_l1 = 'Khách hàng ngoài'
                THEN 'DT ngoài trong nước (gồm cả SI ngoài)'
            ELSE 'Khác'
        END AS customer_segment,
    
    vnd_amount AS product_revenue_amount,
    vnd_amount AS pipeline_revenue_amount,
    CASE WHEN opportunity_level = 'Cao' THEN vnd_amount ELSE 0 END AS high_pipeline_revenue_amount,
    CASE WHEN opportunity_level = 'Thấp' THEN vnd_amount ELSE 0 END AS low_pipeline_revenue_amount,

    CASE WHEN  product_category != 'SI' THEN vnd_amount ELSE 0 END AS expected_non_si_revenue_amount,
    CASE WHEN  product_category = 'SI' THEN vnd_amount ELSE 0 END AS expected_si_revenue_amount,
    CASE WHEN  is_recurring = true THEN vnd_amount ELSE 0 END AS   expected_arr_amount,
    CASE WHEN  is_recurring != true THEN vnd_amount  ELSE 0 END AS   expected_non_arr_amount,
    
    CASE WHEN product_type = 'Renew' AND is_in_year = false THEN vnd_amount  ELSE 0 END      AS  carryover_renew_expected_revenue_amount,
    CASE WHEN product_type = 'Renew' AND is_in_year = true  THEN vnd_amount  ELSE 0  END     AS  in_year_renew_expected_revenue_amount
    
    
    FROM bi_silver.crm_expected_revenue )
    
    SELECT 
    
    report_date,
    COALESCE(product_category, 'UN') as product_category,
    COALESCE(revenue_segment_l1, 'UN') as revenue_segment_l1,
    COALESCE(customer_segment_l1, 'UN') as customer_segment_l1,
    COALESCE(customer_segment_l2, 'UN') as customer_segment_l2,
    COALESCE(am_group, 'UN') as am_group,
    COALESCE(vvip_segment, 'Standard') as vvip_segment,
    COALESCE(deal_status, 'UN') AS deal_status,
    COALESCE(opportunity_level, 'UN') AS opportunity_level,
    COALESCE(customer_segment, 'UN') as customer_segment,
    
    sum(t.product_revenue_amount) as product_revenue_amount,
    sum(t.pipeline_revenue_amount) as pipeline_revenue_amount,
    sum(t.high_pipeline_revenue_amount) AS high_pipeline_revenue_amount,
    sum(t.low_pipeline_revenue_amount) AS low_pipeline_revenue_amount,
    sum(t.expected_non_si_revenue_amount) as expected_non_si_revenue_amount,
    sum(t.expected_si_revenue_amount) as expected_si_revenue_amount,
    sum(t.expected_arr_amount) as expected_arr_amount,
    sum(t.expected_non_arr_amount) as expected_non_arr_amount,
    
    sum(t.carryover_renew_expected_revenue_amount) as carryover_renew_expected_revenue_amount,
    sum(t.in_year_renew_expected_revenue_amount) as in_year_renew_expected_revenue_amount
    
    
    FROM dtdb t
    WHERE report_date is not NULL
    GROUP BY 1,2,3,4,5,6,7,8,9,10
    """
)

expected_table.createOrReplaceTempView("expected_table")

query = """
SELECT 
COALESCE(a.report_date, b.report_date) as report_date,
COALESCE(a.product_category, b.product_category) as product_category,
COALESCE(a.revenue_segment_l1, b.revenue_segment_l1) as revenue_segment_l1,
COALESCE(a.customer_segment_l1, b.customer_segment_l1) as customer_segment_l1,
COALESCE(a.customer_segment_l2, b.customer_segment_l2) as customer_segment_l2,
COALESCE(a.am_group, b.am_group) as am_group,
COALESCE(a.vvip_segment, b.vvip_segment) as vvip_segment,
COALESCE(a.deal_status, b.deal_status) AS deal_status,
COALESCE(a.opportunity_level, b.opportunity_level) AS opportunity_level,
COALESCE(a.customer_segment, b.customer_segment) as customer_segment,

COALESCE(CAST(a.carryover_renew_committed_revenue_amount AS DECIMAL(18,2)), 0.00) AS carryover_renew_committed_revenue_amount,
COALESCE(CAST(a.in_year_renew_committed_revenue_amount AS DECIMAL(18,2)), 0.00) AS in_year_renew_committed_revenue_amount,
COALESCE(CAST(a.carryover_new_revenue_amount AS DECIMAL(18,2)), 0.00) AS carryover_new_revenue_amount,
COALESCE(CAST(a.in_year_new_revenue_amount AS DECIMAL(18,2)), 0.00) AS in_year_new_revenue_amount,
COALESCE(CAST(a.committed_non_si_revenue_amount AS DECIMAL(18,2)), 0.00) AS committed_non_si_revenue_amount,
COALESCE(CAST(a.committed_si_revenue_amount AS DECIMAL(18,2)), 0.00) AS committed_si_revenue_amount,
COALESCE(CAST(a.committed_arr_amount AS DECIMAL(18,2)), 0.00) AS committed_arr_amount,
COALESCE(CAST(a.committed_none_arr_amount AS DECIMAL(18,2)), 0.00) AS committed_none_arr_amount,

COALESCE(CAST(a.renewal_committed_revenue_amount AS DECIMAL(18,2)), 0.00) AS renewal_committed_revenue_amount,
COALESCE(CAST(a.in_year_renew_provisional_revenue_amount AS DECIMAL(18,2)), 0.00) AS in_year_renew_provisional_revenue_amount,
COALESCE(CAST(a.new_committed_revenue_amount AS DECIMAL(18,2)), 0.00) AS new_committed_revenue_amount,
COALESCE(CAST(a.carryover_upsales_committed_revenue_amount AS DECIMAL(18,2)), 0.00) AS carryover_upsales_committed_revenue_amount,
COALESCE(CAST(a.in_year_upsales_committed_revenue_amount AS DECIMAL(18,2)), 0.00) AS in_year_upsales_committed_revenue_amount,
COALESCE(CAST(a.upsales_committed_revenue_amount AS DECIMAL(18,2)), 0.00) AS upsales_committed_revenue_amount,
COALESCE(CAST(a.other_type_committed_revenue_amount AS DECIMAL(18,2)), 0.00) AS other_type_committed_revenue_amount,
COALESCE(CAST(a.renew_estimated_revenue_amount AS DECIMAL(18,2)), 0.00) AS renew_estimated_revenue_amount,

COALESCE(CAST( (
                                COALESCE(a.carryover_renew_committed_revenue_amount, 0)
                            + COALESCE(a.in_year_renew_committed_revenue_amount, 0)
                            + COALESCE(a.renewal_committed_revenue_amount, 0)
                            + COALESCE(a.in_year_renew_provisional_revenue_amount, 0) 
                        ) AS DECIMAL(18,2)), 0.00)
                        AS renewal_revenue_amount,
COALESCE(CAST(a.new_revenue_amount AS DECIMAL(18,2)), 0.00) AS new_revenue_amount,
COALESCE(
    CAST(a.signed_contract_committed_revenue_amount AS DECIMAL(18,2)),
    0.00
) AS committed_revenue_amount,
COALESCE(
    CAST(
        (
            COALESCE(a.signed_contract_committed_revenue_amount, 0)
            + COALESCE(b.high_pipeline_revenue_amount, 0)
            + COALESCE(b.low_pipeline_revenue_amount, 0)
        ) AS DECIMAL(18,2)
    ),
    0.00
) AS forecast_revenue_amount,

COALESCE(CAST( (
                            COALESCE(a.signed_contract_committed_revenue_amount, 0)
                            + COALESCE(b.product_revenue_amount, 0)
                        ) AS DECIMAL(18,2)), 0.00)
            AS potential_revenue_amount,
            
COALESCE(CAST(b.pipeline_revenue_amount AS DECIMAL(18,2)), 0.00) AS pipeline_revenue_amount,
COALESCE(CAST(b.high_pipeline_revenue_amount AS DECIMAL(18,2)), 0.00) AS high_pipeline_revenue_amount,
COALESCE(CAST(b.low_pipeline_revenue_amount AS DECIMAL(18,2)), 0.00) AS low_pipeline_revenue_amount,
COALESCE(CAST(b.expected_non_si_revenue_amount AS DECIMAL(18,2)), 0.00) AS expected_non_si_revenue_amount,
COALESCE(CAST(b.expected_si_revenue_amount AS DECIMAL(18,2)), 0.00) AS expected_si_revenue_amount,
COALESCE(CAST(b.expected_arr_amount AS DECIMAL(18,2)), 0.00) AS expected_arr_amount,
COALESCE(CAST(b.expected_non_arr_amount AS DECIMAL(18,2)), 0.00) AS expected_non_arr_amount,
COALESCE(CAST(b.in_year_renew_expected_revenue_amount AS DECIMAL(18,2)), 0.00) AS in_year_renew_expected_revenue_amount,
COALESCE(CAST(b.carryover_renew_expected_revenue_amount AS DECIMAL(18,2)), 0.00) AS carryover_renew_expected_revenue_amount,
    
CAST( ( COALESCE(a.committed_non_si_revenue_amount, 0) 
        + COALESCE(b.expected_non_si_revenue_amount, 0) ) AS DECIMAL(18,2))  
            AS  estimated_non_si_revenue_amount,
CAST( ( COALESCE(a.committed_si_revenue_amount, 0) 
        + COALESCE(b.expected_si_revenue_amount, 0) ) AS DECIMAL(18,2))
            AS  estimated_si_revenue_amount,
COALESCE(
    CAST(
        (
            COALESCE(a.signed_contract_committed_revenue_amount, 0)
            + COALESCE(b.pipeline_revenue_amount, 0)
        ) AS DECIMAL(18,2)
    ),
    0.00)
         AS  estimated_revenue_amount,

CAST( ( COALESCE(a.committed_arr_amount, 0) + COALESCE(b.expected_arr_amount, 0) ) AS DECIMAL(18,2))  AS  arr_revenue_amount,

CAST( ( COALESCE(a.signed_contract_committed_revenue_amount, 0) 
    ) AS DECIMAL(18,2)) 
            AS  allocation_revenue_amount

FROM allocated_table a
FULL OUTER JOIN expected_table b
    ON a.report_date = b.report_date 
    AND a.product_category = b.product_category
    AND a.am_group = b.am_group
    AND a.vvip_segment = b.vvip_segment
    AND a.customer_segment = b.customer_segment
    AND a.revenue_segment_l1 = b.revenue_segment_l1
    AND a.customer_segment_l1 = b.customer_segment_l1
    AND a.customer_segment_l2 = b.customer_segment_l2
    AND a.deal_status = b.deal_status
    AND a.opportunity_level = b.opportunity_level

"""

df_final = spark.sql(query)

df_final.createOrReplaceTempView("final_table")

df = spark.sql("""
select 
t1.* ,
t2.product_service_group,
t2.product_service_group_vcs

from final_table t1
left join bi_silver.crm_product_service_group_mapping t2 on lower(t1.product_category) = lower(t2.product_category)
""")


# 2) Write
df.repartition(1).write \
  .mode("overwrite") \
  .format("parquet") \
  .save(tgt_path)
  
#   .save(tgt_path)
# .option("path", tgt_path) \
    # .saveAsTable(tgt_table)

spark.catalog.refreshTable(tgt_table)

print(tgt_table)
print("Rows:", df.count())


# | Field                                      | Tên tiếng Việt                           |
# | ------------------------------------------ | ---------------------------------------- |
# | report_date                                | Ngày ghi nhận doanh thu                  |
# | product_category                           | Nhóm sản phẩm                            |
# | customer_segment_l1                        | Phân khúc khách hàng cấp 1               |
# | customer_segment_l2                        | Phân khúc khách hàng cấp 2               |
# | am_group                                   | Nhóm AM                                  |
# | customer_group                             | Nhóm khách hàng                          |
# | customer_segment                           | Nhóm khách hàng báo cáo                  |
# | carryover_renew_committed_revenue_amount   | Doanh thu Renew chuyển năm đã chốt       |
# | in_year_renew_committed_revenue_amount     | Doanh thu Renew trong năm đã chốt        |
# | carryover_new_revenue_amount               | Doanh thu New chuyển năm đã chốt         |
# | in_year_new_revenue_amount                 | Doanh thu New trong năm đã chốt          |
# | committed_non_si_revenue_amount            | Doanh thu đã chốt ngoài SI               |
# | committed_si_revenue_amount                | Doanh thu SI đã chốt                     |
# | committed_arr_amount                       | Doanh thu ARR đã chốt                    |
# | committed_none_arr_amount                  | Doanh thu không ARR đã chốt              |
# | renewal_committed_revenue_amount           | Doanh thu Renewal đã chốt                |
# | in_year_renew_provisional_revenue_amount   | Doanh thu Renew trong năm tạm tính       |
# | new_committed_revenue_amount               | Tổng doanh thu New đã chốt               |
# | carryover_upsales_committed_revenue_amount | Doanh thu Upsales chuyển năm đã chốt     |
# | in_year_upsales_committed_revenue_amount   | Doanh thu Upsales trong năm đã chốt      |
# | upsales_committed_revenue_amount           | Tổng doanh thu Upsales đã chốt           |
# | other_type_committed_revenue_amount        | Doanh thu loại khác đã chốt              |
# | renewal_revenue_amount                     | Doanh thu Renewal ước tính               |
# | new_revenue_amount                         | Tổng doanh thu New                       |
# | committed_revenue_amount                   | Doanh thu cam kết                        |
# | forecast_revenue_amount                    | Doanh thu dự báo                         |
# | pipeline_revenue_amount                    | Doanh thu Pipeline quy đổi theo xác suất |
# | forecast_non_si_revenue_amount             | Doanh thu dự báo ngoài SI                |
# | forecast_si_revenue_amount                 | Doanh thu dự báo SI                      |
# | forecast_arr_amount                        | Doanh thu ARR dự báo                     |
# | forecast_non_arr_amount                    | Doanh thu không ARR dự báo               |
# | in_year_renew_forecast_revenue_amount      | Doanh thu Renew trong năm dự báo         |
# | carryover_renew_forecast_revenue_amount    | Doanh thu Renew chuyển năm dự báo        |
# | estimated_non_si_revenue_amount            | Doanh thu ngoài SI ước tính              |
# | estimated_si_revenue_amount                | Doanh thu SI ước tính                    |
# | estimated_revenue_amount                   | Tổng doanh thu ước tính                  |
# | arr_revenue_amount                         | Tổng doanh thu ARR                       |
# | allocation_revenue_amount                  | Doanh thu phân bổ                        |

# | Hiện tại                        | Đề xuất                     |
# | ------------------------------- | --------------------------- |
# | committed_revenue_amount        | Doanh thu cam kết ước đạt   |
# | forecast_revenue_amount         | Tổng doanh thu dự báo       |
# | estimated_revenue_amount        | Doanh thu kỳ vọng           |
# | allocation_revenue_amount       | Doanh thu phục vụ phân bổ   |
# | renewal_revenue_amount          | Doanh thu Renew ước tính    |
# | new_revenue_amount              | Doanh thu New đã chốt       |
# | committed_non_si_revenue_amount | Doanh thu ngoài SI đã chốt  |
# | committed_none_arr_amount       | Doanh thu không ARR đã chốt |


# | Chỉ tiêu                                | Ý nghĩa                                | Thành phần                                     |
# | --------------------------------------- | -------------------------------------- | ---------------------------------------------- |
# | Doanh thu đã chốt (Committed Revenue)   | Chỉ gồm doanh thu đã ký/chốt           | Committed                                      |
# | Doanh thu cam kết (Estimated Committed) | Doanh thu đã chốt + Renew tạm tính     | Committed + Renew Provisional × 90%            |
# | Doanh thu dự báo (Forecast Revenue)     | Doanh thu cam kết + Pipeline           | Committed + Renew Provisional × 90% + Pipeline |
# | Doanh thu sản phẩm (Product Revenue)    | Toàn bộ Opportunity chưa nhân xác suất | Product Revenue                                |
