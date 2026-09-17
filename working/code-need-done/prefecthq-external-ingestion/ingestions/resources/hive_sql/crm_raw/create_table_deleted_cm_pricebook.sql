CREATE TABLE IF NOT EXISTS crm_raw.deleted_cm_pricebook (
          id STRING
        )
        USING PARQUET
        LOCATION 's3a://vcs-raw/crm-raw/deleted_cm_pricebook'