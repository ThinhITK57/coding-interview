#!/usr/bin/env python3
"""Example: integrate lineage hook into a PySpark ETL job."""

from pyspark.sql import SparkSession

from lineage.pyspark_lineage_hook import LineageCollector, TableNode


def main() -> None:
    spark = SparkSession.builder.appName("crm_raw_to_gold").getOrCreate()

    # Example ETL steps
    raw_df = spark.table("hive.bi_raw.crm_deals_raw")
    silver_df = raw_df.dropDuplicates(["deal_id"])
    silver_df.write.mode("overwrite").saveAsTable("hive.bi_silver.crm_deals")

    gold_df = spark.table("hive.bi_silver.crm_deals")
    gold_df.write.mode("overwrite").saveAsTable("hive.bi_gold.fct_crm_deals")

    # Collect lineage edges during pipeline run
    collector = LineageCollector(host_port="http://localhost:8036/api", jwt_token_env="OM_JWT")

    collector.add_edge(
        source=TableNode.from_table_name("crm-raw", "hive.bi_raw.crm_deals_raw"),
        target=TableNode.from_table_name("crm-silver", "hive.bi_silver.crm_deals"),
        description="PySpark ETL raw to silver",
    )

    collector.add_edge(
        source=TableNode.from_table_name("crm-silver", "hive.bi_silver.crm_deals"),
        target=TableNode.from_table_name("crm-gold", "hive.bi_gold.fct_crm_deals"),
        description="PySpark ETL silver to gold",
    )

    config_path = "lineage/generated/lineage_edges.yaml"
    collector.write_config(config_path)
    collector.run_agent(config_path, agent_script_path="lineage/lineage_agent_trino_hive.py")

    spark.stop()


if __name__ == "__main__":
    main()
