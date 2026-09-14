import sys
from libs.task import sync_dbt_metadata

if __name__ == "__main__":
    sync_dbt_metadata(
        folder=sys.argv[1],
        service="GenBI-DB",
        database="hive",
        schema="bi_silver",
    )
    