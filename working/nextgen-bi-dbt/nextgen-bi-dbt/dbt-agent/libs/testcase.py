from dataclasses import dataclass
from .om_client import OpenMetadataSDK
from pathlib import Path


@dataclass
class TestCaseDTO:
    table: str
    column: str | None
    test_type: str
    entity_type: str = "table"
    

import yaml



def load_yaml(path: str):
    with open(path, "r", encoding="utf8") as f:
        return yaml.safe_load(f)


def parse_tests(data) -> list[TestCaseDTO]:

    tests = []
    #
    # dbt model
    #
    for model in data.get("models", []):
        table = model["name"]
        for col in model.get("columns", []):
            column = col["name"]
            all_tests = []
            all_tests.extend(col.get("tests", []))
            all_tests.extend(col.get("data_tests", []))

            for t in all_tests:
                if isinstance(t, str):
                    tests.append(
                        TestCaseDTO(
                            table=table,
                            column=column,
                            test_type=t,
                        )
                    )
                elif isinstance(t, dict):
                    name = list(t.keys())[0]
                    tests.append(
                        TestCaseDTO(
                            table=table,
                            column=column,
                            test_type=name,
                        )
                    )
    #
    # dbt source
    #
    for source in data.get("sources", []):
        for table_obj in source.get("tables", []):
            table = table_obj["name"]
            for col in table_obj.get("columns", []):
                column = col["name"]
                all_tests = []
                all_tests.extend(col.get("tests", []))
                all_tests.extend(col.get("data_tests", []))
                for t in all_tests:
                    if isinstance(t, str):
                        tests.append(
                            TestCaseDTO(
                                table=table,
                                column=column,
                                test_type=t,
                            )
                        )
                    elif isinstance(t, dict):
                        name = list(t.keys())[0]
                        tests.append(
                            TestCaseDTO(
                                table=table,
                                column=column,
                                test_type=name,
                            )
                        )

    return tests

def make_testcase(data, client: OpenMetadataSDK):

    TEST_MAPPING = {
        "not_null": "columnValuesToBeNotNull",
        "unique": "columnValuesToBeUnique",
    }
    tests: list[TestCaseDTO] = parse_tests(data)

    for t in tests:
        definition = TEST_MAPPING.get(t.test_type)
        if definition is None:
            continue

        table_fqn = f"hive.bi_silver.crm.{t.table}"

        client.create_test_case(
            table_fqn=table_fqn,
            column=t.column,
            test_definition=definition,
            name=f"{t.table}_{t.column}_{t.test_type}",
        )

        print(f"Created {t.table}.{t.column} {t.test_type}")
        

def sync_dbt_testcase(
    folder,
    service,
    database,
    schema,
):
    om = OpenMetadataSDK()
    for file in Path(folder).rglob("*.yml"):
        print(f"Processing file {file}")
        doc = yaml.safe_load(
            file.read_text(
                encoding="utf-8"
            )
        )
        if not doc:
            continue

        make_testcase(doc, om)
        print(f"Done file {file}")
