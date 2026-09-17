import trino

from trino.auth import BasicAuthentication

from .trino_config import TrinoConfig


class TrinoClient:

    def __init__(
        self,
        config: TrinoConfig,
        schema: str,
    ):
        self.config = config
        self.schema = schema

        self.connection = trino.dbapi.connect(
            host=config.host,
            port=config.port,
            user=config.user,
            catalog=config.catalog,
            schema=schema,
            http_scheme=config.http_scheme,
            auth=BasicAuthentication(
                config.user,
                config.password,
            ),
            verify=config.verify_ssl,
        )

        self.cursor = self.connection.cursor()

    def execute(self, query: str):
        self.cursor.execute(query)

    def get_tables(self) -> list[str]:
        self.cursor.execute(
            f"""
            SELECT table_name
            FROM {self.config.catalog}.information_schema.tables
            WHERE table_schema = '{self.schema}'
              AND table_type = 'BASE TABLE'
            """
        )

        return [
            row[0]
            for row in self.cursor.fetchall()
        ]

    def get_create_table_ddl(
        self,
        full_table_name: str,
    ) -> str:
        self.cursor.execute(
            f"SHOW CREATE TABLE {full_table_name}"
        )

        return self.cursor.fetchone()[0]

    def table_exists(
        self,
        catalog: str,
        schema: str,
        table: str,
    ) -> bool:

        self.cursor.execute(
            f"""
            SELECT 1
            FROM {catalog}.information_schema.tables
            WHERE table_catalog = '{catalog}'
              AND table_schema = '{schema}'
              AND table_name = '{table}'
            LIMIT 1
            """
        )

        return self.cursor.fetchone() is not None

    def close(self):
        try:
            self.cursor.close()
        finally:
            self.connection.close()