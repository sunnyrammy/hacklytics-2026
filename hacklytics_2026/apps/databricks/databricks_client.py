import os
from collections.abc import Sequence
from typing import Any

try:
    from databricks import sql
except ImportError:  # pragma: no cover
    sql = None


class DatabricksClient:
    def __init__(self) -> None:
        self.server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
        self.http_path = os.getenv("DATABRICKS_HTTP_PATH")
        self.access_token = os.getenv("DATABRICKS_TOKEN")

        if not self.server_hostname or not self.http_path or not self.access_token:
            raise ValueError("Databricks configuration is incomplete.")

    def _connect(self):
        if sql is None:
            raise RuntimeError("Databricks SQL Connector is not installed.")

        return sql.connect(
            server_hostname=self.server_hostname,
            http_path=self.http_path,
            access_token=self.access_token,
        )

    def query_all(self, sql_text: str, params: Sequence[Any] | None = None) -> list[dict[str, Any]]:
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql_text, params or ())
                rows = cursor.fetchall()
                column_names = [column[0] for column in (cursor.description or [])]

        return [dict(zip(column_names, row)) for row in rows]

    def execute(self, sql_text: str, params: Sequence[Any] | None = None) -> None:
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql_text, params or ())
