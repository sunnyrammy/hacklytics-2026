import os
import time
from collections.abc import Sequence
from typing import Any

import requests

try:
    from databricks import sql
except ImportError:  # pragma: no cover
    sql = None


class DatabricksAPIError(Exception):
    pass


class DatabricksClient:
    CONNECT_TIMEOUT_S = 3
    READ_TIMEOUT_S = 30
    QUERY_RETRY_ATTEMPTS = 3

    def __init__(self) -> None:
        self.host = (os.getenv("DATABRICKS_HOST") or "").rstrip("/")
        self.token = os.getenv("DATABRICKS_TOKEN")
        self.server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
        self.http_path = os.getenv("DATABRICKS_HTTP_PATH")
        self.access_token = os.getenv("DATABRICKS_TOKEN")

        if not self.access_token:
            raise ValueError("Databricks configuration is incomplete.")
        self._sql_enabled = bool(self.server_hostname and self.http_path)

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

    def _build_url(self, path: str) -> str:
        if not self.host:
            raise ValueError("Databricks host is not configured.")
        if path.startswith("http://") or path.startswith("https://"):
            return path
        normalized_path = path if path.startswith("/") else f"/{path}"
        return f"{self.host}{normalized_path}"

    def _raise_for_response(self, response: requests.Response, action: str) -> None:
        if response.ok:
            return
        body = (response.text or "").strip()
        detail = body[:500] if body else "No response body."
        raise DatabricksAPIError(
            f"{action} failed with status {response.status_code}: {detail}"
        )

    def get_json(self, path: str) -> dict[str, Any]:
        url = self._build_url(path)
        try:
            response = requests.get(
                url,
                headers=self._headers(),
                timeout=(self.CONNECT_TIMEOUT_S, self.READ_TIMEOUT_S),
            )
        except requests.RequestException as exc:
            raise DatabricksAPIError(f"GET request failed: {exc}") from exc
        self._raise_for_response(response, "GET request")
        return response.json()

    def post_json(self, path: str, body: dict[str, Any]) -> dict[str, Any]:
        url = self._build_url(path)
        try:
            response = requests.post(
                url,
                headers=self._headers(),
                json=body,
                timeout=(self.CONNECT_TIMEOUT_S, self.READ_TIMEOUT_S),
            )
        except requests.RequestException as exc:
            raise DatabricksAPIError(f"POST request failed: {exc}") from exc
        self._raise_for_response(response, "POST request")
        return response.json() if response.content else {}

    def _connect(self):
        if not self._sql_enabled:
            raise ValueError("Databricks SQL connection configuration is incomplete.")
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

    def create_serving_endpoint(
        self,
        endpoint_name: str,
        model_full_name: str,
        model_version: str,
        workload_size: str = "Small",
        scale_to_zero: bool = True,
        served_entity_name: str | None = None,
    ) -> dict[str, Any]:
        entity_name = served_entity_name or f"{endpoint_name}-entity"
        body = {
            "name": endpoint_name,
            "config": {
                "served_entities": [
                    {
                        "name": entity_name,
                        "entity_name": model_full_name,
                        "entity_version": str(model_version),
                        "workload_size": workload_size,
                        "scale_to_zero_enabled": scale_to_zero,
                    }
                ]
            },
        }
        return self.post_json("/api/2.0/serving-endpoints", body)

    def get_serving_endpoint(self, endpoint_name: str) -> dict[str, Any]:
        return self.get_json(f"/api/2.0/serving-endpoints/{endpoint_name}")

    def wait_endpoint_ready(
        self, endpoint_name: str, timeout_s: int = 900, poll_s: int = 10
    ) -> dict[str, Any]:
        deadline = time.time() + timeout_s
        latest: dict[str, Any] = {}
        while time.time() < deadline:
            latest = self.get_serving_endpoint(endpoint_name)
            state = (latest.get("state") or {}).get("ready", "")
            if isinstance(state, str) and state.upper() == "READY":
                return latest
            time.sleep(poll_s)
        raise TimeoutError(
            f"Serving endpoint '{endpoint_name}' did not become READY within {timeout_s} seconds."
        )

    def query_serving_endpoint(
        self, endpoint_name: str, payload: dict[str, Any]
    ) -> dict[str, Any]:
        path = f"/serving-endpoints/{endpoint_name}/invocations"
        url = self._build_url(path)
        last_error: Exception | None = None

        for attempt in range(self.QUERY_RETRY_ATTEMPTS):
            try:
                response = requests.post(
                    url,
                    headers=self._headers(),
                    json=payload,
                    timeout=(self.CONNECT_TIMEOUT_S, self.READ_TIMEOUT_S),
                )
            except requests.RequestException as exc:
                last_error = DatabricksAPIError(f"Endpoint invocation failed: {exc}")
                break
            if response.status_code in (429, 503):
                last_error = DatabricksAPIError(
                    f"Query request throttled/unavailable with status {response.status_code}."
                )
                if attempt < self.QUERY_RETRY_ATTEMPTS - 1:
                    time.sleep(2**attempt)
                    continue
            try:
                self._raise_for_response(response, "Endpoint invocation")
            except Exception as exc:
                last_error = exc
                break
            return response.json() if response.content else {}

        if last_error is not None:
            raise last_error
        raise DatabricksAPIError("Endpoint invocation failed after retries.")
