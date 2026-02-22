import os
import time
from collections.abc import Sequence
from typing import Any
from urllib.parse import urlparse

import requests

try:
    from databricks import sql
except ImportError:  # pragma: no cover
    sql = None


class DatabricksAPIError(Exception):
    pass


def _read_env(*keys: str) -> str:
    for key in keys:
        value = os.getenv(key)
        if value and value.strip():
            return value.strip()
    return ""


def _extract_host_from_url(url: str) -> str:
    parsed = urlparse(url)
    if parsed.scheme in {"http", "https"} and parsed.netloc:
        return f"{parsed.scheme}://{parsed.netloc}"
    return ""


def _extract_endpoint_name(endpoint: str) -> str:
    if not endpoint:
        return ""
    parsed = urlparse(endpoint)
    if parsed.scheme in {"http", "https"} and parsed.netloc:
        path_parts = [part for part in parsed.path.split("/") if part]
    else:
        path_parts = [part for part in endpoint.split("/") if part]

    if "serving-endpoints" in path_parts:
        idx = path_parts.index("serving-endpoints")
        if idx + 1 < len(path_parts):
            return path_parts[idx + 1].strip()
    return endpoint.strip().strip("/")


def read_endpoint_config() -> tuple[str, str]:
    raw_endpoint = _read_env(
        "DATABRICKS_SERVING_ENDPOINT_NAME",
        "DATABRICKS_ENDPOINT",
        "databricks_endpoint",
    )
    endpoint_host = _extract_host_from_url(raw_endpoint) if raw_endpoint else ""
    endpoint_name = _extract_endpoint_name(raw_endpoint) if raw_endpoint else ""
    return endpoint_host, endpoint_name


class DatabricksClient:
    CONNECT_TIMEOUT_S = 3
    READ_TIMEOUT_S = 30
    QUERY_RETRY_ATTEMPTS = 3

    def __init__(self) -> None:
        endpoint_host, endpoint_name = read_endpoint_config()
        explicit_host = _read_env("DATABRICKS_HOST", "databricks_host")
        self.host = (explicit_host or endpoint_host).rstrip("/")
        self.token = _read_env("DATABRICKS_TOKEN", "databricks_token")
        self.server_hostname = _read_env("DATABRICKS_SERVER_HOSTNAME", "databricks_server_hostname")
        self.http_path = _read_env("DATABRICKS_HTTP_PATH", "databricks_http_path")
        self.access_token = self.token
        self.default_endpoint_name = endpoint_name

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
        endpoint_value = (endpoint_name or "").strip() or self.default_endpoint_name
        if not endpoint_value:
            raise ValueError("Databricks serving endpoint is not configured.")

        if endpoint_value.startswith("http://") or endpoint_value.startswith("https://"):
            url = endpoint_value
        else:
            path = f"/serving-endpoints/{endpoint_value}/invocations"
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
