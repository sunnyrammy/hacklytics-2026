import logging
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

import requests

LOGGER = logging.getLogger(__name__)
_VALIDATION_CACHE_TTL_SECONDS = 60
_validation_cache: dict[str, Any] = {
    "checked_at": 0.0,
    "is_valid": False,
    "details": {"error": "Not validated yet."},
}


@dataclass
class DatabricksConfig:
    host: str
    token: str
    endpoint: str
    input_column: str


def _get_setting(settings_obj: Any, key: str, default: str = "") -> str:
    value = getattr(settings_obj, key, None)
    if value is None:
        return default
    return str(value)


def _read_config(settings_obj: Any) -> DatabricksConfig:
    endpoint_name = _get_setting(settings_obj, "DATABRICKS_ENDPOINT") or _get_setting(
        settings_obj, "DATABRICKS_SERVING_ENDPOINT_NAME"
    )
    return DatabricksConfig(
        host=_get_setting(settings_obj, "DATABRICKS_HOST").rstrip("/"),
        token=_get_setting(settings_obj, "DATABRICKS_TOKEN"),
        endpoint=endpoint_name.strip(),
        input_column=_get_setting(settings_obj, "DATABRICKS_INPUT_COLUMN", "comment_text") or "comment_text",
    )


def _resolve_invocations_url(config: DatabricksConfig) -> str:
    endpoint = config.endpoint
    if endpoint.startswith("http://") or endpoint.startswith("https://"):
        return endpoint
    if endpoint.startswith("/"):
        return f"{config.host}{endpoint}"
    return f"{config.host}/serving-endpoints/{endpoint}/invocations"


def _validate_host(host: str) -> str | None:
    if not host:
        return "DATABRICKS_HOST is missing."
    parsed = urlparse(host)
    if parsed.scheme not in {"http", "https"}:
        return "DATABRICKS_HOST must start with http:// or https://."
    if not parsed.netloc:
        return "DATABRICKS_HOST is not a valid URL."
    return None


def validate_databricks_endpoint(settings_obj: Any, force: bool = False) -> tuple[bool, dict[str, Any]]:
    now = time.time()
    if not force and now - float(_validation_cache["checked_at"]) < _VALIDATION_CACHE_TTL_SECONDS:
        return bool(_validation_cache["is_valid"]), dict(_validation_cache["details"])

    config = _read_config(settings_obj)
    details: dict[str, Any] = {
        "resolved_url": None,
        "status_code": None,
        "error": None,
        "checked_at_epoch": int(now),
    }

    host_error = _validate_host(config.host)
    if host_error:
        details["error"] = host_error
        _validation_cache.update({"checked_at": now, "is_valid": False, "details": details})
        return False, details

    if not config.token:
        details["error"] = "DATABRICKS_TOKEN is missing."
        _validation_cache.update({"checked_at": now, "is_valid": False, "details": details})
        return False, details

    if not config.endpoint:
        details["error"] = "DATABRICKS_ENDPOINT (or DATABRICKS_SERVING_ENDPOINT_NAME) is missing."
        _validation_cache.update({"checked_at": now, "is_valid": False, "details": details})
        return False, details

    info_url = f"{config.host}/api/2.0/serving-endpoints/{config.endpoint}"
    if config.endpoint.startswith("http://") or config.endpoint.startswith("https://") or config.endpoint.startswith("/"):
        info_url = _resolve_invocations_url(config)

    details["resolved_url"] = info_url
    headers = {"Authorization": f"Bearer {config.token}"}
    try:
        response = requests.get(info_url, headers=headers, timeout=(3, 10))
        details["status_code"] = response.status_code
        if response.status_code in {200, 401, 403}:
            is_valid = response.status_code == 200
            if response.status_code in {401, 403}:
                details["error"] = "Authentication/authorization failed while validating endpoint."
            _validation_cache.update({"checked_at": now, "is_valid": is_valid, "details": details})
            return is_valid, details

        # If GET info route is not usable, try invocations route with a tiny payload.
        invocations_url = _resolve_invocations_url(config)
        details["resolved_url"] = invocations_url
        payload = {"dataframe_records": [{config.input_column: "ping"}]}
        post_response = requests.post(
            invocations_url,
            headers={**headers, "Content-Type": "application/json"},
            json=payload,
            timeout=(3, 10),
        )
        details["status_code"] = post_response.status_code
        if post_response.status_code in {200, 400, 401, 403, 429, 503}:
            is_valid = post_response.status_code in {200, 400, 429, 503}
            if post_response.status_code in {401, 403}:
                details["error"] = "Authentication/authorization failed while validating endpoint."
            _validation_cache.update({"checked_at": now, "is_valid": is_valid, "details": details})
            return is_valid, details

        details["error"] = f"Unexpected status code while validating endpoint: {post_response.status_code}"
        _validation_cache.update({"checked_at": now, "is_valid": False, "details": details})
        return False, details
    except requests.RequestException as exc:
        details["error"] = str(exc)
        LOGGER.warning("Databricks validation failed: %s", exc)
        _validation_cache.update({"checked_at": now, "is_valid": False, "details": details})
        return False, details


def call_databricks_inference(text: str, settings_obj: Any) -> dict[str, Any]:
    is_valid, details = validate_databricks_endpoint(settings_obj)
    if not is_valid:
        raise RuntimeError(f"Databricks endpoint validation failed: {details.get('error') or 'unreachable endpoint'}")

    config = _read_config(settings_obj)
    if not text.strip():
        raise ValueError("Text for inference must be non-empty.")
    if not config.host or not config.token or not config.endpoint:
        raise ValueError("Databricks inference configuration is incomplete.")

    url = _resolve_invocations_url(config)
    payload = {"dataframe_records": [{config.input_column: text}]}
    response = requests.post(
        url,
        headers={
            "Authorization": f"Bearer {config.token}",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=(3, 30),
    )
    if response.status_code >= 400:
        raise RuntimeError(f"Databricks inference failed with status {response.status_code}: {(response.text or '').strip()[:300]}")
    return response.json() if response.content else {}
