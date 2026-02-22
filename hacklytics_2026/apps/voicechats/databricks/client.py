import logging
import math
import json
import time
from hashlib import sha256
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

import requests

LOGGER = logging.getLogger(__name__)
_VALIDATION_CACHE_TTL_SECONDS = 60
_validation_cache: dict[str, dict[str, Any]] = {}


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


def _token_fingerprint(token: str) -> str:
    if not token:
        return "none"
    digest = sha256(token.encode("utf-8")).hexdigest()[:12]
    return f"len:{len(token)}:{digest}"


def _validation_cache_key(config: DatabricksConfig) -> str:
    raw = f"{config.host}|{config.endpoint}|{_token_fingerprint(config.token)}"
    return sha256(raw.encode("utf-8")).hexdigest()


def _validate_host(host: str) -> str | None:
    if not host:
        return "DATABRICKS_HOST is missing."
    parsed = urlparse(host)
    if parsed.scheme not in {"http", "https"}:
        return "DATABRICKS_HOST must start with http:// or https://."
    if not parsed.netloc:
        return "DATABRICKS_HOST is not a valid URL."
    return None


def _clamp01(value: float) -> float:
    return max(0.0, min(1.0, value))


def _safe_sigmoid(value: float) -> float:
    if value >= 0:
        z = math.exp(-value)
        return 1.0 / (1.0 + z)
    z = math.exp(value)
    return z / (1.0 + z)


def _load_endpoint_specs(settings_obj: Any) -> dict[str, dict[str, Any]]:
    raw = getattr(settings_obj, "DATABRICKS_ENDPOINT_OUTPUT_SPECS", "")
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str) and raw.strip():
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                return parsed
        except json.JSONDecodeError:
            LOGGER.warning("Invalid DATABRICKS_ENDPOINT_OUTPUT_SPECS JSON; ignoring.")
    return {}


def _resolve_output_spec(settings_obj: Any, endpoint_id: str) -> dict[str, Any]:
    endpoint_specs = _load_endpoint_specs(settings_obj)
    if endpoint_id in endpoint_specs and isinstance(endpoint_specs[endpoint_id], dict):
        return endpoint_specs[endpoint_id]
    return {
        "score_type": _get_setting(settings_obj, "DATABRICKS_SCORE_TYPE", "none").lower(),
        "score_field": _get_setting(settings_obj, "DATABRICKS_SCORE_FIELD"),
        "label_field": _get_setting(settings_obj, "DATABRICKS_LABEL_FIELD"),
        "positive_class": _get_setting(settings_obj, "DATABRICKS_POSITIVE_CLASS"),
    }


def _extract_field(payload: Any, field_path: str) -> Any:
    if not field_path:
        return None
    current = payload
    for segment in field_path.split("."):
        if isinstance(current, dict):
            current = current.get(segment)
        elif isinstance(current, list):
            if not current:
                return None
            current = current[0]
            if isinstance(current, dict):
                current = current.get(segment)
            else:
                return None
        else:
            return None
    return current


def _find_first_numeric(payload: Any) -> float | None:
    if isinstance(payload, (int, float)):
        return float(payload)
    if isinstance(payload, dict):
        for key in ("score", "probability", "confidence", "toxicity", "prediction"):
            value = payload.get(key)
            if isinstance(value, (int, float)):
                return float(value)
        for value in payload.values():
            found = _find_first_numeric(value)
            if found is not None:
                return found
    if isinstance(payload, list):
        for item in payload:
            found = _find_first_numeric(item)
            if found is not None:
                return found
    return None


def _find_first_label(payload: Any) -> str | None:
    if isinstance(payload, str):
        return payload
    if isinstance(payload, dict):
        for key in ("label", "class", "prediction", "category"):
            value = payload.get(key)
            if isinstance(value, str):
                return value
        for value in payload.values():
            found = _find_first_label(value)
            if found:
                return found
    if isinstance(payload, list):
        for item in payload:
            found = _find_first_label(item)
            if found:
                return found
    return None


def normalize_databricks_output(raw_payload: Any, settings_obj: Any, endpoint_id: str) -> dict[str, Any]:
    spec = _resolve_output_spec(settings_obj, endpoint_id)
    score_type = str(spec.get("score_type", "none") or "none").lower()
    score_field = str(spec.get("score_field", "") or "")
    label_field = str(spec.get("label_field", "") or "")
    positive_class = str(spec.get("positive_class", "") or "").strip()
    threshold = float(_get_setting(settings_obj, "TOXICITY_THRESHOLD", "0.7") or 0.7)

    raw_score = _extract_field(raw_payload, score_field) if score_field else _find_first_numeric(raw_payload)
    if isinstance(raw_score, bool) or not isinstance(raw_score, (int, float)):
        raw_score = None
    else:
        raw_score = float(raw_score)

    label_value = _extract_field(raw_payload, label_field) if label_field else _find_first_label(raw_payload)
    label = str(label_value).strip() if isinstance(label_value, str) and str(label_value).strip() else None

    score: float | None = None
    if raw_score is not None:
        if score_type == "probability_0_1":
            score = _clamp01(raw_score)
        elif score_type == "percent_0_100":
            score = _clamp01(raw_score / 100.0)
        elif score_type == "logit":
            score = _clamp01(_safe_sigmoid(raw_score))
        elif score_type in {"none", "unknown"}:
            score = None

    if label and positive_class:
        flagged = label.lower() == positive_class.lower()
    elif score is not None:
        flagged = score >= threshold
    else:
        flagged = False

    severity = int(round(score * 100)) if score is not None else None
    return {
        "label": label,
        "score": score,
        "severity": severity,
        "threshold_used": threshold,
        "flagged": flagged,
        "endpoint_id": endpoint_id,
        "raw": raw_payload,
        "score_type": score_type,
    }


def validate_databricks_endpoint(settings_obj: Any, force: bool = False) -> tuple[bool, dict[str, Any]]:
    now = time.time()
    config = _read_config(settings_obj)
    cache_key = _validation_cache_key(config)
    cached = _validation_cache.get(cache_key)
    if not force and cached and now - float(cached["checked_at"]) < _VALIDATION_CACHE_TTL_SECONDS:
        return bool(cached["is_valid"]), dict(cached["details"])

    details: dict[str, Any] = {
        "resolved_url": None,
        "status_code": None,
        "error": None,
        "checked_at_epoch": int(now),
        "endpoint_id": config.endpoint,
    }

    host_error = _validate_host(config.host)
    if host_error:
        details["error"] = host_error
        _validation_cache[cache_key] = {"checked_at": now, "is_valid": False, "details": details}
        return False, details

    if not config.token:
        details["error"] = "DATABRICKS_TOKEN is missing."
        _validation_cache[cache_key] = {"checked_at": now, "is_valid": False, "details": details}
        return False, details

    if not config.endpoint:
        details["error"] = "DATABRICKS_ENDPOINT (or DATABRICKS_SERVING_ENDPOINT_NAME) is missing."
        _validation_cache[cache_key] = {"checked_at": now, "is_valid": False, "details": details}
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
            _validation_cache[cache_key] = {"checked_at": now, "is_valid": is_valid, "details": details}
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
            _validation_cache[cache_key] = {"checked_at": now, "is_valid": is_valid, "details": details}
            return is_valid, details

        details["error"] = f"Unexpected status code while validating endpoint: {post_response.status_code}"
        _validation_cache[cache_key] = {"checked_at": now, "is_valid": False, "details": details}
        return False, details
    except requests.RequestException as exc:
        details["error"] = str(exc)
        LOGGER.warning("Databricks validation failed: %s", exc)
        _validation_cache[cache_key] = {"checked_at": now, "is_valid": False, "details": details}
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
    raw_payload = response.json() if response.content else {}
    return normalize_databricks_output(raw_payload, settings_obj, config.endpoint)
