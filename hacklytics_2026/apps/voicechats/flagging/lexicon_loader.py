import json
from pathlib import Path
from typing import Any

SUPPORTED_CATEGORIES = {"toxic", "severe_toxic", "obscene", "threat", "insult", "identity_hate"}


def _normalize_type(term: str, value: Any) -> str:
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"word", "phrase"}:
            return lowered
    return "phrase" if " " in term else "word"


def _normalize_category(value: Any) -> str:
    category = str(value or "toxic").strip().lower()
    if not category:
        return "toxic"
    if category in SUPPORTED_CATEGORIES:
        return category
    return category


def load_lexicon(path: str | Path) -> list[dict[str, Any]]:
    with Path(path).open("r", encoding="utf-8") as handle:
        payload = json.load(handle)

    if not isinstance(payload, list):
        return []

    lexicon: list[dict[str, Any]] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        term = str(item.get("term", "")).strip()
        if not term:
            continue

        severity_raw = item.get("severity", 1)
        try:
            severity = int(severity_raw)
        except (TypeError, ValueError):
            severity = 1
        severity = max(1, min(5, severity))

        entry = {
            "term": term,
            "category": _normalize_category(item.get("category")),
            "severity": severity,
            "type": _normalize_type(term, item.get("type")),
        }
        lexicon.append(entry)
    return lexicon
