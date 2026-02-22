import json
import logging
import re
from pathlib import Path
from typing import Any

LOGGER = logging.getLogger(__name__)

_WORD_RE = re.compile(r"\w+")
_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")


def load_flag_terms(path: str | Path) -> list[dict[str, Any]]:
    with Path(path).open("r", encoding="utf-8") as handle:
        payload = json.load(handle)

    terms: list[dict[str, Any]] = []
    if not isinstance(payload, list):
        return terms

    for item in payload:
        if not isinstance(item, dict):
            continue
        term = str(item.get("term", "")).strip().lower()
        if not term:
            continue
        severity_raw = item.get("severity", 1)
        try:
            severity = max(1, int(severity_raw))
        except (TypeError, ValueError):
            severity = 1

        normalized_term, _ = normalize_text(term)
        if normalized_term:
            terms.append({"term": normalized_term, "severity": severity})
    return terms


def normalize_text(text: str) -> tuple[str, list[int]]:
    # Build a normalized text representation while keeping an index map into the original text.
    normalized_chars: list[str] = []
    index_map: list[int] = []
    pending_space = False
    saw_content = False

    for idx, ch in enumerate(text):
        lowered = ch.lower()
        if lowered.isalnum():
            if pending_space and saw_content:
                normalized_chars.append(" ")
                index_map.append(idx)
            normalized_chars.append(lowered)
            index_map.append(idx)
            pending_space = False
            saw_content = True
        elif lowered.isspace() or _NON_ALNUM_RE.match(lowered):
            if saw_content:
                pending_space = True

    normalized = "".join(normalized_chars).strip()
    if not normalized:
        return "", []
    return normalized, index_map[: len(normalized)]


def _edit_distance_at_most_one(left: str, right: str) -> bool:
    if left == right:
        return True
    if abs(len(left) - len(right)) > 1:
        return False

    i = 0
    j = 0
    edits = 0
    while i < len(left) and j < len(right):
        if left[i] == right[j]:
            i += 1
            j += 1
            continue
        edits += 1
        if edits > 1:
            return False
        if len(left) > len(right):
            i += 1
        elif len(right) > len(left):
            j += 1
        else:
            i += 1
            j += 1

    if i < len(left) or j < len(right):
        edits += 1
    return edits <= 1


def _compile_patterns(terms: list[dict[str, Any]]) -> list[dict[str, Any]]:
    compiled: list[dict[str, Any]] = []
    for item in terms:
        term = str(item["term"])
        severity = int(item["severity"])
        compiled.append(
            {
                "term": term,
                "severity": severity,
                "pattern": re.compile(rf"\b{re.escape(term)}\b", flags=re.IGNORECASE),
            }
        )
    return compiled


def _collect_exact_matches(normalized_text: str, index_map: list[int]) -> tuple[list[dict[str, Any]], set[tuple[int, int, str]]]:
    matches: list[dict[str, Any]] = []
    seen: set[tuple[int, int, str]] = set()
    for item in _FLAG_PATTERNS:
        for hit in item["pattern"].finditer(normalized_text):
            start_n = hit.start()
            end_n = hit.end()
            if start_n >= len(index_map) or end_n - 1 >= len(index_map):
                continue
            start_o = index_map[start_n]
            end_o = index_map[end_n - 1] + 1
            key = (start_o, end_o, item["term"])
            if key in seen:
                continue
            seen.add(key)
            matches.append(
                {
                    "term": item["term"],
                    "start": start_o,
                    "end": end_o,
                    "severity": item["severity"],
                }
            )
    return matches, seen


def _collect_fuzzy_matches(
    normalized_text: str, index_map: list[int], seen: set[tuple[int, int, str]]
) -> list[dict[str, Any]]:
    if not normalized_text:
        return []

    fuzzy_matches: list[dict[str, Any]] = []
    words = list(_WORD_RE.finditer(normalized_text))
    for item in _HIGH_SEVERITY_SINGLE_WORD_TERMS:
        target = str(item["term"])
        if len(target) < 5:
            continue
        exact_pattern = item["pattern"]
        if exact_pattern.search(normalized_text):
            continue

        for word_match in words:
            candidate = word_match.group(0)
            if not _edit_distance_at_most_one(candidate, target):
                continue
            start_n = word_match.start()
            end_n = word_match.end()
            if start_n >= len(index_map) or end_n - 1 >= len(index_map):
                continue
            start_o = index_map[start_n]
            end_o = index_map[end_n - 1] + 1
            key = (start_o, end_o, target)
            if key in seen:
                continue
            seen.add(key)
            fuzzy_matches.append(
                {
                    "term": target,
                    "start": start_o,
                    "end": end_o,
                    "severity": int(item["severity"]),
                }
            )
    return fuzzy_matches


def classify_text(text: str) -> dict[str, Any]:
    transcript = (text or "").strip()
    if not transcript:
        return {
            "transcript": transcript,
            "flagged": False,
            "label": "ok",
            "score_0_1": 0.0,
            "matches": [],
            "score": 0.0,
            "severity": 0,
        }

    normalized_text, index_map = normalize_text(transcript)
    matches, seen = _collect_exact_matches(normalized_text, index_map)

    # Optional typo-tolerant fallback for severe single-word terms.
    matches.extend(_collect_fuzzy_matches(normalized_text, index_map, seen))
    matches.sort(key=lambda item: (item["start"], item["end"]))

    total_severity = sum(int(item.get("severity", 0)) for item in matches)
    score = min(1.0, total_severity / 10.0)
    flagged = score > 0.0
    label = "flag" if flagged else "ok"

    if matches:
        LOGGER.info("Flag matches found count=%s terms=%s", len(matches), [m["term"] for m in matches])

    return {
        "transcript": transcript,
        "flagged": flagged,
        "label": label,
        "score_0_1": score,
        "matches": matches,
        # Backward-compatible fields consumed by existing UI code.
        "score": score,
        "severity": int(round(score * 100)),
    }


def flag_terms_status() -> dict[str, Any]:
    return {"flag_terms_loaded": _FLAG_TERMS_LOADED, "flag_terms_count": len(_FLAG_TERMS)}


_FLAG_TERMS_PATH = Path(__file__).resolve().parent / "flag_terms.json"
try:
    _FLAG_TERMS = load_flag_terms(_FLAG_TERMS_PATH)
    _FLAG_TERMS_LOADED = True
except Exception as exc:  # pragma: no cover
    LOGGER.exception("Failed to load flag terms: %s", exc)
    _FLAG_TERMS = []
    _FLAG_TERMS_LOADED = False

_FLAG_PATTERNS = _compile_patterns(_FLAG_TERMS)
_HIGH_SEVERITY_SINGLE_WORD_TERMS = [
    entry for entry in _FLAG_PATTERNS if " " not in str(entry["term"]) and int(entry["severity"]) >= 3
]
