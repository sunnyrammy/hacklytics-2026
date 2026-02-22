import logging
import re
from pathlib import Path
from typing import Any

from django.conf import settings

from .lexicon_loader import load_lexicon

LOGGER = logging.getLogger(__name__)

_APOSTROPHE_VARIANTS = {"’", "‘", "`", "´"}
_TOKEN_CHAR_RE = re.compile(r"[a-z0-9']")


def normalize_for_matching(text: str) -> tuple[str, list[int]]:
    normalized_chars: list[str] = []
    index_map: list[int] = []
    pending_space = False
    wrote_token = False

    for idx, char in enumerate(text):
        lowered = char.lower()
        if lowered in _APOSTROPHE_VARIANTS:
            lowered = "'"

        if _TOKEN_CHAR_RE.fullmatch(lowered):
            if pending_space and wrote_token:
                normalized_chars.append(" ")
                index_map.append(idx)
                pending_space = False
            normalized_chars.append(lowered)
            index_map.append(idx)
            wrote_token = True
            continue

        if lowered.isspace() or not _TOKEN_CHAR_RE.fullmatch(lowered):
            if wrote_token:
                pending_space = True

    normalized_text = "".join(normalized_chars).strip()
    if not normalized_text:
        return "", []
    return normalized_text, index_map[: len(normalized_text)]


def _compile_pattern(normalized_term: str) -> re.Pattern[str]:
    escaped = re.escape(normalized_term)
    escaped = escaped.replace(r"\ ", r"\s+")
    return re.compile(rf"\b{escaped}\b", flags=re.IGNORECASE)


def _build_matchers(lexicon: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    phrase_matchers: list[dict[str, Any]] = []
    word_matchers: list[dict[str, Any]] = []

    for entry in lexicon:
        normalized_term, _ = normalize_for_matching(str(entry["term"]))
        if not normalized_term:
            continue

        matcher = {
            "category": str(entry["category"]),
            "severity": int(entry["severity"]),
            "pattern": _compile_pattern(normalized_term),
            "normalized_term": normalized_term,
        }

        if str(entry["type"]) == "phrase":
            phrase_matchers.append(matcher)
        else:
            word_matchers.append(matcher)

    return phrase_matchers, word_matchers


def _spans_overlap(left: tuple[int, int], right: tuple[int, int]) -> bool:
    return not (left[1] <= right[0] or right[1] <= left[0])


def _add_matches(
    matchers: list[dict[str, Any]],
    normalized_text: str,
    index_map: list[int],
    occupied_spans: list[tuple[int, int]],
    matches: list[dict[str, Any]],
    category_totals: dict[str, int],
) -> None:
    for matcher in matchers:
        for hit in matcher["pattern"].finditer(normalized_text):
            norm_span = (hit.start(), hit.end())
            if any(_spans_overlap(norm_span, taken) for taken in occupied_spans):
                continue
            if norm_span[0] >= len(index_map) or norm_span[1] - 1 >= len(index_map):
                continue

            start = index_map[norm_span[0]]
            end = index_map[norm_span[1] - 1] + 1
            category = str(matcher["category"])
            severity = int(matcher["severity"])

            matches.append(
                {
                    "category": category,
                    "severity": severity,
                    "start": start,
                    "end": end,
                    "redacted": True,
                }
            )
            occupied_spans.append(norm_span)
            category_totals[category] = category_totals.get(category, 0) + severity


def classify_text(text: str) -> dict[str, Any]:
    transcript = (text or "").strip()
    if not transcript:
        return {
            "transcript": "",
            "label": "ok",
            "flagged": False,
            "score_0_1": 0.0,
            "category_scores": {},
            "matches": [],
            # Backward-compatible alias for older UI code.
            "score": 0.0,
        }

    normalized_text, index_map = normalize_for_matching(transcript)
    occupied_spans: list[tuple[int, int]] = []
    matches: list[dict[str, Any]] = []
    category_totals: dict[str, int] = {}

    # Match phrases before words to keep higher-context detections.
    _add_matches(_PHRASE_MATCHERS, normalized_text, index_map, occupied_spans, matches, category_totals)
    _add_matches(_WORD_MATCHERS, normalized_text, index_map, occupied_spans, matches, category_totals)
    matches.sort(key=lambda item: (item["start"], item["end"]))

    total = sum(match["severity"] for match in matches)
    score_0_1 = min(1.0, float(total) / 10.0)
    category_scores = {category: min(1.0, value / 6.0) for category, value in category_totals.items()}
    flagged = score_0_1 > 0.0
    label = "flag" if flagged else "ok"

    if matches:
        LOGGER.info(
            "Flagged transcript segment match_count=%s categories=%s",
            len(matches),
            sorted(category_totals.keys()),
        )

    return {
        "transcript": transcript,
        "label": label,
        "flagged": flagged,
        "score_0_1": score_0_1,
        "category_scores": category_scores,
        "matches": matches,
        # Backward-compatible alias for older UI code.
        "score": score_0_1,
    }


def flag_terms_status() -> dict[str, Any]:
    return {
        "flag_terms_loaded": _FLAG_TERMS_LOADED,
        "flag_terms_count": len(_LEXICON),
        "flag_terms_path": str(_LEXICON_PATH),
        "flag_terms_path_exists": _LEXICON_PATH.exists(),
        "flag_terms_parse_ok": _FLAG_TERMS_PARSE_OK,
    }


def _default_lexicon_path() -> Path:
    return Path(__file__).resolve().parent / "sample_flag_terms.json"


def _resolve_lexicon_path() -> Path:
    configured = str(getattr(settings, "FLAG_TERMS_PATH", "") or "").strip()
    if configured:
        return Path(configured).expanduser().resolve()
    return _default_lexicon_path()


_LEXICON_PATH = _resolve_lexicon_path()
try:
    _LEXICON = load_lexicon(_LEXICON_PATH)
    _FLAG_TERMS_LOADED = True
    _FLAG_TERMS_PARSE_OK = True
except FileNotFoundError:
    LOGGER.warning("Flag lexicon file missing at configured path.")
    _LEXICON = []
    _FLAG_TERMS_LOADED = False
    _FLAG_TERMS_PARSE_OK = False
except Exception:
    LOGGER.exception("Failed loading flag lexicon metadata.")
    _LEXICON = []
    _FLAG_TERMS_LOADED = False
    _FLAG_TERMS_PARSE_OK = False

_PHRASE_MATCHERS, _WORD_MATCHERS = _build_matchers(_LEXICON)
