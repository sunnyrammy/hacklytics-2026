from django.conf import settings

from .classifier import classify_text as _lexicon_classify_text
from .classifier import flag_terms_status


def classify_text(text: str) -> dict:
    # Keep a provider switch for future remote-model reintroduction.
    provider = str(getattr(settings, "FLAGGING_PROVIDER", "lexicon") or "lexicon").lower()
    if provider == "databricks":
        # Databricks runtime path is intentionally disabled in this build.
        return _lexicon_classify_text(text)
    return _lexicon_classify_text(text)


__all__ = ["classify_text", "flag_terms_status"]
