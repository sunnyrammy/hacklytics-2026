import json
import logging
import threading
from pathlib import Path
from typing import Any

try:
    from vosk import KaldiRecognizer, Model
except ImportError:  # pragma: no cover
    KaldiRecognizer = None
    Model = None


LOGGER = logging.getLogger(__name__)
_MODEL_LOCK = threading.Lock()
_MODEL_CACHE: dict[str, Any] = {}


def load_model(model_path: str) -> Any:
    if Model is None:
        raise RuntimeError("Vosk is not installed.")
    if not model_path:
        raise ValueError("VOSK_MODEL_PATH is not configured.")

    resolved = str(Path(model_path).expanduser().resolve())
    with _MODEL_LOCK:
        if resolved in _MODEL_CACHE:
            return _MODEL_CACHE[resolved]
        if not Path(resolved).is_dir():
            raise FileNotFoundError(f"Vosk model path does not exist: {resolved}")
        LOGGER.info("Loading Vosk model from %s", resolved)
        model = Model(resolved)
        _MODEL_CACHE[resolved] = model
        return model


def create_recognizer(model: Any, sample_rate: int) -> Any:
    if KaldiRecognizer is None:
        raise RuntimeError("Vosk recognizer is unavailable.")
    if sample_rate <= 0:
        raise ValueError("sample_rate must be a positive integer.")
    return KaldiRecognizer(model, float(sample_rate))


def accept_audio(recognizer: Any, pcm_bytes: bytes) -> dict[str, Any]:
    if not pcm_bytes:
        return {"partial": "", "final": "", "is_final": False}

    try:
        is_final = bool(recognizer.AcceptWaveform(pcm_bytes))
        if is_final:
            payload = json.loads(recognizer.Result())
            text = (payload.get("text") or "").strip()
            return {"partial": "", "final": text, "is_final": True}
        payload = json.loads(recognizer.PartialResult())
        text = (payload.get("partial") or "").strip()
        return {"partial": text, "final": "", "is_final": False}
    except Exception:
        LOGGER.exception("Failed while processing audio chunk with Vosk")
        raise
