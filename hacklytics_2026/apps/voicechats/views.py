import json
import logging
import threading
import time
import uuid

from django.conf import settings
from django.http import HttpRequest, HttpResponse, JsonResponse
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_GET, require_http_methods

from .databricks.client import call_databricks_inference, validate_databricks_endpoint
from .stt.vosk_engine import accept_audio, create_recognizer, load_model

LOGGER = logging.getLogger(__name__)
_STREAM_LOCK = threading.Lock()
_STREAM_TTL_SECONDS = 300
_STREAMS: dict[str, dict[str, object]] = {}


def index(request):
    return render(request, "voicechat/index.html")


def _cleanup_streams() -> None:
    now = time.time()
    expired = []
    for stream_id, state in _STREAMS.items():
        if now - float(state.get("updated_at", 0.0)) > _STREAM_TTL_SECONDS:
            expired.append(stream_id)
    for stream_id in expired:
        _STREAMS.pop(stream_id, None)


@require_GET
def health(request: HttpRequest) -> JsonResponse:
    vosk_loaded = False
    vosk_error = None
    try:
        load_model(str(getattr(settings, "VOSK_MODEL_PATH", "")))
        vosk_loaded = True
    except Exception as exc:
        vosk_error = str(exc)

    databricks_ok, databricks_details = validate_databricks_endpoint(settings)
    details = {
        "vosk_error": vosk_error,
        "databricks": databricks_details,
    }
    return JsonResponse(
        {
            "vosk_model_loaded": vosk_loaded,
            "databricks_reachable": databricks_ok,
            "details": details,
        }
    )


@csrf_exempt
@require_http_methods(["POST"])
def transcribe_chunk(request: HttpRequest) -> JsonResponse:
    stream_id = (request.GET.get("stream_id") or "").strip() or str(uuid.uuid4())
    sample_rate_raw = request.GET.get("sample_rate", "16000")
    try:
        sample_rate = int(sample_rate_raw)
    except ValueError:
        return JsonResponse({"error": "sample_rate must be an integer."}, status=400)
    chunk = request.body
    if not chunk:
        return JsonResponse({"error": "Audio chunk body is required."}, status=400)

    with _STREAM_LOCK:
        _cleanup_streams()
        state = _STREAMS.get(stream_id)
        if state is None:
            try:
                model = load_model(str(getattr(settings, "VOSK_MODEL_PATH", "")))
                recognizer = create_recognizer(model, sample_rate)
                state = {
                    "recognizer": recognizer,
                    "segments": [],
                    "sample_rate": sample_rate,
                    "segment_counter": 0,
                }
                _STREAMS[stream_id] = state
            except Exception as exc:
                LOGGER.exception("Failed to initialize stream %s: %s", stream_id, exc)
                return JsonResponse({"error": str(exc)}, status=500)

        state["updated_at"] = time.time()
        recognizer = state["recognizer"]
        if not hasattr(recognizer, "AcceptWaveform"):
            return JsonResponse({"error": "Invalid stream recognizer state."}, status=500)
        result = accept_audio(recognizer, chunk)
        partial = result.get("partial", "")
        final = result.get("final", "")
        score_payload = None
        segment_id = None
        if final:
            state["segment_counter"] = int(state.get("segment_counter", 0)) + 1
            segment_id = str(state["segment_counter"])
            segments = state["segments"]
            if isinstance(segments, list):
                segments.append(final)
            try:
                response = call_databricks_inference(final, settings)
                score_payload = {
                    "segment_id": segment_id,
                    "text": final,
                    "label": response.get("label"),
                    "score": response.get("score"),
                    "severity": response.get("severity"),
                    "threshold_used": response.get("threshold_used"),
                    "flagged": bool(response.get("flagged")),
                    "endpoint_id": response.get("endpoint_id"),
                    "raw": response.get("raw"),
                }
            except Exception as exc:
                score_payload = {"segment_id": segment_id, "error": str(exc), "text": final}

    return JsonResponse(
        {
            "stream_id": stream_id,
            "partial": partial,
            "final": final,
            "segment_id": segment_id,
            "score": score_payload,
        }
    )


@csrf_exempt
@require_http_methods(["POST"])
def finalize_stream(request: HttpRequest) -> JsonResponse:
    try:
        payload = json.loads((request.body or b"{}").decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        payload = {}
    stream_id = str(payload.get("stream_id", "")).strip()
    if not stream_id:
        return JsonResponse({"error": "stream_id is required."}, status=400)

    with _STREAM_LOCK:
        state = _STREAMS.pop(stream_id, None)
    if state is None:
        return JsonResponse({"error": "Unknown stream_id."}, status=404)

    final_text = ""
    try:
        recognizer = state["recognizer"]
        if not hasattr(recognizer, "FinalResult"):
            return JsonResponse({"error": "Invalid stream recognizer state."}, status=500)
        final_payload = json.loads(recognizer.FinalResult())
        tail = (final_payload.get("text") or "").strip()
        segments = state.get("segments", [])
        if not isinstance(segments, list):
            segments = []
        if tail:
            segments.append(tail)
        final_text = " ".join(segments).strip()
    except Exception as exc:
        return JsonResponse({"error": f"Failed to finalize stream: {exc}"}, status=500)

    return JsonResponse({"stream_id": stream_id, "transcript": final_text})
