import json
import os
import threading
import time
from pathlib import Path
from typing import Any

from asgiref.sync import sync_to_async
from channels.generic.websocket import AsyncWebsocketConsumer

from .databricks_client import DatabricksAPIError, DatabricksClient, read_endpoint_config

try:
    from vosk import KaldiRecognizer, Model
except ImportError:  # pragma: no cover
    KaldiRecognizer = None
    Model = None


DEFAULT_MODEL_PATH = "models/vosk-model-small-en-us-0.15"
_MODEL_LOCK = threading.Lock()
_VOSK_MODEL = None


def _resolve_model_path() -> Path:
    configured = os.getenv("VOSK_MODEL_PATH", DEFAULT_MODEL_PATH)
    base_dir = Path(__file__).resolve().parents[2]
    candidates = [
        Path(configured),
        base_dir / configured,
        base_dir / DEFAULT_MODEL_PATH,
    ]

    for candidate in candidates:
        if candidate.exists() and candidate.is_dir():
            return candidate

    raise FileNotFoundError(
        f"Vosk model directory was not found. Checked: {', '.join(str(p) for p in candidates)}"
    )


def _get_vosk_model():
    global _VOSK_MODEL

    if Model is None:
        raise RuntimeError("Vosk is not installed. Add 'vosk' to your dependencies.")

    with _MODEL_LOCK:
        if _VOSK_MODEL is None:
            _VOSK_MODEL = Model(str(_resolve_model_path()))
    return _VOSK_MODEL


def _extract_numeric_score(payload: Any) -> float | None:
    if isinstance(payload, (int, float)):
        return float(payload)

    if isinstance(payload, dict):
        for key in ("toxicity", "score", "probability", "toxic", "prediction"):
            value = payload.get(key)
            if isinstance(value, (int, float)):
                return float(value)

        for key in ("predictions", "outputs", "result", "data"):
            if key in payload:
                score = _extract_numeric_score(payload[key])
                if score is not None:
                    return score

        for value in payload.values():
            score = _extract_numeric_score(value)
            if score is not None:
                return score

    if isinstance(payload, list):
        for item in payload:
            score = _extract_numeric_score(item)
            if score is not None:
                return score

    return None


class FlagAudioConsumer(AsyncWebsocketConsumer):
    async def connect(self):
        self.recognizer = None
        self.sample_rate = 16000
        self.transcript_segments: list[str] = []
        self.last_score_time = 0.0
        self.last_scored_text = ""
        self.score_every_seconds = float(os.getenv("SCORE_EVERY_SECONDS", "1.0"))
        self.toxicity_threshold = float(os.getenv("TOXICITY_THRESHOLD", "0.7"))
        self.input_column = os.getenv("DATABRICKS_INPUT_COLUMN", "text")
        _, self.endpoint_name = read_endpoint_config()

        await self.accept()
        await self._send_json({
            "type": "connected",
            "message": "Send {'type':'start','sample_rate':16000} then stream PCM16 binary audio frames.",
        })

    async def disconnect(self, close_code):
        self.recognizer = None

    async def receive(self, text_data=None, bytes_data=None):
        if text_data is not None:
            await self._handle_text_message(text_data)
            return

        if bytes_data is not None:
            await self._handle_audio_bytes(bytes_data)

    async def _handle_text_message(self, text_data: str):
        try:
            payload = json.loads(text_data)
        except json.JSONDecodeError:
            await self._send_error("Invalid JSON message.")
            return

        msg_type = payload.get("type")
        if msg_type == "start":
            await self._handle_start(payload)
            return
        if msg_type == "stop":
            await self._handle_stop()
            return

        await self._send_error("Unsupported message type. Use 'start' or 'stop'.")

    async def _handle_start(self, payload: dict[str, Any]):
        sample_rate = payload.get("sample_rate", 16000)
        if not isinstance(sample_rate, int) or sample_rate <= 0:
            await self._send_error("sample_rate must be a positive integer.")
            return

        try:
            model = _get_vosk_model()
        except Exception as exc:
            await self._send_error(str(exc), close=True)
            return

        if KaldiRecognizer is None:
            await self._send_error("Vosk recognizer is unavailable.", close=True)
            return

        self.sample_rate = sample_rate
        self.recognizer = KaldiRecognizer(model, sample_rate)
        self.transcript_segments = []
        self.last_score_time = time.monotonic()
        self.last_scored_text = ""

        await self._send_json({"type": "started", "sample_rate": sample_rate})

    async def _handle_audio_bytes(self, bytes_data: bytes):
        if self.recognizer is None:
            await self._send_error("Send a start message before audio frames.")
            return

        accepted = self.recognizer.AcceptWaveform(bytes_data)
        if accepted:
            result = json.loads(self.recognizer.Result())
            text = (result.get("text") or "").strip()
            if text:
                self.transcript_segments.append(text)
                await self._send_json({"type": "segment", "text": text})
        else:
            partial_payload = json.loads(self.recognizer.PartialResult())
            partial = (partial_payload.get("partial") or "").strip()
            if partial:
                await self._send_json({"type": "partial", "text": partial})

        await self._maybe_score(final=False)

    async def _handle_stop(self):
        if self.recognizer is None:
            await self._send_error("No active stream. Send start first.", close=True)
            return

        final_payload = json.loads(self.recognizer.FinalResult())
        final_text = (final_payload.get("text") or "").strip()
        if final_text:
            self.transcript_segments.append(final_text)
            await self._send_json({"type": "segment", "text": final_text})

        await self._maybe_score(final=True, force=True)
        final_transcript = " ".join(self.transcript_segments).strip()
        await self._send_json({"type": "final", "transcript": final_transcript})
        await self.close()

    async def _maybe_score(self, final: bool, force: bool = False):
        transcript = " ".join(self.transcript_segments).strip()
        if not transcript:
            return

        now = time.monotonic()
        should_score = force or (now - self.last_score_time >= self.score_every_seconds)
        if not should_score:
            return
        if not force and transcript == self.last_scored_text:
            return

        if not self.endpoint_name:
            await self._send_error("DATABRICKS endpoint is not configured.")
            return

        try:
            response = await sync_to_async(self._score_transcript_sync, thread_sensitive=False)(
                transcript
            )
        except Exception as exc:
            await self._send_json(
                {
                    "type": "score_error",
                    "error": str(exc),
                    "final": final,
                }
            )
            self.last_score_time = now
            return

        score = _extract_numeric_score(response)
        flagged = bool(score is not None and score >= self.toxicity_threshold)
        await self._send_json(
            {
                "type": "score",
                "score": score,
                "flagged": flagged,
                "threshold": self.toxicity_threshold,
                "final": final,
                "response": response,
            }
        )
        self.last_score_time = now
        self.last_scored_text = transcript

    def _score_transcript_sync(self, transcript: str) -> dict[str, Any]:
        payload = {"dataframe_records": [{self.input_column: transcript}]}
        client = DatabricksClient()
        try:
            return client.query_serving_endpoint(self.endpoint_name, payload)
        except DatabricksAPIError:
            raise

    async def _send_error(self, message: str, close: bool = False):
        await self._send_json({"type": "error", "error": message})
        if close:
            await self.close()

    async def _send_json(self, payload: dict[str, Any]):
        await self.send(text_data=json.dumps(payload))
