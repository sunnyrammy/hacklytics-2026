import json
import logging
from typing import Any

from asgiref.sync import sync_to_async
from channels.generic.websocket import AsyncWebsocketConsumer
from django.conf import settings

from .databricks.client import call_databricks_inference
from .stt.vosk_engine import accept_audio, create_recognizer, load_model

LOGGER = logging.getLogger(__name__)


def _extract_numeric_score(payload: Any) -> float | None:
    if isinstance(payload, (int, float)):
        return float(payload)
    if isinstance(payload, dict):
        for key in ("toxicity", "score", "probability", "toxic", "prediction"):
            value = payload.get(key)
            if isinstance(value, (int, float)):
                return float(value)
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


class VoiceChatStreamConsumer(AsyncWebsocketConsumer):
    async def connect(self):
        self.recognizer = None
        self.sample_rate = 16000
        self.final_segments: list[str] = []
        self.toxicity_threshold = float(getattr(settings, "TOXICITY_THRESHOLD", 0.7))
        await self.accept()
        await self._send_json(
            {
                "type": "connected",
                "message": "Send {'type':'start'} then stream PCM16 mono 16kHz chunks as binary frames.",
            }
        )

    async def disconnect(self, close_code):
        LOGGER.info("Voicechat websocket disconnected code=%s", close_code)
        self.recognizer = None

    async def receive(self, text_data: str | None = None, bytes_data: bytes | None = None):
        if text_data is not None:
            await self._handle_text(text_data)
            return
        if bytes_data is not None:
            await self._handle_audio_chunk(bytes_data)

    async def _handle_text(self, text_data: str):
        try:
            payload = json.loads(text_data)
        except json.JSONDecodeError:
            await self._send_error("Invalid JSON payload.")
            return

        message_type = payload.get("type")
        if message_type == "start":
            sample_rate = int(payload.get("sample_rate", 16000))
            await self._start_stream(sample_rate)
            return
        if message_type == "stop":
            await self._stop_stream()
            return
        await self._send_error("Unsupported message type.")

    async def _start_stream(self, sample_rate: int):
        try:
            model_path = str(getattr(settings, "VOSK_MODEL_PATH", "")).strip()
            model = await sync_to_async(load_model, thread_sensitive=True)(model_path)
            self.recognizer = await sync_to_async(create_recognizer, thread_sensitive=True)(model, sample_rate)
            self.final_segments = []
            self.sample_rate = sample_rate
            await self._send_json({"type": "started", "sample_rate": sample_rate})
        except Exception as exc:
            LOGGER.exception("Failed to start voicechat stream: %s", exc)
            await self._send_error(str(exc), close=True)

    async def _handle_audio_chunk(self, chunk: bytes):
        if self.recognizer is None:
            await self._send_error("Stream not started. Send {'type':'start'} first.")
            return

        try:
            result = await sync_to_async(accept_audio, thread_sensitive=True)(self.recognizer, chunk)
        except Exception as exc:
            await self._send_error(f"Failed to process audio chunk: {exc}")
            return

        partial_text = result.get("partial", "")
        final_text = result.get("final", "")
        if partial_text:
            await self._send_json({"type": "partial", "text": partial_text})
        if final_text:
            self.final_segments.append(final_text)
            await self._send_json({"type": "segment", "text": final_text})
            await self._score_and_send(final_text)

    async def _stop_stream(self):
        if self.recognizer is None:
            await self._send_error("Stream not started.")
            return

        try:
            final_payload = await sync_to_async(self.recognizer.FinalResult, thread_sensitive=True)()
            parsed = json.loads(final_payload)
            final_text = (parsed.get("text") or "").strip()
            if final_text:
                self.final_segments.append(final_text)
                await self._send_json({"type": "segment", "text": final_text})
                await self._score_and_send(final_text)
            transcript = " ".join(self.final_segments).strip()
            await self._send_json({"type": "final", "transcript": transcript})
        except Exception as exc:
            LOGGER.exception("Failed during stream stop/finalize: %s", exc)
            await self._send_error(f"Failed to finalize stream: {exc}")
        finally:
            await self.close()

    async def _score_and_send(self, finalized_text: str):
        if not finalized_text.strip():
            return
        try:
            response = await sync_to_async(call_databricks_inference, thread_sensitive=False)(
                finalized_text, settings
            )
            score = _extract_numeric_score(response)
            flagged = bool(score is not None and score >= self.toxicity_threshold)
            await self._send_json(
                {
                    "type": "score",
                    "text": finalized_text,
                    "score": score,
                    "flagged": flagged,
                    "threshold": self.toxicity_threshold,
                    "response": response,
                }
            )
        except Exception as exc:
            LOGGER.warning("Databricks scoring failed for finalized segment: %s", exc)
            await self._send_json({"type": "score_error", "error": str(exc), "text": finalized_text})

    async def _send_error(self, message: str, close: bool = False):
        await self._send_json({"type": "error", "error": message})
        if close:
            await self.close()

    async def _send_json(self, payload: dict[str, Any]):
        await self.send(text_data=json.dumps(payload))
