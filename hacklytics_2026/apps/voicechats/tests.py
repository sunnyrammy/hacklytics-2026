from django.conf import settings
from django.test import TestCase, override_settings

from .databricks.client import normalize_databricks_output


class VoiceChatApiTests(TestCase):
    @override_settings(VOSK_MODEL_PATH="/tmp/does-not-exist")
    def test_health_endpoint_returns_expected_shape(self):
        response = self.client.get("/api/voicechat/health/")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertIn("vosk_model_loaded", payload)
        self.assertIn("databricks_reachable", payload)
        self.assertIn("details", payload)
        self.assertIn("databricks", payload["details"])

    def test_transcribe_requires_audio_body(self):
        response = self.client.post(
            "/api/voicechat/transcribe/?stream_id=test-stream",
            data=b"",
            content_type="application/octet-stream",
        )
        self.assertEqual(response.status_code, 400)
        self.assertIn("error", response.json())

    def test_finalize_requires_stream_id(self):
        response = self.client.post(
            "/api/voicechat/finalize/",
            data="{}",
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 400)
        self.assertIn("error", response.json())


class DatabricksNormalizationTests(TestCase):
    @override_settings(DATABRICKS_SCORE_TYPE="percent_0_100")
    def test_percent_score_normalizes_to_zero_one(self):
        payload = {"score": 82}
        normalized = normalize_databricks_output(payload, settings_obj=settings, endpoint_id="ep")
        self.assertAlmostEqual(normalized["score"], 0.82, places=3)
        self.assertEqual(normalized["severity"], 82)

    @override_settings(DATABRICKS_SCORE_TYPE="none", DATABRICKS_LABEL_FIELD="label", DATABRICKS_POSITIVE_CLASS="flag")
    def test_unknown_score_uses_label_for_flag(self):
        payload = {"label": "flag", "score": 9999}
        normalized = normalize_databricks_output(payload, settings_obj=settings, endpoint_id="ep")
        self.assertIsNone(normalized["score"])
        self.assertTrue(normalized["flagged"])
