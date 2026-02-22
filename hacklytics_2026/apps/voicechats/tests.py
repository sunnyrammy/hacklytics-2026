from django.test import TestCase, override_settings


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
