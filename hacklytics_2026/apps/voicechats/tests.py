from django.test import TestCase, override_settings

from .flagging.classifier import classify_text, flag_terms_status


class VoiceChatApiTests(TestCase):
    @override_settings(VOSK_MODEL_PATH="/tmp/does-not-exist")
    def test_health_endpoint_returns_expected_shape(self):
        response = self.client.get("/api/voicechat/health/")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertIn("vosk_model_loaded", payload)
        self.assertIn("flag_terms_loaded", payload)
        self.assertIn("flag_terms_count", payload)

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


class LocalFlaggingTests(TestCase):
    def test_classifier_flags_known_term(self):
        result = classify_text("You are useless trash.")
        self.assertTrue(result["flagged"])
        self.assertEqual(result["label"], "flag")
        self.assertGreater(result["score_0_1"], 0.0)
        self.assertGreater(len(result["matches"]), 0)

    def test_classifier_clean_text_is_ok(self):
        result = classify_text("Team regroup and rotate left.")
        self.assertFalse(result["flagged"])
        self.assertEqual(result["label"], "ok")
        self.assertEqual(result["score_0_1"], 0.0)
        self.assertEqual(result["matches"], [])

    def test_word_boundary_safe(self):
        result = classify_text("This class is hard.")
        self.assertFalse(any(match["term"] == "ass" for match in result["matches"]))

    def test_multi_word_phrase_match(self):
        result = classify_text("Please uninstall the game now.")
        self.assertTrue(any(match["term"] == "uninstall the game" for match in result["matches"]))

    def test_flag_terms_status(self):
        status = flag_terms_status()
        self.assertIn("flag_terms_loaded", status)
        self.assertIn("flag_terms_count", status)
        self.assertGreaterEqual(int(status["flag_terms_count"]), 1)
