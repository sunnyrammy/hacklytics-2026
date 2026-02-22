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
        self.assertIn("flagging_provider", payload)
        self.assertIn("flag_terms_path_exists", payload)
        self.assertIn("flag_terms_parse_ok", payload)

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
        result = classify_text("TERM should trigger a local flag.")
        self.assertTrue(result["flagged"])
        self.assertEqual(result["label"], "flag")
        self.assertGreater(result["score_0_1"], 0.0)
        self.assertGreater(len(result["matches"]), 0)
        self.assertIn("category_scores", result)
        self.assertTrue(all(match.get("redacted") is True for match in result["matches"]))
        self.assertTrue(all("term" not in match for match in result["matches"]))

    def test_classifier_clean_text_is_ok(self):
        result = classify_text("Team regroup and rotate left.")
        self.assertFalse(result["flagged"])
        self.assertEqual(result["label"], "ok")
        self.assertEqual(result["score_0_1"], 0.0)
        self.assertEqual(result["matches"], [])

    def test_word_boundary_safe(self):
        result = classify_text("TERMINAL output should not trigger TERM.")
        # One exact TERM token should match; TERMINAL must not trigger.
        self.assertEqual(len(result["matches"]), 1)

    def test_multi_word_phrase_match(self):
        result = classify_text("This includes TERM PLACEHOLDER PHRASE now.")
        self.assertTrue(result["flagged"])
        self.assertTrue(any(match["category"] == "threat" for match in result["matches"]))

    def test_flag_terms_status(self):
        status = flag_terms_status()
        self.assertIn("flag_terms_loaded", status)
        self.assertIn("flag_terms_count", status)
        self.assertGreaterEqual(int(status["flag_terms_count"]), 1)
        self.assertIn("flag_terms_path_exists", status)
        self.assertIn("flag_terms_parse_ok", status)
