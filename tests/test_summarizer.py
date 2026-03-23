from __future__ import annotations

import json
import os
import unittest
from unittest.mock import patch

from data_center_digest.summarizer import (
    SummaryRequest,
    Summarizer,
    SummarizerConfig,
    SummarizerError,
    build_summary_prompt,
    _parse_summary_payload,
)


class ParseSummaryPayloadTests(unittest.TestCase):
    def test_parses_canonical_summary_payload(self) -> None:
        payload = json.dumps(
            {
                "summary": "A zoning amendment for a data center project.",
                "why_it_matters": "This is directly relevant to data center siting.",
                "topic_tags": ["data center", "zoning"],
                "confidence": "high",
                "next_watch": "Watch the next board vote.",
            }
        )

        result = _parse_summary_payload(payload)

        self.assertEqual(result["summary"], "A zoning amendment for a data center project.")
        self.assertEqual(result["why_it_matters"], "This is directly relevant to data center siting.")
        self.assertEqual(result["topic_tags"], ["data center", "zoning"])
        self.assertEqual(result["confidence"], "high")
        self.assertEqual(result["next_watch"], "Watch the next board vote.")

    def test_accepts_common_ollama_key_typos(self) -> None:
        payload = json.dumps(
            {
                "summaary": "A rezoning request for a data center and substation.",
                "why_it_matteers": "This is directly relevant to data center approvals.",
                "topic_tags": ["data center", "substation"],
                "confidence": "high",
                "next_watch": "Watch the next public hearing.",
            }
        )

        result = _parse_summary_payload(payload)

        self.assertEqual(result["summary"], "A rezoning request for a data center and substation.")
        self.assertEqual(result["why_it_matters"], "This is directly relevant to data center approvals.")
        self.assertEqual(result["topic_tags"], ["data center", "substation"])
        self.assertEqual(result["confidence"], "high")
        self.assertEqual(result["next_watch"], "Watch the next public hearing.")

    def test_rejects_answer_only_payload(self) -> None:
        payload = json.dumps(
            {
                "answer": "This ordinance vacates a landscape buffer easement for a residential lot.",
            }
        )

        with self.assertRaises(SummarizerError):
            _parse_summary_payload(payload)

    def test_rejects_non_json_payload(self) -> None:
        with self.assertRaises(SummarizerError):
            _parse_summary_payload("not-json")


class BuildSummaryPromptTests(unittest.TestCase):
    def test_prompt_includes_core_guardrails_for_legal_digest_task(self) -> None:
        prompt = build_summary_prompt(
            SummaryRequest(
                title="Concorde Industrial Park",
                text="This is a sample rezoning text.",
                jurisdiction="Loudoun County, VA",
                meeting_title="03-17-26 Business Meeting",
                source_url="https://example.invalid/doc.pdf",
                max_input_chars=50,
            )
        )

        self.assertIn("Use only facts supported by the document text and metadata.", prompt)
        self.assertIn("Do not infer missing facts", prompt)
        self.assertIn("Ignore boilerplate", prompt)
        self.assertIn("OCR", prompt)
        self.assertIn("zoning, land use, utilities, substations, transmission", prompt)
        self.assertIn('"title": "Concorde Industrial Park"', prompt)
        self.assertIn("This is a sample rezoning text.", prompt)

    def test_prompt_trims_document_text_to_max_input_chars(self) -> None:
        prompt = build_summary_prompt(
            SummaryRequest(
                title="Title",
                text="abcdefghijklmnopqrstuvwxyz",
                jurisdiction="Loudoun County, VA",
                max_input_chars=10,
            )
        )

        self.assertIn("abcdefghij", prompt)
        self.assertNotIn("klmnopqrstuvwxyz", prompt)


class GeminiSummarizerTests(unittest.TestCase):
    def test_gemini_summarize_uses_mocked_response(self) -> None:
        summarizer = Summarizer(
            SummarizerConfig(
                backend="gemini",
                model="gemini-2.5-flash-lite",
                endpoint="https://example.invalid/v1beta",
                api_key="test-key",
                request_timeout_seconds=42,
            )
        )
        request = SummaryRequest(
            title="Concorde Industrial Park",
            text="Data center rezoning and substation request.",
            jurisdiction="Loudoun County, VA",
            meeting_title="03-17-26 Business Meeting",
            source_url="https://example.invalid/doc.pdf",
            max_input_chars=100,
        )

        with patch("data_center_digest.summarizer._post_json") as mock_post_json:
            mock_post_json.return_value = {
                "candidates": [
                    {
                        "content": {
                            "parts": [
                                {
                                    "text": json.dumps(
                                        {
                                            "summary": "A zoning request for a data center project.",
                                            "why_it_matters": "This directly affects data center siting.",
                                            "topic_tags": ["data center", "zoning"],
                                            "confidence": "high",
                                            "next_watch": "Watch the next board vote.",
                                        }
                                    )
                                }
                            ]
                        }
                    }
                ]
            }

            result = summarizer.summarize(request)

        self.assertEqual(result.backend, "gemini")
        self.assertEqual(result.model, "gemini-2.5-flash-lite")
        self.assertEqual(result.summary, "A zoning request for a data center project.")
        self.assertEqual(result.why_it_matters, "This directly affects data center siting.")
        self.assertEqual(result.topic_tags, ["data center", "zoning"])
        self.assertEqual(result.confidence, "high")
        self.assertEqual(result.next_watch, "Watch the next board vote.")

        called_url, called_body = mock_post_json.call_args.args[:2]
        self.assertIn("gemini-2.5-flash-lite:generateContent?key=test-key", called_url)
        self.assertEqual(mock_post_json.call_args.kwargs["timeout_seconds"], 42)
        self.assertEqual(called_body["generationConfig"]["responseMimeType"], "application/json")
        self.assertIn("Concorde Industrial Park", called_body["contents"][0]["parts"][0]["text"])

    def test_gemini_summarize_requires_api_key(self) -> None:
        summarizer = Summarizer(
            SummarizerConfig(
                backend="gemini",
                model="gemini-2.5-flash-lite",
                endpoint="https://example.invalid/v1beta",
                api_key=None,
            )
        )

        with self.assertRaises(SummarizerError):
            summarizer.summarize(
                SummaryRequest(
                    title="Title",
                    text="Some text",
                    jurisdiction="Loudoun County, VA",
                )
            )

    def test_gemini_summarize_rejects_unexpected_response_shape(self) -> None:
        summarizer = Summarizer(
            SummarizerConfig(
                backend="gemini",
                model="gemini-2.5-flash-lite",
                endpoint="https://example.invalid/v1beta",
                api_key="test-key",
            )
        )

        with patch("data_center_digest.summarizer._post_json", return_value={"unexpected": True}):
            with self.assertRaises(SummarizerError):
                summarizer.summarize(
                    SummaryRequest(
                        title="Title",
                        text="Some text",
                        jurisdiction="Loudoun County, VA",
                    )
                )

    def test_from_env_builds_gemini_config_without_network(self) -> None:
        original = {
            "SUMMARY_BACKEND": os.environ.get("SUMMARY_BACKEND"),
            "GEMINI_API_KEY": os.environ.get("GEMINI_API_KEY"),
            "GEMINI_MODEL": os.environ.get("GEMINI_MODEL"),
            "SUMMARY_REQUEST_TIMEOUT_SECONDS": os.environ.get("SUMMARY_REQUEST_TIMEOUT_SECONDS"),
        }
        try:
            os.environ["SUMMARY_BACKEND"] = "gemini"
            os.environ["GEMINI_API_KEY"] = "env-key"
            os.environ["GEMINI_MODEL"] = "gemini-test-model"
            os.environ["SUMMARY_REQUEST_TIMEOUT_SECONDS"] = "77"

            summarizer = Summarizer.from_env()

            self.assertEqual(summarizer.config.backend, "gemini")
            self.assertEqual(summarizer.config.model, "gemini-test-model")
            self.assertEqual(summarizer.config.api_key, "env-key")
            self.assertEqual(summarizer.config.request_timeout_seconds, 77)
        finally:
            for key, value in original.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value


if __name__ == "__main__":
    unittest.main()
