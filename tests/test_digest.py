from __future__ import annotations

import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path

from data_center_digest.db import connect, list_digest_entries
from data_center_digest.digest import render_markdown_digest


class DigestQueryTests(unittest.TestCase):
    def test_list_digest_entries_returns_latest_summary_per_document(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "app.db"
            conn = connect(db_path)
            conn.execute(
                "INSERT INTO sources (source_id, name, jurisdiction, kind, url) VALUES (?, ?, ?, ?, ?)",
                ("source-1", "Source", "Loudoun County, VA", "kind", "https://example.invalid/source"),
            )
            conn.execute(
                "INSERT INTO items (item_id, source_id, title, url, first_seen_at, last_seen_at) VALUES (?, ?, ?, ?, ?, ?)",
                ("item-1", "source-1", "03-17-26 Business Meeting", "https://example.invalid/meeting", "2026-03-17T12:00:00+00:00", "2026-03-17T12:00:00+00:00"),
            )
            conn.execute(
                "INSERT INTO documents (document_id, item_id, title, url, local_path, sha256, first_seen_at, downloaded_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                ("doc-1", "item-1", "Concorde Industrial Park.pdf", "https://example.invalid/doc-1.pdf", "/tmp/doc-1.pdf", "hash", "2026-03-17T12:00:00+00:00", "2026-03-17T12:00:00+00:00"),
            )
            conn.execute(
                "INSERT INTO document_relevance (document_id, is_relevant, score, categories_json, matched_terms_json, matches_json, rationale, analyzed_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                ("doc-1", 1, 28, "[]", "[]", "[]", "Matched data center", "2026-03-17T12:00:00+00:00"),
            )
            conn.execute(
                """
                INSERT INTO document_summaries (
                    document_id, backend, model, summary_path, summary, why_it_matters,
                    topic_tags_json, confidence, next_watch, raw_response, summarized_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "doc-1",
                    "ollama",
                    "gemma3n:e2b",
                    "/tmp/old.json",
                    "Old summary",
                    "Old why it matters",
                    '["old"]',
                    "low",
                    "Old next watch",
                    "{}",
                    "2026-03-17T12:00:00+00:00",
                ),
            )
            conn.execute(
                """
                INSERT INTO document_summaries (
                    document_id, backend, model, summary_path, summary, why_it_matters,
                    topic_tags_json, confidence, next_watch, raw_response, summarized_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "doc-1",
                    "gemini",
                    "gemini-2.5-flash-lite",
                    "/tmp/new.json",
                    "New summary",
                    "New why it matters",
                    '["data center", "zoning"]',
                    "high",
                    "Watch the next board vote.",
                    "{}",
                    "2026-03-18T12:00:00+00:00",
                ),
            )
            conn.commit()

            rows = list_digest_entries(conn)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["summary"], "New summary")
            self.assertEqual(rows[0]["backend"], "gemini")
            self.assertEqual(rows[0]["model"], "gemini-2.5-flash-lite")

            filtered_rows = list_digest_entries(conn, backend="ollama", model="gemma3n:e2b")
            self.assertEqual(len(filtered_rows), 1)
            self.assertEqual(filtered_rows[0]["summary"], "Old summary")
            conn.close()


class DigestRenderTests(unittest.TestCase):
    def test_render_markdown_digest_groups_entries_by_meeting(self) -> None:
        entries = [
            {
                "meeting_title": "03-17-26 Business Meeting",
                "meeting_url": "https://example.invalid/meeting-1",
                "document_title": "Concorde Industrial Park.pdf",
                "document_url": "https://example.invalid/doc-1.pdf",
                "summary": "A rezoning and special-exception request for a data center project.",
                "why_it_matters": "This directly affects data center siting and utility approvals.",
                "topic_tags": ["data center", "zoning"],
                "confidence": "high",
                "next_watch": "Watch the next board vote.",
                "jurisdiction": "Loudoun County, VA",
                "source_name": "Loudoun BOS",
                "backend": "gemini",
                "model": "gemini-2.5-flash-lite",
                "score": 28,
            },
            {
                "meeting_title": "03-11-26 Public Hearing",
                "meeting_url": "https://example.invalid/meeting-2",
                "document_title": "West Belmont.pdf",
                "document_url": "https://example.invalid/doc-2.pdf",
                "summary": "A zoning map amendment request tied to a land-use change.",
                "why_it_matters": "This may affect future data center-adjacent land use.",
                "topic_tags": ["zoning", "land use"],
                "confidence": "medium",
                "next_watch": "Watch the next public hearing.",
                "jurisdiction": "Loudoun County, VA",
                "source_name": "Loudoun BOS",
                "backend": "gemini",
                "model": "gemini-2.5-flash-lite",
                "score": 5,
            },
        ]

        markdown = render_markdown_digest(
            entries=entries,
            generated_at=datetime(2026, 3, 23, 10, 30, tzinfo=UTC),
            source_label="Loudoun County, VA",
        )

        self.assertIn("# Data Center Legal Digest", markdown)
        self.assertIn("Generated: 2026-03-23 10:30 UTC", markdown)
        self.assertIn("## 03-17-26 Business Meeting", markdown)
        self.assertIn("## 03-11-26 Public Hearing", markdown)
        self.assertIn("[Concorde Industrial Park.pdf](https://example.invalid/doc-1.pdf)", markdown)
        self.assertIn("Why it matters: This directly affects data center siting and utility approvals.", markdown)
        self.assertIn("Tags: `data center`, `zoning`", markdown)


if __name__ == "__main__":
    unittest.main()
