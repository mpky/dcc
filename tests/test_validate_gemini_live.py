from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from scripts.validate_gemini_live import resolve_docs


class ResolveDocsTests(unittest.TestCase):
    def test_resolve_docs_returns_existing_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "doc.txt"
            path.write_text("sample", encoding="utf-8")

            resolved = resolve_docs([path])

            self.assertEqual(resolved, [path])

    def test_resolve_docs_raises_clear_error_for_missing_paths(self) -> None:
        missing = Path("/tmp/does-not-exist-for-gemini-validation.txt")

        with self.assertRaises(SystemExit) as ctx:
            resolve_docs([missing])

        message = str(ctx.exception)
        self.assertIn("Validation sample documents are missing.", message)
        self.assertIn("Run the ingestion pipeline first", message)
        self.assertIn(str(missing), message)


if __name__ == "__main__":
    unittest.main()
