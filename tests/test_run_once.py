from __future__ import annotations

import unittest
from pathlib import Path

from data_center_digest.run_once import summary_path_for


class SummaryPathTests(unittest.TestCase):
    def test_summary_paths_are_unique_for_duplicate_titles(self) -> None:
        base_kwargs = {
            "data_dir": Path("data"),
            "source_id": "loudoun_bos_meeting_documents",
            "meeting_folder_id": "1969511",
            "document_title": "Item 11 LEGI-2024-0002_ Concorde Industrial Park.pdf",
            "backend": "ollama",
            "model": "gemma3:4b-it-qat",
        }

        first_path = summary_path_for(document_id="abc1234567899999", **base_kwargs)
        second_path = summary_path_for(document_id="def9876543219999", **base_kwargs)

        self.assertNotEqual(first_path, second_path)
        self.assertIn("abc123456789", first_path.name)
        self.assertIn("def987654321", second_path.name)


if __name__ == "__main__":
    unittest.main()
