from __future__ import annotations

import unittest
from unittest.mock import patch

from PIL import Image
import pytesseract

from data_center_digest.pdf_text import PDFTextExtractor


class PDFTextExtractorTests(unittest.TestCase):
    def test_returns_empty_string_when_tesseract_binary_is_missing(self) -> None:
        extractor = PDFTextExtractor()
        image = Image.new("RGB", (10, 10), color="white")
        try:
            with patch(
                "data_center_digest.pdf_text.pytesseract.image_to_string",
                side_effect=pytesseract.TesseractNotFoundError(),
            ):
                self.assertEqual(extractor._ocr_with_tesseract(image), "")
        finally:
            image.close()


if __name__ == "__main__":
    unittest.main()
