from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from tempfile import NamedTemporaryFile

import pypdfium2 as pdfium
import pytesseract
from PIL import Image
from pypdf import PdfReader

try:
    from ocrmac import ocrmac
except ImportError:  # pragma: no cover - environment-dependent
    ocrmac = None


MIN_PAGE_TEXT_ALNUM = 40


@dataclass(frozen=True)
class ExtractionResult:
    text: str
    method: str
    page_count: int


class PDFTextExtractor:
    def extract(self, pdf_path: Path) -> ExtractionResult:
        reader = PdfReader(str(pdf_path))
        page_count = len(reader.pages)
        pdf = None
        page_texts: list[str] = []
        methods_used: set[str] = set()

        for page_index, page in enumerate(reader.pages):
            page_text = self._normalize(page.extract_text() or "")
            if self._is_sufficient_text(page_text):
                page_texts.append(page_text)
                methods_used.add("pypdf")
                continue

            if pdf is None:
                pdf = pdfium.PdfDocument(str(pdf_path))
            ocr_text, ocr_method = self._ocr_pdf_page(pdf, page_index)
            if ocr_text:
                page_texts.append(ocr_text)
                methods_used.add(ocr_method)
            else:
                page_texts.append(page_text)

        extracted_text = "\n\n".join(text for text in page_texts if text).strip()
        method = "+".join(sorted(methods_used)) if methods_used else "none"
        return ExtractionResult(text=extracted_text, method=method, page_count=page_count)

    def _ocr_pdf_page(self, pdf: pdfium.PdfDocument, page_index: int) -> tuple[str, str]:
        page = pdf[page_index]
        image = page.render(scale=2).to_pil()
        try:
            text = self._ocr_with_ocrmac(image)
            if self._is_sufficient_text(text):
                return text, "ocrmac"
            text = self._ocr_with_tesseract(image)
            if text:
                return text, "pytesseract"
            return "", "none"
        finally:
            image.close()

    def _ocr_with_ocrmac(self, image: Image.Image) -> str:
        if ocrmac is None:
            return ""

        with NamedTemporaryFile(suffix=".png", delete=True) as tmp:
            image.save(tmp.name)
            annotations = ocrmac.OCR(tmp.name).recognize()
        texts = [item[0].strip() for item in annotations if isinstance(item, tuple) and item and item[0].strip()]
        return self._normalize("\n".join(texts))

    def _ocr_with_tesseract(self, image: Image.Image) -> str:
        grayscale = image.convert("L")
        try:
            text = pytesseract.image_to_string(grayscale)
        finally:
            grayscale.close()
        return self._normalize(text)

    def _is_sufficient_text(self, text: str) -> bool:
        return len(re.findall(r"[A-Za-z0-9]", text)) >= MIN_PAGE_TEXT_ALNUM

    def _normalize(self, text: str) -> str:
        lines = [re.sub(r"[ \t]+", " ", line).strip() for line in text.splitlines()]
        return "\n".join(line for line in lines if line)
