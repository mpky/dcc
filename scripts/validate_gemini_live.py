from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from data_center_digest.summarizer import SummaryRequest, Summarizer, SummarizerError


DEFAULT_DOCS = [
    ROOT / "data" / "text" / "loudoun_bos_meeting_documents" / "1969511" / "Item 11 LEGI-2024-0002_ Concorde Industrial Park.txt",
    ROOT / "data" / "text" / "loudoun_bos_meeting_documents" / "1969511" / "Item 11 LEGI-2024-0002_ Concorde Industrial Park-Supplemental.txt",
    ROOT / "data" / "text" / "loudoun_bos_meeting_documents" / "1969511" / "Item 10 LEGI-2023-0114_ Franklin Park West.txt",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a live Gemini validation pass on the default sample document set.")
    parser.add_argument(
        "--docs",
        nargs="+",
        type=Path,
        default=DEFAULT_DOCS,
        help="Extracted text files to summarize with Gemini.",
    )
    parser.add_argument("--jurisdiction", default="Loudoun County, VA")
    parser.add_argument("--max-input-chars", type=int, default=12000)
    parser.add_argument(
        "--output-path",
        type=Path,
        default=ROOT / "data" / "evals" / "gemini_live_validation.json",
        help="Where to store the Gemini validation results as JSON.",
    )
    return parser.parse_args()


def resolve_docs(requested_docs: list[Path]) -> list[Path]:
    missing = [path for path in requested_docs if not path.exists()]
    if not missing:
        return requested_docs

    missing_list = "\n".join(f"- {path}" for path in missing)
    raise SystemExit(
        "Validation sample documents are missing.\n"
        "Run the ingestion pipeline first to populate `data/text/...`, or pass explicit `--docs` paths.\n"
        f"Missing paths:\n{missing_list}"
    )


def main() -> None:
    args = parse_args()
    summarizer = Summarizer.from_env()
    if summarizer.config.backend != "gemini":
        raise SystemExit("Set SUMMARY_BACKEND=gemini before running this script.")
    docs = resolve_docs(args.docs)

    started_at = datetime.now(UTC).isoformat()
    results: list[dict[str, object]] = []
    total_runs = len(docs)
    print(
        f"backend={summarizer.config.backend} "
        f"model={summarizer.config.model} docs={total_runs}"
    )

    for index, doc_path in enumerate(docs, start=1):
        text = doc_path.read_text(encoding="utf-8", errors="ignore")
        request = SummaryRequest(
            title=doc_path.stem,
            text=text,
            jurisdiction=args.jurisdiction,
            max_input_chars=args.max_input_chars,
        )

        started = time.monotonic()
        try:
            result = summarizer.summarize(request)
        except SummarizerError as exc:
            elapsed = time.monotonic() - started
            failure = {
                "document": str(doc_path),
                "elapsed_seconds": round(elapsed, 1),
                "ok": False,
                "error": str(exc),
            }
            results.append(failure)
            print(
                f"[{index}/{total_runs}] fail "
                f"doc={doc_path.name} seconds={elapsed:.1f} error={exc}"
            )
            continue

        elapsed = time.monotonic() - started
        success = {
            "document": str(doc_path),
            "elapsed_seconds": round(elapsed, 1),
            "ok": True,
            "result": asdict(result),
        }
        results.append(success)
        print(
            f"[{index}/{total_runs}] ok "
            f"doc={doc_path.name} seconds={elapsed:.1f} confidence={result.confidence}"
        )

    args.output_path.parent.mkdir(parents=True, exist_ok=True)
    args.output_path.write_text(
        json.dumps(
            {
                "started_at": started_at,
                "backend": summarizer.config.backend,
                "model": summarizer.config.model,
                "documents": [str(path) for path in docs],
                "max_input_chars": args.max_input_chars,
                "results": results,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    print(f"output_path={args.output_path}")


if __name__ == "__main__":
    main()
