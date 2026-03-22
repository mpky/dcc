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

from data_center_digest.summarizer import SummaryRequest, Summarizer, SummarizerConfig, SummarizerError


DEFAULT_DOCS = [
    ROOT / "data" / "text" / "loudoun_bos_meeting_documents" / "1969511" / "Item 11 LEGI-2024-0002_ Concorde Industrial Park.txt",
    ROOT / "data" / "text" / "loudoun_bos_meeting_documents" / "1969511" / "Item 11 LEGI-2024-0002_ Concorde Industrial Park-Supplemental.txt",
    ROOT / "data" / "text" / "loudoun_bos_meeting_documents" / "1969511" / "Item 10 LEGI-2023-0114_ Franklin Park West.txt",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare local summarizer models on the same document set.")
    parser.add_argument(
        "--models",
        nargs="+",
        default=["gemma3n:e2b", "gemma3:4b-it-qat"],
        help="Ollama model names to compare.",
    )
    parser.add_argument(
        "--docs",
        nargs="+",
        type=Path,
        default=DEFAULT_DOCS,
        help="Extracted text files to summarize.",
    )
    parser.add_argument("--jurisdiction", default="Loudoun County, VA")
    parser.add_argument("--max-input-chars", type=int, default=12000)
    parser.add_argument("--timeout-seconds", type=int, default=180)
    parser.add_argument(
        "--output-path",
        type=Path,
        default=ROOT / "data" / "evals" / "summarizer_bakeoff.json",
        help="Where to store the comparison results as JSON.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    started_at = datetime.now(UTC).isoformat()
    results: list[dict[str, object]] = []
    total_runs = len(args.models) * len(args.docs)
    run_number = 0

    print(f"models={len(args.models)} docs={len(args.docs)} total_runs={total_runs}")

    for model in args.models:
        summarizer = Summarizer(
            SummarizerConfig(
                backend="ollama",
                model=model,
                endpoint="http://localhost:11434",
                request_timeout_seconds=args.timeout_seconds,
            )
        )
        print(f"model={model}")

        for doc_path in args.docs:
            run_number += 1
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
                    "model": model,
                    "document": str(doc_path),
                    "elapsed_seconds": round(elapsed, 1),
                    "ok": False,
                    "error": str(exc),
                }
                results.append(failure)
                print(
                    f"[{run_number}/{total_runs}] fail model={model} "
                    f"doc={doc_path.name} seconds={elapsed:.1f} error={exc}"
                )
                continue

            elapsed = time.monotonic() - started
            success = {
                "model": model,
                "document": str(doc_path),
                "elapsed_seconds": round(elapsed, 1),
                "ok": True,
                "result": asdict(result),
            }
            results.append(success)
            print(
                f"[{run_number}/{total_runs}] ok model={model} "
                f"doc={doc_path.name} seconds={elapsed:.1f} confidence={result.confidence}"
            )

    args.output_path.parent.mkdir(parents=True, exist_ok=True)
    args.output_path.write_text(
        json.dumps(
            {
                "started_at": started_at,
                "models": args.models,
                "documents": [str(path) for path in args.docs],
                "max_input_chars": args.max_input_chars,
                "timeout_seconds": args.timeout_seconds,
                "results": results,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    print(f"output_path={args.output_path}")


if __name__ == "__main__":
    main()
