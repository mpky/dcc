from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
import shutil
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from data_center_digest.db import (
    connect,
    list_relevant_documents_for_summary,
    upsert_document_summary,
)
from data_center_digest.run_once import summary_path_for, write_summary_file
from data_center_digest.summarizer import SummaryRequest, Summarizer, SummarizerError


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Summarize relevant extracted documents from the SQLite corpus.")
    parser.add_argument("--db-path", type=Path, default=ROOT / "data" / "app.db")
    parser.add_argument("--data-dir", type=Path, default=ROOT / "data")
    parser.add_argument("--source-id", help="Limit to a single source id.")
    parser.add_argument("--limit", type=int, default=5, help="Maximum number of documents to summarize.")
    parser.add_argument(
        "--max-input-chars",
        type=int,
        default=24000,
        help="Maximum number of extracted characters to send to the summarizer per document.",
    )
    parser.add_argument(
        "--include-existing",
        action="store_true",
        help="Re-summarize documents even if a summary already exists for the active backend/model.",
    )
    return parser.parse_args()


def render_progress(
    *,
    completed: int,
    total: int,
    successful: int,
    failed: int,
    current_title: str | None = None,
    current_index: int | None = None,
) -> str:
    width = min(max(shutil.get_terminal_size((100, 20)).columns - 50, 10), 40)
    ratio = 1.0 if total == 0 else completed / total
    filled = int(width * ratio)
    bar = "#" * filled + "-" * (width - filled)
    parts = [f"[{bar}] {completed}/{total}", f"ok={successful}", f"fail={failed}"]
    if current_title is not None and current_index is not None:
        parts.append(f"current={current_index}/{total} {current_title}")
    return " ".join(parts)


def print_progress(
    *,
    completed: int,
    total: int,
    successful: int,
    failed: int,
    current_title: str | None = None,
    current_index: int | None = None,
) -> None:
    line = render_progress(
        completed=completed,
        total=total,
        successful=successful,
        failed=failed,
        current_title=current_title,
        current_index=current_index,
    )
    if sys.stdout.isatty():
        print(f"\r{line}", end="", flush=True)
    else:
        print(line, flush=True)


def main() -> None:
    args = parse_args()
    summarizer = Summarizer.from_env()
    summarized_at = datetime.now(UTC).isoformat()
    connection = connect(args.db_path)
    try:
        rows = list_relevant_documents_for_summary(
            connection,
            backend=summarizer.config.backend,
            model=summarizer.config.model,
            source_id=args.source_id,
            limit=args.limit,
            include_existing=args.include_existing,
        )
        print(f"candidate_documents={len(rows)}")
        successful = 0
        failed = 0
        total = len(rows)
        print_progress(completed=0, total=total, successful=successful, failed=failed)

        for index, row in enumerate(rows, start=1):
            print()
            print_progress(
                completed=index - 1,
                total=total,
                successful=successful,
                failed=failed,
                current_title=row["title"],
                current_index=index,
            )
            text = row["extracted_text"]
            started = time.monotonic()
            try:
                result = summarizer.summarize(
                    SummaryRequest(
                        title=row["title"],
                        text=text,
                        jurisdiction=row["jurisdiction"],
                        meeting_title=row["meeting_title"],
                        source_url=row["url"],
                        max_input_chars=args.max_input_chars,
                    )
                )
            except SummarizerError as exc:
                failed += 1
                elapsed = time.monotonic() - started
                print(f"[{index}/{len(rows)}] error title={row['title']} error={exc}")
                print(f"  seconds={elapsed:.1f}")
                print_progress(completed=index, total=total, successful=successful, failed=failed)
                continue
            meeting_folder_id = Path(row["text_path"]).parent.name
            summary_payload = {
                "document_id": row["document_id"],
                "backend": result.backend,
                "model": result.model,
                "title": row["title"],
                "meeting_title": row["meeting_title"],
                "source_id": row["source_id"],
                "source_url": row["url"],
                "summary": result.summary,
                "why_it_matters": result.why_it_matters,
                "topic_tags": result.topic_tags,
                "confidence": result.confidence,
                "next_watch": result.next_watch,
            }
            summary_path = summary_path_for(
                data_dir=args.data_dir,
                source_id=row["source_id"],
                meeting_folder_id=meeting_folder_id,
                document_title=row["title"],
                backend=result.backend,
                model=result.model,
            )
            write_summary_file(summary_path, summary_payload)
            upsert_document_summary(
                connection,
                document_id=row["document_id"],
                backend=result.backend,
                model=result.model,
                summary_path=str(summary_path),
                summary=result.summary,
                why_it_matters=result.why_it_matters,
                topic_tags_json=json.dumps(result.topic_tags),
                confidence=result.confidence,
                next_watch=result.next_watch,
                raw_response=result.raw_response,
                summarized_at=summarized_at,
            )
            connection.commit()
            successful += 1
            elapsed = time.monotonic() - started
            print(
                f"[{index}/{len(rows)}] {row['title']} "
                f"confidence={result.confidence} seconds={elapsed:.1f} summary_path={summary_path}"
            )
            print_progress(completed=index, total=total, successful=successful, failed=failed)
        print()
        print(f"successful={successful}")
        print(f"failed={failed}")
    finally:
        connection.close()


if __name__ == "__main__":
    main()
