from __future__ import annotations

import argparse
from datetime import UTC, datetime
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from data_center_digest.db import connect, list_digest_entries
from data_center_digest.digest import render_html_digest, render_markdown_digest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build local Markdown and HTML digests from recent summarized documents.")
    parser.add_argument("--db-path", type=Path, default=ROOT / "data" / "app.db")
    parser.add_argument("--data-dir", type=Path, default=ROOT / "data")
    parser.add_argument("--source-id", help="Limit the digest to one source id.")
    parser.add_argument("--backend", help="Limit the digest to one summary backend.")
    parser.add_argument("--model", help="Limit the digest to one summary model.")
    parser.add_argument("--limit", type=int, default=10, help="Maximum number of digest entries.")
    parser.add_argument("--output-path", type=Path, help="Optional explicit output path for the Markdown digest.")
    return parser.parse_args()


def default_output_path(*, data_dir: Path, source_id: str | None, generated_at: datetime) -> Path:
    stamp = generated_at.strftime("%Y%m%dT%H%M%SZ")
    label = source_id or "all_sources"
    return data_dir / "digests" / label / f"{stamp}.md"


def html_output_path(markdown_path: Path) -> Path:
    return markdown_path.with_suffix(".html")


def source_label_for(entries: list[dict], source_id: str | None) -> str:
    jurisdictions = sorted({entry["jurisdiction"] for entry in entries if entry.get("jurisdiction")})
    if len(jurisdictions) == 1:
        return jurisdictions[0]
    if source_id:
        return source_id
    return "All sources"


def main() -> None:
    args = parse_args()
    generated_at = datetime.now(UTC)
    connection = connect(args.db_path)
    try:
        rows = list_digest_entries(
            connection,
            source_id=args.source_id,
            backend=args.backend,
            model=args.model,
            limit=args.limit,
        )
    finally:
        connection.close()

    if not rows:
        raise SystemExit("No summarized relevant documents matched the requested digest filters.")

    entries = [dict(row) for row in rows]
    markdown = render_markdown_digest(
        entries=entries,
        generated_at=generated_at,
        source_label=source_label_for(entries, args.source_id),
    )
    html = render_html_digest(
        entries=entries,
        generated_at=generated_at,
        source_label=source_label_for(entries, args.source_id),
    )

    markdown_output_path = args.output_path or default_output_path(
        data_dir=args.data_dir,
        source_id=args.source_id,
        generated_at=generated_at,
    )
    html_output = html_output_path(markdown_output_path)
    markdown_output_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_output_path.write_text(markdown, encoding="utf-8")
    html_output.write_text(html, encoding="utf-8")

    print(f"entries={len(entries)}")
    print(f"markdown_output_path={markdown_output_path}")
    print(f"html_output_path={html_output}")


if __name__ == "__main__":
    main()
