from __future__ import annotations

import argparse
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
import re
import time
from urllib.parse import unquote, urlparse
from urllib.request import Request, urlopen

from .config import SourceConfig, load_sources
from .db import (
    connect,
    document_needs_summary,
    item_needs_expansion,
    mark_item_expanded,
    record_source_run,
    upsert_document,
    upsert_document_relevance,
    upsert_document_summary,
    upsert_document_text,
    upsert_item,
    upsert_source,
)
from .html_links import LinkExtractor, filter_links
from .laserfiche import LaserficheClient, extract_folder_id
from .pdf_text import PDFTextExtractor
from .relevance import analyze_relevance
from .summarizer import SummaryRequest, Summarizer, SummarizerError


DEFAULT_CONFIG_PATH = Path("config/sources.json")
DEFAULT_DB_PATH = Path("data/app.db")
DEFAULT_DATA_DIR = Path("data")
USER_AGENT = "data-center-ceramics/0.1 (+https://github.local/data-center-ceramics)"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch one configured government source and record new items.")
    parser.add_argument("--config-path", type=Path, default=DEFAULT_CONFIG_PATH)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
    parser.add_argument("--source-id", help="Limit execution to a single source id.")
    parser.add_argument("--document-download-limit", type=int, help="Limit document expansion to the first N new meeting folders.")
    parser.add_argument("--summarize-relevant", action="store_true", help="Summarize relevant documents with the configured LLM backend.")
    parser.add_argument("--summarize-limit", type=int, help="Limit summaries to the first N relevant documents in this run.")
    parser.add_argument("--force-resummarize", action="store_true", help="Refresh summaries even if one already exists for the active backend/model.")
    return parser.parse_args()


def fetch_html(url: str) -> bytes:
    request = Request(url, headers={"User-Agent": USER_AGENT})
    with urlopen(request, timeout=30) as response:
        return response.read()


def snapshot_path_for(source_id: str, fetched_at: datetime, data_dir: Path) -> Path:
    stamp = fetched_at.strftime("%Y%m%dT%H%M%SZ")
    return data_dir / "raw" / source_id / f"{stamp}.html"


def save_snapshot(content: bytes, path: Path) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return hashlib.sha256(content).hexdigest()


def save_binary(content: bytes, path: Path) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return hashlib.sha256(content).hexdigest()


def safe_filename(name: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._ -]+", "_", name).strip()
    return cleaned or "document.pdf"


def summary_path_for(
    *,
    data_dir: Path,
    source_id: str,
    meeting_folder_id: str,
    document_title: str,
    backend: str,
    model: str,
) -> Path:
    model_slug = safe_filename(model.replace(":", "-"))
    file_name = safe_filename(f"{Path(document_title).stem}.{backend}.{model_slug}.json")
    return data_dir / "summaries" / source_id / meeting_folder_id / file_name


def write_summary_file(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def discover_links(source: SourceConfig, html_bytes: bytes) -> list[tuple[str, str, str]]:
    extractor = LinkExtractor(base_url=source.url)
    extractor.feed(html_bytes.decode("utf-8", errors="ignore"))
    links = filter_links(
        extractor.links,
        base_url=source.url,
        allowed_domains=source.allowed_domains,
        include_patterns=source.include_patterns,
        exclude_patterns=source.exclude_patterns,
    )
    discovered: list[tuple[str, str, str]] = []
    for link in links:
        item_id = hashlib.sha256(link.url.encode("utf-8")).hexdigest()
        discovered.append((item_id, link.title, link.url))
    return discovered


def run_generic_source(source: SourceConfig, fetched_at: datetime, data_dir: Path) -> tuple[Path, str, list[tuple[str, str, str]]]:
    html_bytes = fetch_html(source.url)
    snapshot_path = snapshot_path_for(source.id, fetched_at, data_dir)
    snapshot_hash = save_snapshot(html_bytes, snapshot_path)
    discovered = discover_links(source, html_bytes)
    return snapshot_path, snapshot_hash, discovered


def run_laserfiche_source(source: SourceConfig, fetched_at: datetime, data_dir: Path) -> tuple[Path, str, list[tuple[str, str, str]]]:
    client = LaserficheClient(user_agent=USER_AGENT)
    discovery = client.discover_meetings(source)

    stamp = fetched_at.strftime("%Y%m%dT%H%M%SZ")
    root_snapshot_path = data_dir / "raw" / source.id / f"{stamp}-{discovery.root_artifact.name}.{discovery.root_artifact.extension}"
    snapshot_hash = save_snapshot(discovery.root_artifact.content, root_snapshot_path)

    for artifact in discovery.year_artifacts:
        year_snapshot_path = data_dir / "raw" / source.id / f"{stamp}-{artifact.name}.{artifact.extension}"
        save_snapshot(artifact.content, year_snapshot_path)

    discovered = []
    for link in discovery.meetings:
        item_id = hashlib.sha256(link.url.encode("utf-8")).hexdigest()
        discovered.append((item_id, link.title, link.url))

    return root_snapshot_path, snapshot_hash, discovered


def collect_documents_for_new_laserfiche_items(
    source: SourceConfig,
    items_to_expand: list[tuple[str, str, str]],
    fetched_at: datetime,
    data_dir: Path,
    connection,
    document_download_limit: int | None,
    summarizer: Summarizer | None,
    summarize_limit: int | None,
    force_resummarize: bool,
) -> tuple[int, int, int, int]:
    if not items_to_expand:
        return 0, 0, 0, 0

    items_to_process = items_to_expand[:document_download_limit] if document_download_limit is not None else items_to_expand
    client = LaserficheClient(user_agent=USER_AGENT)
    extractor = PDFTextExtractor()
    stamp = fetched_at.strftime("%Y%m%dT%H%M%SZ")
    new_documents = 0
    expanded_items = 0
    relevant_documents = 0
    summarized_documents = 0

    print(f"expanding_items={len(items_to_process)}")

    for item_index, (item_id, meeting_title, meeting_url) in enumerate(items_to_process, start=1):
        meeting_started = time.monotonic()
        artifact, pdf_links = client.fetch_meeting_documents(source, meeting_url)
        meeting_folder_id = extract_folder_id(meeting_url)
        snapshot_path = data_dir / "raw" / source.id / f"{stamp}-{artifact.name}.{artifact.extension}"
        save_snapshot(artifact.content, snapshot_path)
        print(f"[meeting {item_index}/{len(items_to_process)}] {meeting_title} pdfs={len(pdf_links)}")

        for pdf_index, pdf_link in enumerate(pdf_links, start=1):
            document_started = time.monotonic()
            file_name = Path(unquote(urlparse(pdf_link.url).path)).name
            local_path = data_dir / "items" / source.id / meeting_folder_id / safe_filename(file_name)
            pdf_bytes = client.fetch(pdf_link.url)
            pdf_hash = save_binary(pdf_bytes, local_path)
            document_id = hashlib.sha256(pdf_link.url.encode("utf-8")).hexdigest()
            is_new = upsert_document(
                connection,
                document_id=document_id,
                item_id=item_id,
                title=pdf_link.title,
                url=pdf_link.url,
                local_path=str(local_path),
                sha256=pdf_hash,
                seen_at=fetched_at.isoformat(),
            )
            if is_new:
                new_documents += 1

            extraction = extractor.extract(local_path)
            text_file_name = f"{Path(file_name).stem}.txt"
            text_path = data_dir / "text" / source.id / meeting_folder_id / safe_filename(text_file_name)
            save_binary(extraction.text.encode("utf-8"), text_path)
            upsert_document_text(
                connection,
                document_id=document_id,
                text_path=str(text_path),
                extracted_text=extraction.text,
                extraction_method=extraction.method,
                page_count=extraction.page_count,
                extracted_at=fetched_at.isoformat(),
            )
            relevance = analyze_relevance(pdf_link.title, extraction.text)
            upsert_document_relevance(
                connection,
                document_id=document_id,
                is_relevant=relevance.is_relevant,
                score=relevance.score,
                categories_json=json.dumps(relevance.categories),
                matched_terms_json=json.dumps(relevance.matched_terms),
                matches_json=relevance.matches_json,
                rationale=relevance.rationale,
                analyzed_at=fetched_at.isoformat(),
            )
            connection.commit()
            elapsed = time.monotonic() - document_started
            if relevance.is_relevant:
                relevant_documents += 1
            print(
                f"  [pdf {pdf_index}/{len(pdf_links)}] {pdf_link.title} "
                f"pages={extraction.page_count} method={extraction.method} "
                f"chars={len(extraction.text)} score={relevance.score} "
                f"relevant={str(relevance.is_relevant).lower()} seconds={elapsed:.1f}"
            )
            if relevance.is_relevant:
                print(f"    rationale={relevance.rationale}")
                should_summarize = summarizer is not None and (
                    summarize_limit is None or summarized_documents < summarize_limit
                )
                if should_summarize and (
                    force_resummarize
                    or document_needs_summary(
                        connection,
                        document_id=document_id,
                        backend=summarizer.config.backend,
                        model=summarizer.config.model,
                    )
                ):
                    try:
                        summary_result = summarizer.summarize(
                            SummaryRequest(
                                title=pdf_link.title,
                                text=extraction.text,
                                jurisdiction=source.jurisdiction,
                                source_url=pdf_link.url,
                                meeting_title=meeting_title,
                            )
                        )
                    except SummarizerError as exc:
                        print(f"    summary_error={exc}")
                    else:
                        summary_payload = {
                            "document_id": document_id,
                            "backend": summary_result.backend,
                            "model": summary_result.model,
                            "title": pdf_link.title,
                            "meeting_title": meeting_title,
                            "source_url": pdf_link.url,
                            "summary": summary_result.summary,
                            "why_it_matters": summary_result.why_it_matters,
                            "topic_tags": summary_result.topic_tags,
                            "confidence": summary_result.confidence,
                            "next_watch": summary_result.next_watch,
                        }
                        summary_path = summary_path_for(
                            data_dir=data_dir,
                            source_id=source.id,
                            meeting_folder_id=meeting_folder_id,
                            document_title=pdf_link.title,
                            backend=summary_result.backend,
                            model=summary_result.model,
                        )
                        write_summary_file(summary_path, summary_payload)
                        upsert_document_summary(
                            connection,
                            document_id=document_id,
                            backend=summary_result.backend,
                            model=summary_result.model,
                            summary_path=str(summary_path),
                            summary=summary_result.summary,
                            why_it_matters=summary_result.why_it_matters,
                            topic_tags_json=json.dumps(summary_result.topic_tags),
                            confidence=summary_result.confidence,
                            next_watch=summary_result.next_watch,
                            raw_response=summary_result.raw_response,
                            summarized_at=fetched_at.isoformat(),
                        )
                        connection.commit()
                        summarized_documents += 1
                        print(
                            f"    summarized backend={summary_result.backend} "
                            f"model={summary_result.model} confidence={summary_result.confidence}"
                        )

        mark_item_expanded(
            connection,
            item_id=item_id,
            snapshot_path=str(snapshot_path),
            document_count=len(pdf_links),
            expanded_at=fetched_at.isoformat(),
        )
        connection.commit()
        expanded_items += 1
        print(f"[meeting complete] {meeting_title} seconds={time.monotonic() - meeting_started:.1f}")

    return new_documents, expanded_items, relevant_documents, summarized_documents


def run_for_source(
    source: SourceConfig,
    db_path: Path,
    data_dir: Path,
    document_download_limit: int | None = None,
    summarize_relevant: bool = False,
    summarize_limit: int | None = None,
    force_resummarize: bool = False,
) -> None:
    fetched_at = datetime.now(UTC)
    if source.kind == "laserfiche_meeting_folders":
        snapshot_path, snapshot_hash, discovered = run_laserfiche_source(source, fetched_at, data_dir)
    else:
        snapshot_path, snapshot_hash, discovered = run_generic_source(source, fetched_at, data_dir)

    connection = connect(db_path)
    try:
        upsert_source(connection, source.id, source.name, source.jurisdiction, source.kind, source.url)
        record_source_run(
            connection,
            source_id=source.id,
            fetched_at=fetched_at.isoformat(),
            snapshot_path=str(snapshot_path),
            snapshot_sha256=snapshot_hash,
            link_count=len(discovered),
        )

        new_items: list[tuple[str, str]] = []
        for item_id, title, url in discovered:
            is_new = upsert_item(
                connection,
                item_id=item_id,
                source_id=source.id,
                title=title,
                url=url,
                seen_at=fetched_at.isoformat(),
            )
            if is_new:
                new_items.append((title, url))

        items_requiring_expansion = [
            (item_id, title, url)
            for item_id, title, url in discovered
            if item_needs_expansion(connection, item_id)
        ]
        connection.commit()

        new_documents = 0
        expanded_items = 0
        relevant_documents = 0
        summarized_documents = 0
        summarizer = Summarizer.from_env() if summarize_relevant else None
        if source.kind == "laserfiche_meeting_folders":
            new_documents, expanded_items, relevant_documents, summarized_documents = collect_documents_for_new_laserfiche_items(
                source,
                items_requiring_expansion,
                fetched_at,
                data_dir,
                connection,
                document_download_limit=document_download_limit,
                summarizer=summarizer,
                summarize_limit=summarize_limit,
                force_resummarize=force_resummarize,
            )

        connection.commit()
    finally:
        connection.close()

    print(f"source={source.id}")
    print(f"snapshot={snapshot_path}")
    print(f"discovered_links={len(discovered)}")
    print(f"new_items={len(new_items)}")
    if source.kind == "laserfiche_meeting_folders":
        print(f"items_requiring_expansion={len(items_requiring_expansion)}")
        print(f"expanded_items={expanded_items}")
        print(f"new_documents={new_documents}")
        print(f"relevant_documents={relevant_documents}")
        print(f"summarized_documents={summarized_documents}")
    for title, url in new_items[:10]:
        print(f"- {title} -> {url}")


def main() -> None:
    args = parse_args()
    sources = load_sources(args.config_path)
    if args.source_id:
        sources = [source for source in sources if source.id == args.source_id]
    if not sources:
        raise SystemExit("No sources matched the requested source id.")

    for source in sources:
        run_for_source(
            source,
            db_path=args.db_path,
            data_dir=args.data_dir,
            document_download_limit=args.document_download_limit,
            summarize_relevant=args.summarize_relevant,
            summarize_limit=args.summarize_limit,
            force_resummarize=args.force_resummarize,
        )


if __name__ == "__main__":
    main()
