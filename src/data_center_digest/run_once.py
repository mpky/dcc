from __future__ import annotations

import argparse
import hashlib
from datetime import UTC, datetime
from pathlib import Path
import re
from urllib.parse import unquote, urlparse
from urllib.request import Request, urlopen

from .config import SourceConfig, load_sources
from .db import connect, record_source_run, upsert_document, upsert_document_text, upsert_item, upsert_source
from .html_links import LinkExtractor, filter_links
from .laserfiche import LaserficheClient, extract_folder_id
from .pdf_text import PDFTextExtractor


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
    new_items: list[tuple[str, str, str]],
    fetched_at: datetime,
    data_dir: Path,
    connection,
    document_download_limit: int | None,
) -> int:
    if not new_items:
        return 0

    items_to_process = new_items[:document_download_limit] if document_download_limit is not None else new_items
    client = LaserficheClient(user_agent=USER_AGENT)
    extractor = PDFTextExtractor()
    stamp = fetched_at.strftime("%Y%m%dT%H%M%SZ")
    new_documents = 0

    for item_id, _, meeting_url in items_to_process:
        artifact, pdf_links = client.fetch_meeting_documents(source, meeting_url)
        meeting_folder_id = extract_folder_id(meeting_url)
        snapshot_path = data_dir / "raw" / source.id / f"{stamp}-{artifact.name}.{artifact.extension}"
        save_snapshot(artifact.content, snapshot_path)

        for pdf_link in pdf_links:
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

    return new_documents


def run_for_source(source: SourceConfig, db_path: Path, data_dir: Path, document_download_limit: int | None = None) -> None:
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
        new_item_records: list[tuple[str, str, str]] = []
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
                new_item_records.append((item_id, title, url))

        new_documents = 0
        if source.kind == "laserfiche_meeting_folders":
            new_documents = collect_documents_for_new_laserfiche_items(
                source,
                new_item_records,
                fetched_at,
                data_dir,
                connection,
                document_download_limit=document_download_limit,
            )

        connection.commit()
    finally:
        connection.close()

    print(f"source={source.id}")
    print(f"snapshot={snapshot_path}")
    print(f"discovered_links={len(discovered)}")
    print(f"new_items={len(new_items)}")
    if source.kind == "laserfiche_meeting_folders":
        print(f"new_documents={new_documents}")
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
        )


if __name__ == "__main__":
    main()
