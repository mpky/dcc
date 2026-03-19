from __future__ import annotations

import argparse
import hashlib
from datetime import UTC, datetime
from pathlib import Path
from urllib.request import Request, urlopen

from .config import SourceConfig, load_sources
from .db import connect, record_source_run, upsert_item, upsert_source
from .html_links import LinkExtractor, filter_links
from .laserfiche import LaserficheClient


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


def run_for_source(source: SourceConfig, db_path: Path, data_dir: Path) -> None:
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

        connection.commit()
    finally:
        connection.close()

    print(f"source={source.id}")
    print(f"snapshot={snapshot_path}")
    print(f"discovered_links={len(discovered)}")
    print(f"new_items={len(new_items)}")
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
        run_for_source(source, db_path=args.db_path, data_dir=args.data_dir)


if __name__ == "__main__":
    main()
