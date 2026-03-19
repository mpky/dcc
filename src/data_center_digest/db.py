from __future__ import annotations

import sqlite3
from pathlib import Path


SCHEMA = """
CREATE TABLE IF NOT EXISTS sources (
    source_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    jurisdiction TEXT NOT NULL,
    kind TEXT NOT NULL,
    url TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS source_runs (
    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id TEXT NOT NULL,
    fetched_at TEXT NOT NULL,
    snapshot_path TEXT NOT NULL,
    snapshot_sha256 TEXT NOT NULL,
    link_count INTEGER NOT NULL,
    FOREIGN KEY (source_id) REFERENCES sources(source_id)
);

CREATE TABLE IF NOT EXISTS items (
    item_id TEXT PRIMARY KEY,
    source_id TEXT NOT NULL,
    title TEXT NOT NULL,
    url TEXT NOT NULL,
    first_seen_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    FOREIGN KEY (source_id) REFERENCES sources(source_id)
);
"""


def connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    connection.executescript(SCHEMA)
    return connection


def upsert_source(connection: sqlite3.Connection, source_id: str, name: str, jurisdiction: str, kind: str, url: str) -> None:
    connection.execute(
        """
        INSERT INTO sources (source_id, name, jurisdiction, kind, url)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(source_id) DO UPDATE SET
            name = excluded.name,
            jurisdiction = excluded.jurisdiction,
            kind = excluded.kind,
            url = excluded.url
        """,
        (source_id, name, jurisdiction, kind, url),
    )


def record_source_run(
    connection: sqlite3.Connection,
    source_id: str,
    fetched_at: str,
    snapshot_path: str,
    snapshot_sha256: str,
    link_count: int,
) -> None:
    connection.execute(
        """
        INSERT INTO source_runs (source_id, fetched_at, snapshot_path, snapshot_sha256, link_count)
        VALUES (?, ?, ?, ?, ?)
        """,
        (source_id, fetched_at, snapshot_path, snapshot_sha256, link_count),
    )


def upsert_item(connection: sqlite3.Connection, item_id: str, source_id: str, title: str, url: str, seen_at: str) -> bool:
    existing = connection.execute(
        "SELECT item_id FROM items WHERE item_id = ?",
        (item_id,),
    ).fetchone()

    if existing is None:
        connection.execute(
            """
            INSERT INTO items (item_id, source_id, title, url, first_seen_at, last_seen_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (item_id, source_id, title, url, seen_at, seen_at),
        )
        return True

    connection.execute(
        """
        UPDATE items
        SET title = ?, url = ?, last_seen_at = ?
        WHERE item_id = ?
        """,
        (title, url, seen_at, item_id),
    )
    return False
