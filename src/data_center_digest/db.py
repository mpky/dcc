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

CREATE TABLE IF NOT EXISTS item_expansions (
    item_id TEXT PRIMARY KEY,
    snapshot_path TEXT NOT NULL,
    document_count INTEGER NOT NULL,
    expanded_at TEXT NOT NULL,
    FOREIGN KEY (item_id) REFERENCES items(item_id)
);

CREATE TABLE IF NOT EXISTS documents (
    document_id TEXT PRIMARY KEY,
    item_id TEXT NOT NULL,
    title TEXT NOT NULL,
    url TEXT NOT NULL,
    local_path TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    first_seen_at TEXT NOT NULL,
    downloaded_at TEXT NOT NULL,
    FOREIGN KEY (item_id) REFERENCES items(item_id)
);

CREATE TABLE IF NOT EXISTS document_texts (
    document_id TEXT PRIMARY KEY,
    text_path TEXT NOT NULL,
    extracted_text TEXT NOT NULL,
    extraction_method TEXT NOT NULL,
    page_count INTEGER NOT NULL,
    extracted_at TEXT NOT NULL,
    FOREIGN KEY (document_id) REFERENCES documents(document_id)
);

CREATE TABLE IF NOT EXISTS document_relevance (
    document_id TEXT PRIMARY KEY,
    is_relevant INTEGER NOT NULL,
    score INTEGER NOT NULL,
    categories_json TEXT NOT NULL,
    matched_terms_json TEXT NOT NULL,
    matches_json TEXT NOT NULL,
    rationale TEXT NOT NULL,
    analyzed_at TEXT NOT NULL,
    FOREIGN KEY (document_id) REFERENCES documents(document_id)
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


def item_needs_expansion(connection: sqlite3.Connection, item_id: str) -> bool:
    row = connection.execute(
        "SELECT item_id FROM item_expansions WHERE item_id = ?",
        (item_id,),
    ).fetchone()
    return row is None


def mark_item_expanded(
    connection: sqlite3.Connection,
    item_id: str,
    snapshot_path: str,
    document_count: int,
    expanded_at: str,
) -> None:
    connection.execute(
        """
        INSERT INTO item_expansions (item_id, snapshot_path, document_count, expanded_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(item_id) DO UPDATE SET
            snapshot_path = excluded.snapshot_path,
            document_count = excluded.document_count,
            expanded_at = excluded.expanded_at
        """,
        (item_id, snapshot_path, document_count, expanded_at),
    )


def upsert_document(
    connection: sqlite3.Connection,
    document_id: str,
    item_id: str,
    title: str,
    url: str,
    local_path: str,
    sha256: str,
    seen_at: str,
) -> bool:
    existing = connection.execute(
        "SELECT document_id FROM documents WHERE document_id = ?",
        (document_id,),
    ).fetchone()

    if existing is None:
        connection.execute(
            """
            INSERT INTO documents (document_id, item_id, title, url, local_path, sha256, first_seen_at, downloaded_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (document_id, item_id, title, url, local_path, sha256, seen_at, seen_at),
        )
        return True

    connection.execute(
        """
        UPDATE documents
        SET title = ?, url = ?, local_path = ?, sha256 = ?, downloaded_at = ?
        WHERE document_id = ?
        """,
        (title, url, local_path, sha256, seen_at, document_id),
    )
    return False


def upsert_document_text(
    connection: sqlite3.Connection,
    document_id: str,
    text_path: str,
    extracted_text: str,
    extraction_method: str,
    page_count: int,
    extracted_at: str,
) -> bool:
    existing = connection.execute(
        "SELECT document_id FROM document_texts WHERE document_id = ?",
        (document_id,),
    ).fetchone()

    if existing is None:
        connection.execute(
            """
            INSERT INTO document_texts (document_id, text_path, extracted_text, extraction_method, page_count, extracted_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (document_id, text_path, extracted_text, extraction_method, page_count, extracted_at),
        )
        return True

    connection.execute(
        """
        UPDATE document_texts
        SET text_path = ?, extracted_text = ?, extraction_method = ?, page_count = ?, extracted_at = ?
        WHERE document_id = ?
        """,
        (text_path, extracted_text, extraction_method, page_count, extracted_at, document_id),
    )
    return False


def upsert_document_relevance(
    connection: sqlite3.Connection,
    document_id: str,
    is_relevant: bool,
    score: int,
    categories_json: str,
    matched_terms_json: str,
    matches_json: str,
    rationale: str,
    analyzed_at: str,
) -> bool:
    existing = connection.execute(
        "SELECT document_id FROM document_relevance WHERE document_id = ?",
        (document_id,),
    ).fetchone()

    if existing is None:
        connection.execute(
            """
            INSERT INTO document_relevance (
                document_id,
                is_relevant,
                score,
                categories_json,
                matched_terms_json,
                matches_json,
                rationale,
                analyzed_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                document_id,
                int(is_relevant),
                score,
                categories_json,
                matched_terms_json,
                matches_json,
                rationale,
                analyzed_at,
            ),
        )
        return True

    connection.execute(
        """
        UPDATE document_relevance
        SET is_relevant = ?, score = ?, categories_json = ?, matched_terms_json = ?, matches_json = ?, rationale = ?, analyzed_at = ?
        WHERE document_id = ?
        """,
        (
            int(is_relevant),
            score,
            categories_json,
            matched_terms_json,
            matches_json,
            rationale,
            analyzed_at,
            document_id,
        ),
    )
    return False
