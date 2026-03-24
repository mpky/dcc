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

CREATE TABLE IF NOT EXISTS document_summaries (
    summary_id INTEGER PRIMARY KEY AUTOINCREMENT,
    document_id TEXT NOT NULL,
    backend TEXT NOT NULL,
    model TEXT NOT NULL,
    summary_path TEXT NOT NULL,
    summary TEXT NOT NULL,
    why_it_matters TEXT NOT NULL,
    topic_tags_json TEXT NOT NULL,
    confidence TEXT NOT NULL,
    next_watch TEXT NOT NULL,
    raw_response TEXT NOT NULL,
    summarized_at TEXT NOT NULL,
    FOREIGN KEY (document_id) REFERENCES documents(document_id),
    UNIQUE (document_id, backend, model)
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


def document_needs_summary(connection: sqlite3.Connection, document_id: str, backend: str, model: str) -> bool:
    row = connection.execute(
        """
        SELECT 1
        FROM document_summaries
        WHERE document_id = ? AND backend = ? AND model = ?
        """,
        (document_id, backend, model),
    ).fetchone()
    return row is None


def upsert_document_summary(
    connection: sqlite3.Connection,
    document_id: str,
    backend: str,
    model: str,
    summary_path: str,
    summary: str,
    why_it_matters: str,
    topic_tags_json: str,
    confidence: str,
    next_watch: str,
    raw_response: str,
    summarized_at: str,
) -> bool:
    existing = connection.execute(
        """
        SELECT summary_id
        FROM document_summaries
        WHERE document_id = ? AND backend = ? AND model = ?
        """,
        (document_id, backend, model),
    ).fetchone()

    if existing is None:
        connection.execute(
            """
            INSERT INTO document_summaries (
                document_id,
                backend,
                model,
                summary_path,
                summary,
                why_it_matters,
                topic_tags_json,
                confidence,
                next_watch,
                raw_response,
                summarized_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                document_id,
                backend,
                model,
                summary_path,
                summary,
                why_it_matters,
                topic_tags_json,
                confidence,
                next_watch,
                raw_response,
                summarized_at,
            ),
        )
        return True

    connection.execute(
        """
        UPDATE document_summaries
        SET summary_path = ?, summary = ?, why_it_matters = ?, topic_tags_json = ?, confidence = ?, next_watch = ?, raw_response = ?, summarized_at = ?
        WHERE document_id = ? AND backend = ? AND model = ?
        """,
        (
            summary_path,
            summary,
            why_it_matters,
            topic_tags_json,
            confidence,
            next_watch,
            raw_response,
            summarized_at,
            document_id,
            backend,
            model,
        ),
    )
    return False


def list_relevant_documents_for_summary(
    connection: sqlite3.Connection,
    backend: str,
    model: str,
    source_id: str | None = None,
    limit: int | None = None,
    include_existing: bool = False,
) -> list[sqlite3.Row]:
    params: list[object] = []
    clauses = [
        "dr.is_relevant = 1",
        "dt.extracted_text <> ''",
    ]

    if not include_existing:
        params.extend([backend, model])
        clauses.append(
            """
            NOT EXISTS (
                SELECT 1
                FROM document_summaries ds
                WHERE ds.document_id = d.document_id
                  AND ds.backend = ?
                  AND ds.model = ?
            )
            """.strip()
        )

    if source_id:
        clauses.append("i.source_id = ?")
        params.append(source_id)

    query = f"""
        SELECT
            d.document_id,
            d.title,
            d.url,
            d.local_path,
            dt.text_path,
            dt.extracted_text,
            i.title AS meeting_title,
            i.source_id,
            s.jurisdiction,
            dr.score
        FROM documents d
        JOIN document_texts dt ON dt.document_id = d.document_id
        JOIN document_relevance dr ON dr.document_id = d.document_id
        JOIN items i ON i.item_id = d.item_id
        JOIN sources s ON s.source_id = i.source_id
        WHERE {" AND ".join(clauses)}
        ORDER BY dr.score DESC, d.first_seen_at DESC
    """
    if limit is not None:
        query += " LIMIT ?"
        params.append(limit)

    return connection.execute(query, params).fetchall()


def list_digest_entries(
    connection: sqlite3.Connection,
    source_id: str | None = None,
    backend: str | None = None,
    model: str | None = None,
    limit: int | None = 10,
) -> list[sqlite3.Row]:
    params: list[object] = []
    summary_filters: list[str] = []
    outer_filters: list[str] = ["dr.is_relevant = 1"]

    if backend is not None:
        summary_filters.append("ds.backend = ?")
        params.append(backend)
    if model is not None:
        summary_filters.append("ds.model = ?")
        params.append(model)
    if source_id is not None:
        outer_filters.append("i.source_id = ?")
        params.append(source_id)

    summary_where = f"WHERE {' AND '.join(summary_filters)}" if summary_filters else ""
    outer_filters.append("rs.rn = 1")

    query = f"""
        WITH ranked_summaries AS (
            SELECT
                ds.*,
                ROW_NUMBER() OVER (
                    PARTITION BY ds.document_id
                    ORDER BY ds.summarized_at DESC, ds.summary_id DESC
                ) AS rn
            FROM document_summaries ds
            {summary_where}
        )
        SELECT
            rs.document_id,
            rs.backend,
            rs.model,
            rs.summary_path,
            rs.summary,
            rs.why_it_matters,
            rs.topic_tags_json,
            rs.confidence,
            rs.next_watch,
            rs.summarized_at,
            d.title AS document_title,
            d.url AS document_url,
            i.title AS meeting_title,
            i.url AS meeting_url,
            i.source_id,
            s.name AS source_name,
            s.jurisdiction,
            dr.score
        FROM ranked_summaries rs
        JOIN documents d ON d.document_id = rs.document_id
        JOIN items i ON i.item_id = d.item_id
        JOIN sources s ON s.source_id = i.source_id
        JOIN document_relevance dr ON dr.document_id = d.document_id
        WHERE {" AND ".join(outer_filters)}
        ORDER BY rs.summarized_at DESC, dr.score DESC, d.title ASC
    """
    if limit is not None:
        query += " LIMIT ?"
        params.append(limit)

    return connection.execute(query, params).fetchall()
