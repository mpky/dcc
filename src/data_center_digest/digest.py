from __future__ import annotations

import json
from collections import OrderedDict
from collections.abc import Mapping, Sequence
from datetime import datetime
from html import escape
from typing import Any


def _entry_dict(entry: Mapping[str, Any]) -> dict[str, Any]:
    topic_tags = entry.get("topic_tags")
    if topic_tags is None:
        topic_tags_json = entry.get("topic_tags_json", "[]")
        if isinstance(topic_tags_json, str):
            topic_tags = json.loads(topic_tags_json)
        else:
            topic_tags = list(topic_tags_json)
    return {
        "meeting_title": entry["meeting_title"],
        "meeting_url": entry.get("meeting_url"),
        "document_title": entry["document_title"],
        "document_url": entry["document_url"],
        "summary": entry["summary"],
        "why_it_matters": entry["why_it_matters"],
        "topic_tags": [str(tag).strip() for tag in topic_tags if str(tag).strip()],
        "confidence": entry["confidence"],
        "next_watch": entry["next_watch"],
        "jurisdiction": entry.get("jurisdiction"),
        "source_name": entry.get("source_name"),
        "backend": entry.get("backend"),
        "model": entry.get("model"),
        "score": entry.get("score"),
        "summarized_at": entry.get("summarized_at"),
    }


def _group_entries(entries: Sequence[Mapping[str, Any]]) -> tuple[list[dict[str, Any]], OrderedDict[str, list[dict[str, Any]]]]:
    normalized_entries = [_entry_dict(entry) for entry in entries]
    grouped: OrderedDict[str, list[dict[str, Any]]] = OrderedDict()
    for entry in normalized_entries:
        grouped.setdefault(entry["meeting_title"], []).append(entry)
    return normalized_entries, grouped


def render_markdown_digest(
    *,
    entries: Sequence[Mapping[str, Any]],
    generated_at: datetime,
    source_label: str,
) -> str:
    normalized_entries, grouped = _group_entries(entries)

    lines = [
        "# Data Center Legal Digest",
        "",
        f"Source: {source_label}",
        f"Generated: {generated_at.strftime('%Y-%m-%d %H:%M UTC')}",
        f"Documents: {len(normalized_entries)}",
        "",
    ]

    for meeting_title, meeting_entries in grouped.items():
        meeting_url = meeting_entries[0].get("meeting_url")
        lines.extend([f"## {meeting_title}", ""])
        if meeting_url:
            lines.extend([f"Meeting link: <{meeting_url}>", ""])

        for entry in meeting_entries:
            lines.append(f"### [{entry['document_title']}]({entry['document_url']})")
            lines.append("")
            lines.append(entry["summary"])
            lines.append("")
            lines.append(f"Why it matters: {entry['why_it_matters']}")
            lines.append("")
            meta = [f"Confidence: `{entry['confidence']}`"]
            if entry.get("score") is not None:
                meta.append(f"Score: `{entry['score']}`")
            if entry.get("backend") and entry.get("model"):
                meta.append(f"Model: `{entry['backend']}` / `{entry['model']}`")
            lines.append(" | ".join(meta))
            if entry["topic_tags"]:
                tags = ", ".join(f"`{tag}`" for tag in entry["topic_tags"])
                lines.append(f"Tags: {tags}")
            lines.append(f"Next watch: {entry['next_watch']}")
            lines.append("")

    return "\n".join(lines).strip() + "\n"


def render_html_digest(
    *,
    entries: Sequence[Mapping[str, Any]],
    generated_at: datetime,
    source_label: str,
) -> str:
    normalized_entries, grouped = _group_entries(entries)

    parts = [
        "<!doctype html>",
        '<html lang="en">',
        "<head>",
        '<meta charset="utf-8">',
        '<meta name="viewport" content="width=device-width, initial-scale=1">',
        "<title>Data Center Legal Digest</title>",
        "<style>",
        "body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; color: #172033; background: #f4f6f8; margin: 0; padding: 24px; }",
        ".wrap { max-width: 920px; margin: 0 auto; background: #ffffff; padding: 32px; border-radius: 16px; }",
        "h1, h2, h3 { color: #0f2a43; }",
        "h1 { margin-top: 0; }",
        ".meta { color: #4f5d75; margin-bottom: 24px; }",
        ".meeting { margin-top: 32px; padding-top: 12px; border-top: 1px solid #d9e2ec; }",
        ".card { margin: 18px 0; padding: 18px; background: #f8fbff; border: 1px solid #d9e2ec; border-radius: 12px; }",
        ".pill { display: inline-block; margin-right: 8px; padding: 2px 8px; border-radius: 999px; background: #e6f0ff; color: #123b63; font-size: 12px; }",
        ".small { color: #52606d; font-size: 14px; }",
        "a { color: #0b69a3; }",
        "</style>",
        "</head>",
        "<body>",
        '<div class="wrap">',
        "<h1>Data Center Legal Digest</h1>",
        f'<p class="meta"><strong>Source:</strong> {escape(source_label)}<br><strong>Generated:</strong> {escape(generated_at.strftime("%Y-%m-%d %H:%M UTC"))}<br><strong>Documents:</strong> {len(normalized_entries)}</p>',
    ]

    for meeting_title, meeting_entries in grouped.items():
        meeting_url = meeting_entries[0].get("meeting_url")
        parts.append('<section class="meeting">')
        parts.append(f"<h2>{escape(meeting_title)}</h2>")
        if meeting_url:
            parts.append(f'<p class="small"><a href="{escape(meeting_url)}">Meeting link</a></p>')

        for entry in meeting_entries:
            parts.append('<article class="card">')
            parts.append(f'<h3><a href="{escape(entry["document_url"])}">{escape(entry["document_title"])}</a></h3>')
            parts.append(f"<p>{escape(entry['summary'])}</p>")
            parts.append(f'<p><strong>Why it matters:</strong> {escape(entry["why_it_matters"])}</p>')

            meta_bits = [f'Confidence: {escape(entry["confidence"])}']
            if entry.get("score") is not None:
                meta_bits.append(f'Score: {escape(str(entry["score"]))}')
            if entry.get("backend") and entry.get("model"):
                meta_bits.append(f'Model: {escape(str(entry["backend"]))} / {escape(str(entry["model"]))}')
            parts.append(f'<p class="small">{" | ".join(meta_bits)}</p>')

            if entry["topic_tags"]:
                pills = "".join(f'<span class="pill">{escape(tag)}</span>' for tag in entry["topic_tags"])
                parts.append(f"<p>{pills}</p>")

            parts.append(f'<p><strong>Next watch:</strong> {escape(entry["next_watch"])}</p>')
            parts.append("</article>")

        parts.append("</section>")

    parts.extend(["</div>", "</body>", "</html>"])
    return "\n".join(parts) + "\n"
