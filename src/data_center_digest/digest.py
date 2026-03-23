from __future__ import annotations

import json
from collections import OrderedDict
from collections.abc import Mapping, Sequence
from datetime import datetime
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


def render_markdown_digest(
    *,
    entries: Sequence[Mapping[str, Any]],
    generated_at: datetime,
    source_label: str,
) -> str:
    normalized_entries = [_entry_dict(entry) for entry in entries]
    grouped: OrderedDict[str, list[dict[str, Any]]] = OrderedDict()
    for entry in normalized_entries:
        grouped.setdefault(entry["meeting_title"], []).append(entry)

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
