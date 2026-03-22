from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class SourceConfig:
    id: str
    name: str
    jurisdiction: str
    kind: str
    url: str
    allowed_domains: list[str]
    include_patterns: list[str]
    exclude_patterns: list[str]
    settings: dict[str, Any] | None = None


def load_sources(config_path: Path) -> list[SourceConfig]:
    payload = json.loads(config_path.read_text())
    return [SourceConfig(**source) for source in payload["sources"]]
