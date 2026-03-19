from __future__ import annotations

from dataclasses import dataclass
from html.parser import HTMLParser
from urllib.parse import urljoin, urlparse, urlunparse


@dataclass(frozen=True)
class Link:
    title: str
    url: str


class LinkExtractor(HTMLParser):
    def __init__(self, base_url: str) -> None:
        super().__init__()
        self.base_url = base_url
        self._current_href: str | None = None
        self._current_text: list[str] = []
        self.links: list[Link] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag != "a":
            return
        attrs_dict = dict(attrs)
        href = attrs_dict.get("href")
        if not href:
            return
        self._current_href = urljoin(self.base_url, href.strip())
        self._current_text = []

    def handle_data(self, data: str) -> None:
        if self._current_href is not None:
            self._current_text.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag != "a" or self._current_href is None:
            return
        title = " ".join(part.strip() for part in self._current_text if part.strip())
        self.links.append(Link(title=title or self._current_href, url=self._current_href))
        self._current_href = None
        self._current_text = []


def filter_links(
    links: list[Link],
    base_url: str,
    allowed_domains: list[str],
    include_patterns: list[str],
    exclude_patterns: list[str],
) -> list[Link]:
    seen_urls: set[str] = set()
    filtered: list[Link] = []
    base_parts = urlparse(base_url)

    for link in links:
        haystack = f"{link.title} {link.url}".casefold()
        parsed = urlparse(link.url)
        normalized_url = urlunparse(parsed._replace(fragment=""))

        if parsed.scheme not in {"http", "https"}:
            continue
        if base_parts is not None and parsed.fragment and (
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            parsed.query,
        ) == (
            base_parts.scheme,
            base_parts.netloc,
            base_parts.path,
            base_parts.query,
        ):
            continue
        if parsed.netloc and allowed_domains and not any(parsed.netloc.endswith(domain) for domain in allowed_domains):
            continue
        if any(pattern.casefold() in haystack for pattern in exclude_patterns):
            continue
        if include_patterns and not any(pattern.casefold() in haystack for pattern in include_patterns):
            continue
        if normalized_url in seen_urls:
            continue

        seen_urls.add(normalized_url)
        filtered.append(Link(title=link.title, url=normalized_url))

    return filtered
