from __future__ import annotations

from datetime import datetime
import re
from dataclasses import dataclass
from html.parser import HTMLParser
from http.cookiejar import CookieJar
from urllib.parse import urlencode, urljoin, urlparse
from urllib.request import HTTPCookieProcessor, Request, build_opener

from .config import SourceConfig
from .html_links import Link, LinkExtractor


YEAR_TITLE_RE = re.compile(r"^\d{4}$")
MEETING_TITLE_RE = re.compile(r"^\d{2}-\d{2}-\d{2}\b")


@dataclass(frozen=True)
class LaserficheDiscovery:
    root_html: bytes
    year_pages: list[tuple[str, bytes]]
    meetings: list[Link]


class LoginFormParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.in_form = False
        self.form_action: str | None = None
        self.fields: dict[str, str] = {}
        self._current_select_name: str | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attrs_dict = dict(attrs)

        if tag == "form" and attrs_dict.get("method", "").lower() == "post":
            self.in_form = True
            self.form_action = attrs_dict.get("action")
            return

        if not self.in_form:
            return

        if tag == "input":
            name = attrs_dict.get("name")
            if name:
                self.fields[name] = attrs_dict.get("value", "")
            return

        if tag == "select":
            self._current_select_name = attrs_dict.get("name")
            return

        if tag == "option" and self._current_select_name:
            value = attrs_dict.get("value", "")
            if "selected" in attrs_dict or self._current_select_name not in self.fields:
                self.fields[self._current_select_name] = value

    def handle_endtag(self, tag: str) -> None:
        if tag == "form":
            self.in_form = False
        elif tag == "select":
            self._current_select_name = None


def _filter_row_links(base_url: str, html_bytes: bytes) -> list[Link]:
    extractor = LinkExtractor(base_url=base_url)
    extractor.feed(html_bytes.decode("utf-8", errors="ignore"))
    links: list[Link] = []
    seen: set[str] = set()

    for link in extractor.links:
        parsed = urlparse(link.url)
        if parsed.scheme not in {"http", "https"}:
            continue
        if parsed.netloc != "lfportal.loudoun.gov":
            continue
        if parsed.path.endswith("/Row1.aspx") or "/Row1.aspx" in parsed.path:
            normalized = link.url
        else:
            continue
        title = link.title.replace("[Icon]", "").strip()
        if not title or title == "Name":
            continue
        if normalized in seen:
            continue
        seen.add(normalized)
        links.append(Link(title=title, url=normalized))

    return links


class LaserficheClient:
    def __init__(self, user_agent: str) -> None:
        cookie_jar = CookieJar()
        self.opener = build_opener(HTTPCookieProcessor(cookie_jar))
        self.user_agent = user_agent

    def fetch(self, url: str, data: bytes | None = None) -> bytes:
        request = Request(url, data=data, headers={"User-Agent": self.user_agent})
        with self.opener.open(request, timeout=30) as response:
            return response.read()

    def login(self, login_url: str) -> None:
        welcome_html = self.fetch(login_url)
        parser = LoginFormParser()
        parser.feed(welcome_html.decode("utf-8", errors="ignore"))
        if not parser.form_action:
            raise RuntimeError("Unable to locate Laserfiche login form.")

        post_url = urljoin(login_url, parser.form_action)
        payload = urlencode(parser.fields).encode("utf-8")
        self.fetch(post_url, data=payload)

    def discover_meetings(self, source: SourceConfig) -> LaserficheDiscovery:
        settings = source.settings or {}
        login_url = str(settings.get("login_url", ""))
        year_count = int(settings.get("year_count", 1))
        if not login_url:
            raise RuntimeError("Laserfiche source is missing login_url in settings.")

        self.login(login_url)
        root_html = self.fetch(source.url)
        root_links = _filter_row_links(source.url, root_html)

        year_links = [link for link in root_links if YEAR_TITLE_RE.fullmatch(link.title)]
        year_links = sorted(year_links, key=lambda link: int(link.title), reverse=True)[:year_count]

        year_pages: list[tuple[str, bytes]] = []
        meetings: list[Link] = []
        seen_meeting_urls: set[str] = set()

        for year_link in year_links:
            year_html = self.fetch(year_link.url)
            year_pages.append((year_link.title, year_html))
            for link in _filter_row_links(year_link.url, year_html):
                if not MEETING_TITLE_RE.match(link.title):
                    continue
                if link.url in seen_meeting_urls:
                    continue
                seen_meeting_urls.add(link.url)
                meetings.append(link)

        meetings.sort(key=_meeting_sort_key, reverse=True)
        return LaserficheDiscovery(root_html=root_html, year_pages=year_pages, meetings=meetings)


def _meeting_sort_key(link: Link) -> tuple[datetime, str]:
    match = MEETING_TITLE_RE.match(link.title)
    if match:
        return (datetime.strptime(match.group(0), "%m-%d-%y"), link.title)
    return (datetime.min, link.title)
