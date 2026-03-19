from __future__ import annotations

from datetime import datetime
import re
from dataclasses import dataclass
from html.parser import HTMLParser
from http.cookiejar import CookieJar
from urllib.parse import parse_qs, urlencode, urljoin, urlparse
from urllib.request import HTTPCookieProcessor, Request, build_opener
import xml.etree.ElementTree as ET

from .config import SourceConfig
from .html_links import Link, LinkExtractor


YEAR_TITLE_RE = re.compile(r"^\d{4}$")
MEETING_TITLE_RE = re.compile(r"^\d{2}-\d{2}-\d{2}\b")
FOLDER_ID_RE = re.compile(r"/fol/(\d+)/Row1\.aspx", re.IGNORECASE)


@dataclass(frozen=True)
class SnapshotArtifact:
    name: str
    extension: str
    content: bytes


@dataclass(frozen=True)
class LaserficheDiscovery:
    root_artifact: SnapshotArtifact
    year_artifacts: list[SnapshotArtifact]
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


def _extract_folder_id(url: str) -> str:
    match = FOLDER_ID_RE.search(urlparse(url).path)
    if not match:
        raise ValueError(f"Unable to determine Laserfiche folder id from URL: {url}")
    return match.group(1)


def _build_row1_url(folder_id: str) -> str:
    return f"https://lfportal.loudoun.gov/LFPortalinternet/0/fol/{folder_id}/Row1.aspx"


def _build_rss_url(folder_id: str) -> str:
    return f"https://lfportal.loudoun.gov/LFPortalinternet/rss/dbid/0/folder/{folder_id}/feed.rss"


def _rss_item_links(feed_url: str, rss_bytes: bytes) -> list[Link]:
    root = ET.fromstring(rss_bytes)
    links: list[Link] = []
    seen: set[str] = set()

    for item in root.findall("./channel/item"):
        title = (item.findtext("title") or "").strip()
        link_text = (item.findtext("link") or "").strip()
        if not title or not link_text:
            continue

        absolute_link = urljoin(feed_url, link_text)
        parsed = urlparse(absolute_link)
        query = parse_qs(parsed.query)
        start_ids = query.get("startid")
        if not start_ids:
            continue

        row1_url = _build_row1_url(start_ids[0])
        if row1_url in seen:
            continue
        seen.add(row1_url)
        links.append(Link(title=title, url=row1_url))

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

    def _discover_via_rss(self, source: SourceConfig) -> LaserficheDiscovery:
        settings = source.settings or {}
        year_count = int(settings.get("year_count", 1))
        root_folder_id = _extract_folder_id(source.url)
        root_rss_url = str(settings.get("root_rss_url") or _build_rss_url(root_folder_id))
        root_rss = self.fetch(root_rss_url)
        root_links = _rss_item_links(root_rss_url, root_rss)

        year_links = [link for link in root_links if YEAR_TITLE_RE.fullmatch(link.title)]
        year_links = sorted(year_links, key=lambda link: int(link.title), reverse=True)[:year_count]
        if not year_links:
            raise RuntimeError("Laserfiche root RSS did not expose any year folders.")

        year_artifacts: list[SnapshotArtifact] = []
        meetings: list[Link] = []
        seen_meeting_urls: set[str] = set()

        for year_link in year_links:
            year_folder_id = _extract_folder_id(year_link.url)
            year_rss_url = _build_rss_url(year_folder_id)
            year_rss = self.fetch(year_rss_url)
            year_artifacts.append(SnapshotArtifact(name=year_link.title, extension="rss", content=year_rss))
            for link in _rss_item_links(year_rss_url, year_rss):
                if MEETING_TITLE_RE.match(link.title) and link.url not in seen_meeting_urls:
                    seen_meeting_urls.add(link.url)
                    meetings.append(link)

        meetings.sort(key=_meeting_sort_key, reverse=True)
        return LaserficheDiscovery(
            root_artifact=SnapshotArtifact(name="root", extension="rss", content=root_rss),
            year_artifacts=year_artifacts,
            meetings=meetings,
        )

    def _discover_via_html(self, source: SourceConfig) -> LaserficheDiscovery:
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

        year_artifacts: list[SnapshotArtifact] = []
        meetings: list[Link] = []
        seen_meeting_urls: set[str] = set()

        for year_link in year_links:
            year_html = self.fetch(year_link.url)
            year_artifacts.append(SnapshotArtifact(name=year_link.title, extension="html", content=year_html))
            for link in _filter_row_links(year_link.url, year_html):
                if not MEETING_TITLE_RE.match(link.title):
                    continue
                if link.url in seen_meeting_urls:
                    continue
                seen_meeting_urls.add(link.url)
                meetings.append(link)

        meetings.sort(key=_meeting_sort_key, reverse=True)
        return LaserficheDiscovery(
            root_artifact=SnapshotArtifact(name="root", extension="html", content=root_html),
            year_artifacts=year_artifacts,
            meetings=meetings,
        )

    def discover_meetings(self, source: SourceConfig) -> LaserficheDiscovery:
        try:
            return self._discover_via_rss(source)
        except Exception:
            return self._discover_via_html(source)


def _meeting_sort_key(link: Link) -> tuple[datetime, str]:
    match = MEETING_TITLE_RE.match(link.title)
    if match:
        return (datetime.strptime(match.group(0), "%m-%d-%y"), link.title)
    return (datetime.min, link.title)
