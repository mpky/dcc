from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


DEFAULT_MAX_INPUT_CHARS = 24000
DEFAULT_SUMMARY_SCHEMA = {
    "summary": "2-4 sentence factual summary of what the document is about",
    "why_it_matters": "1-3 sentence explanation of relevance to data center development, regulation, power, land use, or permitting",
    "topic_tags": ["list", "of", "short", "tags"],
    "confidence": "high|medium|low",
    "next_watch": "short note on what to watch next",
}


@dataclass(frozen=True)
class SummaryRequest:
    title: str
    text: str
    jurisdiction: str
    source_url: str | None = None
    meeting_title: str | None = None
    max_input_chars: int = DEFAULT_MAX_INPUT_CHARS


@dataclass(frozen=True)
class SummaryResult:
    backend: str
    model: str
    summary: str
    why_it_matters: str
    topic_tags: list[str]
    confidence: str
    next_watch: str
    raw_response: str


@dataclass(frozen=True)
class SummarizerConfig:
    backend: str
    model: str
    endpoint: str
    api_key: str | None = None
    request_timeout_seconds: int = 60


class SummarizerError(RuntimeError):
    pass


class Summarizer:
    def __init__(self, config: SummarizerConfig) -> None:
        self.config = config

    @classmethod
    def from_env(cls) -> "Summarizer":
        backend = os.environ.get("SUMMARY_BACKEND", "gemini").strip().lower()

        if backend == "gemini":
            return cls(
                SummarizerConfig(
                    backend="gemini",
                    model=os.environ.get("GEMINI_MODEL", "gemini-2.5-flash-lite"),
                    endpoint=os.environ.get(
                        "GEMINI_API_BASE",
                        "https://generativelanguage.googleapis.com/v1beta",
                    ),
                    api_key=os.environ.get("GEMINI_API_KEY"),
                    request_timeout_seconds=int(os.environ.get("SUMMARY_REQUEST_TIMEOUT_SECONDS", "60")),
                )
            )

        if backend == "ollama":
            return cls(
                SummarizerConfig(
                    backend="ollama",
                    model=os.environ.get("OLLAMA_MODEL", "gemma3:4b-it-qat"),
                    endpoint=os.environ.get("OLLAMA_API_BASE", "http://localhost:11434"),
                    request_timeout_seconds=int(os.environ.get("SUMMARY_REQUEST_TIMEOUT_SECONDS", "180")),
                )
            )

        raise SummarizerError(f"Unsupported SUMMARY_BACKEND: {backend}")

    def summarize(self, request: SummaryRequest) -> SummaryResult:
        prompt = build_summary_prompt(request)
        if self.config.backend == "gemini":
            payload = self._call_gemini(prompt)
        elif self.config.backend == "ollama":
            payload = self._call_ollama(prompt)
        else:
            raise SummarizerError(f"Unsupported backend: {self.config.backend}")

        parsed = _parse_summary_payload(payload)
        return SummaryResult(
            backend=self.config.backend,
            model=self.config.model,
            summary=parsed["summary"],
            why_it_matters=parsed["why_it_matters"],
            topic_tags=parsed["topic_tags"],
            confidence=parsed["confidence"],
            next_watch=parsed["next_watch"],
            raw_response=payload,
        )

    def _call_gemini(self, prompt: str) -> str:
        if not self.config.api_key:
            raise SummarizerError("GEMINI_API_KEY is required when SUMMARY_BACKEND=gemini")

        url = (
            f"{self.config.endpoint}/models/{self.config.model}:generateContent"
            f"?key={self.config.api_key}"
        )
        body = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {
                "temperature": 0.2,
                "responseMimeType": "application/json",
            },
        }
        response = _post_json(url, body, timeout_seconds=self.config.request_timeout_seconds)
        try:
            return response["candidates"][0]["content"]["parts"][0]["text"]
        except (KeyError, IndexError, TypeError) as exc:
            raise SummarizerError(f"Unexpected Gemini response shape: {response}") from exc

    def _call_ollama(self, prompt: str) -> str:
        url = f"{self.config.endpoint.rstrip('/')}/api/generate"
        body = {
            "model": self.config.model,
            "prompt": prompt,
            "stream": False,
            "format": "json",
            "options": {"temperature": 0.2},
        }
        response = _post_json(url, body, timeout_seconds=self.config.request_timeout_seconds)
        try:
            return response["response"]
        except KeyError as exc:
            raise SummarizerError(f"Unexpected Ollama response shape: {response}") from exc


def build_summary_prompt(request: SummaryRequest) -> str:
    trimmed_text = request.text[: request.max_input_chars].strip()
    metadata = {
        "title": request.title,
        "jurisdiction": request.jurisdiction,
        "meeting_title": request.meeting_title,
        "source_url": request.source_url,
    }
    return (
        "You summarize public-government documents for a legal-news digest about data centers.\n"
        "Return valid JSON only. Do not include markdown fences.\n"
        "Use only facts supported by the document text and metadata.\n"
        "Do not infer missing facts, motives, outcomes, or project details that are not stated.\n"
        "If relevance to data centers is indirect, say so explicitly.\n"
        "If the document text appears noisy, partial, or OCR-derived, lower confidence accordingly.\n"
        "Use this JSON shape:\n"
        f"{json.dumps(DEFAULT_SUMMARY_SCHEMA, indent=2)}\n\n"
        "Task guidance:\n"
        "- Focus on zoning, land use, utilities, substations, transmission, tax, permitting, and industrial development.\n"
        "- Prefer concrete government actions such as hearings, votes, ordinances, rezonings, special exceptions, staff recommendations, proffers, and utility approvals.\n"
        "- Ignore boilerplate, template language, contact blocks, generic agenda mechanics, and legal formalities unless they are central to the document.\n"
        "- `why_it_matters` should connect the document to data centers only when justified by the text.\n"
        "- `summary` should be 2-4 concise sentences focused on what the government action or proposal is.\n"
        "- `topic_tags` should be 3-6 short lower-case tags.\n"
        "- `confidence` should reflect both document clarity and how directly the text supports the summary.\n"
        "- `next_watch` should mention the next public process step when one is apparent; otherwise use a short watch-item note.\n"
        "- When the document is not clearly about data centers, say that plainly instead of stretching the connection.\n\n"
        f"Metadata:\n{json.dumps(metadata, indent=2)}\n\n"
        f"Document text:\n{trimmed_text}\n"
    )


def _parse_summary_payload(payload: str) -> dict[str, Any]:
    try:
        data = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise SummarizerError(f"Model did not return valid JSON: {payload[:500]}") from exc

    summary = str(
        data.get(
            "summary",
            data.get(
                "summaery",
                data.get(
                    "summaary",
                    data.get("answer", ""),
                ),
            ),
        )
    ).strip()
    if not summary:
        raise SummarizerError(f"Summary payload missing `summary`: {data}")

    topic_tags = data.get("topic_tags", data.get("keywords", []))
    if not isinstance(topic_tags, list):
        raise SummarizerError(f"`topic_tags` must be a list: {data}")

    normalized_tags = [str(tag).strip() for tag in topic_tags if str(tag).strip()][:6]
    why_it_matters = str(data.get("why_it_matters", data.get("why_it_matteers", ""))).strip()
    if not why_it_matters:
        why_it_matters = _fallback_why_it_matters(summary, normalized_tags)

    confidence = str(data.get("confidence", "")).strip().lower()
    if not confidence:
        confidence = _fallback_confidence(summary, normalized_tags)

    next_watch = str(data.get("next_watch", "")).strip()
    if not next_watch:
        next_watch = _fallback_next_watch(data)

    return {
        "summary": summary,
        "why_it_matters": why_it_matters,
        "topic_tags": normalized_tags,
        "confidence": confidence,
        "next_watch": next_watch,
    }


def _fallback_why_it_matters(summary: str, topic_tags: list[str]) -> str:
    if "data center" in summary.casefold() or any("data center" in tag.casefold() for tag in topic_tags):
        return "This appears directly relevant because the document concerns land-use or approval steps for a data center project."
    if topic_tags:
        return f"This appears potentially relevant because it touches land-use or infrastructure topics such as {', '.join(topic_tags[:3])}."
    return "This may be relevant if it affects land use, infrastructure, or approvals tied to data center development."


def _fallback_confidence(summary: str, topic_tags: list[str]) -> str:
    direct_terms = {"data center", "substation", "transmission line", "zoning amendment", "special exceptions"}
    haystack = f"{summary} {' '.join(topic_tags)}".casefold()
    if any(term in haystack for term in direct_terms):
        return "medium"
    return "low"


def _fallback_next_watch(data: dict[str, Any]) -> str:
    details = data.get("details")
    if isinstance(details, list):
        for detail in details:
            if not isinstance(detail, dict):
                continue
            section = str(detail.get("section", "")).casefold()
            content = str(detail.get("content", "")).strip()
            if "timeline" in section and content:
                return content.split(". ")[0].strip()
    return "Watch the next public hearing, board action, staff report, or revised filing for this item."


def _post_json(url: str, body: dict[str, Any], timeout_seconds: int) -> dict[str, Any]:
    payload = json.dumps(body).encode("utf-8")
    request = Request(
        url,
        data=payload,
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
        method="POST",
    )
    try:
        with urlopen(request, timeout=timeout_seconds) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")
        raise SummarizerError(f"HTTP error from summarizer backend: {exc.code} {detail}") from exc
    except URLError as exc:
        raise SummarizerError(f"Unable to reach summarizer backend: {exc}") from exc
