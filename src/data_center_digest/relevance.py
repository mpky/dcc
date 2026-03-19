from __future__ import annotations

import json
import re
from dataclasses import dataclass


@dataclass(frozen=True)
class Rule:
    category: str
    label: str
    pattern: re.Pattern[str]
    weight: int


@dataclass(frozen=True)
class Match:
    category: str
    label: str
    weight: int
    start: int
    end: int


@dataclass(frozen=True)
class RelevanceResult:
    is_relevant: bool
    score: int
    categories: list[str]
    matched_terms: list[str]
    rationale: str
    matches_json: str


RULES = [
    Rule("direct", "data center", re.compile(r"\bdata[\s-]?center(s)?\b", re.IGNORECASE), 5),
    Rule("direct", "datacenter", re.compile(r"\bdatacenter(s)?\b", re.IGNORECASE), 5),
    Rule("direct", "server farm", re.compile(r"\bserver farm(s)?\b", re.IGNORECASE), 5),
    Rule("direct", "colocation", re.compile(r"\bcolo(cation)?\b", re.IGNORECASE), 4),
    Rule("direct", "digital campus", re.compile(r"\bdigital campus\b", re.IGNORECASE), 4),
    Rule("power", "substation", re.compile(r"\bsubstation(s)?\b", re.IGNORECASE), 3),
    Rule("power", "transmission line", re.compile(r"\btransmission line(s)?\b", re.IGNORECASE), 3),
    Rule("power", "electric transmission", re.compile(r"\belectric transmission\b", re.IGNORECASE), 3),
    Rule("power", "distribution line", re.compile(r"\bdistribution line(s)?\b", re.IGNORECASE), 2),
    Rule("power", "megawatt", re.compile(r"\bmegawatt(s)?\b|\bMW\b", re.IGNORECASE), 2),
    Rule("power", "dominion energy", re.compile(r"\bdominion energy\b", re.IGNORECASE), 2),
    Rule("land_use", "zoning ordinance amendment", re.compile(r"\bzoning ordinance amendment\b", re.IGNORECASE), 4),
    Rule("land_use", "zoning map amendment", re.compile(r"\bzoning map amendment\b", re.IGNORECASE), 4),
    Rule("land_use", "special exception", re.compile(r"\bspecial exception\b", re.IGNORECASE), 3),
    Rule("land_use", "zoning modification", re.compile(r"\bzoning ordinance modification\b", re.IGNORECASE), 3),
    Rule("land_use", "land development", re.compile(r"\bland development\b", re.IGNORECASE), 2),
    Rule("land_use", "industrial park", re.compile(r"\bindustrial park\b", re.IGNORECASE), 2),
    Rule("land_use_code", "ZOAM", re.compile(r"\bZOAM\b", re.IGNORECASE), 1),
    Rule("land_use_code", "ZMAP", re.compile(r"\bZMAP\b", re.IGNORECASE), 1),
    Rule("land_use_code", "ZMOD", re.compile(r"\bZMOD\b", re.IGNORECASE), 1),
    Rule("land_use_code", "SPEX", re.compile(r"\bSPEX\b", re.IGNORECASE), 1),
    Rule("infra", "fiber", re.compile(r"\bfiber\b|\bfibre\b", re.IGNORECASE), 1),
    Rule("infra", "telecommunications", re.compile(r"\btelecommunications?\b", re.IGNORECASE), 1),
    Rule("infra", "generator", re.compile(r"\bgenerator(s)?\b", re.IGNORECASE), 1),
    Rule("infra", "cooling water", re.compile(r"\bcooling water\b", re.IGNORECASE), 2),
    Rule("infra", "wastewater", re.compile(r"\bwastewater\b", re.IGNORECASE), 2),
    Rule("infra", "water supply", re.compile(r"\bwater supply\b", re.IGNORECASE), 2),
    Rule("proxy", "server", re.compile(r"\bserver(s)?\b", re.IGNORECASE), 1),
    Rule("proxy", "campus", re.compile(r"\bcampus\b", re.IGNORECASE), 1),
]


def analyze_relevance(title: str, text: str) -> RelevanceResult:
    haystack = f"{title}\n{text}"
    matches: list[Match] = []
    seen_labels: set[tuple[str, str]] = set()

    for rule in RULES:
        match = rule.pattern.search(haystack)
        if not match:
            continue
        key = (rule.category, rule.label)
        if key in seen_labels:
            continue
        seen_labels.add(key)
        matches.append(
            Match(
                category=rule.category,
                label=rule.label,
                weight=rule.weight,
                start=match.start(),
                end=match.end(),
            )
        )

    score = sum(match.weight for match in matches)
    categories = sorted({match.category for match in matches})
    matched_terms = [match.label for match in matches]
    is_relevant = _is_relevant(title, text, matches, score)
    rationale = _rationale(matches, score, is_relevant)
    matches_json = json.dumps(
        [
            {
                "category": match.category,
                "label": match.label,
                "weight": match.weight,
                "start": match.start,
                "end": match.end,
            }
            for match in matches
        ]
    )

    return RelevanceResult(
        is_relevant=is_relevant,
        score=score,
        categories=categories,
        matched_terms=matched_terms,
        rationale=rationale,
        matches_json=matches_json,
    )


def _is_relevant(title: str, text: str, matches: list[Match], score: int) -> bool:
    categories = {match.category for match in matches}
    normalized_title = title.casefold()
    normalized_text = text.casefold()

    is_agenda = "agenda" in normalized_title
    has_land_use_glossary = "land development application definitions" in normalized_text

    if is_agenda and has_land_use_glossary and categories.issubset({"land_use", "land_use_code", "proxy"}):
        return False
    if any(match.category == "direct" for match in matches):
        return True
    if "power" in categories and "land_use" in categories:
        return True
    if score >= 5 and not categories.issubset({"land_use_code", "proxy"}):
        return True
    return False


def _rationale(matches: list[Match], score: int, is_relevant: bool) -> str:
    if not matches:
        return "No configured data-center-related terms matched."
    terms = ", ".join(match.label for match in matches[:5])
    if is_relevant:
        return f"Matched {terms}; score={score}."
    return f"Matched weak or indirect terms ({terms}); score={score}."
