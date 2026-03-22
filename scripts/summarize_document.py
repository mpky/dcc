from pathlib import Path
import argparse
import json
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from data_center_digest.summarizer import SummaryRequest, Summarizer


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Summarize one extracted document with Gemini or Ollama.")
    parser.add_argument("text_path", type=Path, help="Path to extracted text file.")
    parser.add_argument("--title", help="Override document title. Defaults to file stem.")
    parser.add_argument("--jurisdiction", default="Loudoun County, VA")
    parser.add_argument("--meeting-title")
    parser.add_argument("--source-url")
    parser.add_argument("--max-input-chars", type=int, default=24000)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    text = args.text_path.read_text(encoding="utf-8", errors="ignore")
    summarizer = Summarizer.from_env()
    result = summarizer.summarize(
        SummaryRequest(
            title=args.title or args.text_path.stem,
            text=text,
            jurisdiction=args.jurisdiction,
            meeting_title=args.meeting_title,
            source_url=args.source_url,
            max_input_chars=args.max_input_chars,
        )
    )
    print(
        json.dumps(
            {
                "backend": result.backend,
                "model": result.model,
                "summary": result.summary,
                "why_it_matters": result.why_it_matters,
                "topic_tags": result.topic_tags,
                "confidence": result.confidence,
                "next_watch": result.next_watch,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
