# Repository Guidelines

## Project Structure & Module Organization

Core application code lives in `src/data_center_digest/`. The main pipeline entrypoint is `src/data_center_digest/run_once.py`, with source-specific logic in `laserfiche.py`, PDF extraction in `pdf_text.py`, relevance scoring in `relevance.py`, and persistence in `db.py`. CLI bootstrapping lives in `scripts/run_once.py`. Source configuration is in `config/sources.json`, and source research notes belong in `docs/source_inventory.md`. Runtime artifacts are written under `data/` (`raw/`, `items/`, `text/`) and should not be committed.

## Build, Test, and Development Commands

- `uv run python scripts/run_once.py`
  Runs the full one-source ingestion pipeline.
- `uv run python scripts/run_once.py --source-id loudoun_bos_meeting_documents --document-download-limit 1`
  Runs a smaller sample against one meeting folder.
- `uv run python -m py_compile src/data_center_digest/*.py scripts/run_once.py`
  Fast syntax check for all Python modules.
- `uv add <package>`
  Add new Python dependencies and update `uv.lock`.

## Coding Style & Naming Conventions

Use Python 3.11+ with 4-space indentation and type hints where practical. Keep modules focused and procedural; this repo currently favors small helpers over deep class hierarchies. Use `snake_case` for functions, variables, files, and SQLite column names. Prefer ASCII filenames and deterministic path generation. Keep comments short and only where logic is not obvious.

## Testing Guidelines

There is no formal test suite yet. For now, validate changes with:
- `uv run python -m py_compile ...`
- a scoped pipeline run using `--document-download-limit 1`
- direct SQLite inspection, for example:
  `sqlite3 /tmp/example.db 'select count(*) from document_relevance;'`

When adding tests, place them under `tests/` and name them `test_<feature>.py`.

## Commit & Pull Request Guidelines

Follow the existing commit style: short, imperative, sentence-case summaries such as `Add PDF text extraction pipeline`. Do not commit directly to `main`; create a feature branch first, e.g. `git checkout -b feat/pdf-text-extraction`. PRs should describe the pipeline change, note any new dependencies or schema changes, and include the exact validation commands you ran.

## Security & Configuration Tips

Only use official public-government sources unless explicitly expanding scope. Do not commit secrets, API keys, or generated `data/` artifacts. Keep source decisions documented in `docs/source_inventory.md` so future contributors can distinguish active sources from rejected candidates.
