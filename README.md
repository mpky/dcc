# Data Center Ceramics

Initial scope: monitor one official Loudoun County government source, detect newly posted meeting-related items, and persist them locally.

## First milestone

Track the Loudoun County Board of Supervisors meeting-documents page:

- source page snapshot saved on each run
- extracted links stored in SQLite
- new links reported on stdout

This is intentionally smaller than keyword filtering, summarization, or email delivery. The immediate goal is proving that one source is stable and can be monitored idempotently.

## Chosen source

- Loudoun County Board of Supervisors Meeting Documents
- <https://www.loudoun.gov/4829/Board-of-Supervisors-Meeting-Documents>

This page is an official Loudoun County page that points to current meeting packets, committee materials, and archives. It is a reasonable first ingestion target before moving to deeper agenda pages or Granicus views.

## Project layout

- `config/sources.json`: source registry
- `data/`: local runtime artifacts
- `src/data_center_digest/`: application code

## Local run

```bash
python3 scripts/run_once.py
```

Optional arguments:

```bash
python3 scripts/run_once.py --source-id loudoun_bos_meeting_documents
python3 scripts/run_once.py --db-path data/app.db --data-dir data
```

## What comes next

After this baseline works:

1. tighten extraction for the chosen source so navigation links are excluded
2. add keyword relevance filtering
3. add item-body downloads for PDFs and agenda pages
4. add email notifications
