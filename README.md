# Data Center Ceramics

Initial scope: monitor one official Loudoun County government source, detect newly posted Board of Supervisors meeting entries, and persist them locally.

## First milestone

Track the Loudoun County Board of Supervisors business-meeting packet repository:

- source feed snapshots saved on each run
- extracted meeting folders stored in SQLite
- PDFs from newly discovered meeting folders downloaded locally
- new meeting entries reported on stdout

This is intentionally smaller than keyword filtering, summarization, or email delivery. The immediate goal is proving that one source is stable and can be monitored idempotently.

## Chosen source

- Loudoun County Laserfiche repository for Board of Supervisors business meetings, public hearings, and special meetings
- Root folder: <https://lfportal.loudoun.gov/LFPortalinternet/0/fol/98907/Row1.aspx>

The public Loudoun page embeds this Laserfiche folder. The scraper now uses the Laserfiche root RSS feed to discover recent year folders, the year-level RSS feeds to discover actual meeting folders such as `03-17-26 Business Meeting`, and falls back to the HTML portal if the RSS path fails.

## Project layout

- `config/sources.json`: source registry
- `docs/source_inventory.md`: active and candidate source record
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

1. add keyword relevance filtering
2. add item-body parsing for PDFs
3. add email notifications
