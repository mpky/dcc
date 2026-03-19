# Source Inventory

This file records both active sources and plausible alternatives so source research does not get lost as the project evolves.

## Active now

### `loudoun_bos_laserfiche_root`

- Status: active
- Jurisdiction: Loudoun County, VA
- Scope: Board of Supervisors business meetings, public hearings, and special meetings
- URL: <https://lfportal.loudoun.gov/LFPortalinternet/0/fol/98907/Row1.aspx>
- Access pattern: root RSS for year discovery, year RSS for meeting discovery, HTML portal as fallback
- Why it matters: this is the cleanest source of actual meeting folders and packet PDFs for the first milestone
- Structure:
  - root folder
  - year folders
  - meeting folders
  - PDFs inside each meeting folder
- Current use:
  - discover newest year folders from root RSS
  - discover meeting folders from year RSS
  - fetch meeting-folder HTML when a new meeting appears
  - download PDFs linked from the new meeting folder
  - fall back to HTML crawling if RSS fails

## Candidate sources

### `loudoun_bos_meeting_documents_page`

- Status: candidate
- Jurisdiction: Loudoun County, VA
- Scope: top-level county page for Board of Supervisors meeting documents
- URL: <https://www.loudoun.gov/4829/Board-of-Supervisors-Meeting-Documents>
- Why considered: obvious official entry point for BOS materials
- Why not primary:
  - mostly a wrapper page
  - links to multiple downstream systems
  - direct scraping mostly returns navigation links rather than meeting-level records
- Likely future role:
  - human-facing reference page
  - fallback pointer if downstream source layout changes

### `loudoun_bos_packets_wrapper`

- Status: candidate
- Jurisdiction: Loudoun County, VA
- Scope: county page for BOS business meetings, public hearings, and special meetings
- URL: <https://www.loudoun.gov/3426/Board-of-Supervisors-Meetings-Packets>
- Why considered: more specific than the top-level BOS document page
- Why not primary:
  - embeds Laserfiche in an iframe
  - still acts mainly as a wrapper rather than the real record source
- Likely future role:
  - watchdog page to detect if the embedded source changes

### `loudoun_bos_granicus_archive`

- Status: candidate
- Jurisdiction: Loudoun County, VA
- Scope: webcast/archive system for meetings
- URL: <https://loudoun.granicus.com/ViewPublisher.php?view_id=14>
- Why considered: official archive with meeting names, agenda links, video, and RSS
- Strengths:
  - useful for archive metadata
  - likely useful for meeting dates, clips, minutes, and video references
- Why not primary for current milestone:
  - less direct path to packet documents
  - not as clean as Laserfiche for enumerating packet folders and underlying PDFs
- Likely future role:
  - supplement packet data with minutes and webcast/video links

### `loudoun_bos_laserfiche_folder_rss`

- Status: candidate
- Jurisdiction: Loudoun County, VA
- Scope: RSS for the Laserfiche root folder
- URL: <https://lfportal.loudoun.gov/LFPortalinternet/rss/dbid/0/folder/98907/feed.rss>
- Why considered: exposed directly by the Laserfiche folder page
- What it actually returns:
  - year folders like `2025` and `2026`
  - not individual meeting folders at the root level
- Current conclusion:
  - useful for detecting new year folders
  - not enough by itself for meeting-level monitoring
- Likely future role:
  - root-level change detection for year rollover
  - pointer to year-level RSS feeds

### `loudoun_bos_laserfiche_year_rss`

- Status: candidate
- Jurisdiction: Loudoun County, VA
- Scope: RSS for a specific year folder inside Laserfiche
- Example URL: <https://lfportal.loudoun.gov/LFPortalinternet/rss/dbid/0/folder/1966224/feed.rss>
- Why considered: year feed is more granular than the root feed
- What it actually returns:
  - individual meeting folders such as `03-17-26 Business Meeting`
  - useful `pubDate` values for when entries were added or updated in the folder
- Current conclusion:
  - this is a strong candidate for a simpler polling layer than HTML crawling
  - but it requires a way to discover or maintain the current year folder IDs
- Likely future role:
  - primary meeting-folder polling for the current year
  - HTML crawl reserved for discovering new year folders or fallback recovery

## Deferred but likely needed later

### `loudoun_planning_commission`

- Status: deferred
- Jurisdiction: Loudoun County, VA
- Scope: planning commission agendas, packets, hearings, and land-use actions
- Why likely needed:
  - zoning and land-use actions may matter more to data centers than BOS materials alone

### `loudoun_county_code_and_ordinances`

- Status: deferred
- Jurisdiction: Loudoun County, VA
- Scope: enacted county code and ordinance changes
- Why likely needed:
  - BOS packets show what is proposed or discussed
  - code/ordinance sources are better for what is actually adopted

### `virginia_general_assembly`

- Status: deferred
- Jurisdiction: Virginia
- Scope: state legislation and bill tracking
- Why likely needed:
  - state-level legal changes will eventually belong in the digest

## Source selection notes

- For the first milestone, prefer the source with the cleanest meeting-folder hierarchy over the source with the nicest landing page.
- Wrapper pages are still worth tracking in this inventory because they are often the first sign that the county changed vendors or moved documents.
- Candidate sources should stay documented even when not yet implemented.
