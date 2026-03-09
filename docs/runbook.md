# Law DB Update Runbook

## Goal

Refresh staged law artifacts and regenerate runtime-compatible regulatory CSV output.
See also `ra-law-db/docs/scraper_architecture.md` for code organization and extension boundaries.

## Prerequisites

- Repository root checkout
- Python environment with dependencies available
- Network access to e-Gov law API (or existing cached snapshots under `ra-law-db/raw/laws/`)

## Steps

1. Run the pipeline:

   ```bash
   python scripts/scrape_regulatory.py
   ```

2. Confirm artifact updates:

   - `ra-law-db/parsed/source_snapshots.jsonl`
   - `ra-law-db/parsed/law_entries.jsonl`
   - `ra-law-db/mappings/cas_mappings.jsonl`
   - `ra-law-db/mappings/unresolved_entries.jsonl`
   - `ra-law-db/exports/regulatory_substances.csv`
   - `ra-law-db/masters/substance_aliases.csv`
   - `ra-law-db/masters/cscl_substances.csv`
   - `ra-law-db/masters/poison_control_substances.csv`
   - `ra-law-db/masters/cwc_substances.csv`
   - `ra-law-db/masters/master_coverage.json`

3. Confirm compatibility outputs:

   - `cache/regulatory_substances.csv`
   - `ra-library/src/ra_library/data/regulatory_substances.csv`

4. Validate tests/lint:

   ```bash
   uv run pytest  # run from ra-library and ra-mcp dirs
   uv run ruff check
   ```

## Troubleshooting

- If e-Gov fetch fails, pipeline falls back to the newest local snapshot for each law ID.
- Review unresolved law names in `ra-law-db/mappings/unresolved_entries.jsonl` and add mapping logic if needed.
