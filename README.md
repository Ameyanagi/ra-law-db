# ra-law-db

Public dataset workspace for Japanese regulatory law artifacts.

This repo now also ships a small Python package, `ra_law_db`, which exposes the
shared screening/search runtime used by downstream consumers such as
`ra-law-mcp`.

## Directory layout

- `regulatory.sqlite3`: bundled SQLite database for downstream consumers
- `parsed/`: normalized parsed entries and source snapshot manifests
  - `source_snapshots.jsonl`
  - `law_entries.jsonl`
- `mappings/`: CAS mapping outputs and unresolved entries
  - `cas_mappings.jsonl`
  - `unresolved_entries.jsonl`
- `exports/`: compatibility export files
  - `regulatory_substances.csv`
- `masters/`: public-safe law master datasets and alias rows

## Generation

This repository is published by the private `ra-law-scraper` pipeline. The public repo contains the generated dataset and compatibility exports, not the private scraping workflow itself.

## Package

- Install with `uv` from GitHub or as a local editable package.
- Runtime entrypoints:
  - `ra_law_db.LawScreeningDatabase`
  - `ra_law_db.get_law_screening_database()`

## Notes

- SQLite is the preferred distribution format for downstream consumers.
- The compatibility CSV remains for consumers that have not migrated yet.
- Alias data is published in `masters/substance_aliases.csv` and bundled into `regulatory.sqlite3`.
- Unresolved law entries are tracked in `mappings/unresolved_entries.jsonl` for manual review.
