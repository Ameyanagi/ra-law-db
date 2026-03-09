# ra-law-db

Public dataset workspace for Japanese regulatory law artifacts.

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

## Notes

- SQLite is the preferred distribution format for downstream consumers.
- The compatibility CSV remains for consumers that have not migrated yet.
- Alias data is published in `masters/substance_aliases.csv` and bundled into `regulatory.sqlite3`.
- Unresolved law entries are tracked in `mappings/unresolved_entries.jsonl` for manual review.
