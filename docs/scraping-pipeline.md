# Public Dataset Pipeline

`ra-law-db` is the public output repository for Japanese regulatory law data.

## What is published

- `regulatory.sqlite3`: primary SQLite bundle for downstream consumers
- `exports/regulatory_substances.csv`: compatibility export for existing readers
- `parsed/`: parsed law-entry artifacts
- `mappings/`: CAS mapping and unresolved-entry artifacts
- `masters/`: law-specific master datasets and aliases

## What is not published here

- Private scraping orchestration
- Sensitive enrichment inputs
- Source-material copies that are not intended for redistribution

## Generation model

The private `ra-law-scraper` repository:

1. fetches and snapshots law sources
2. parses and normalizes entries
3. resolves CAS mappings
4. builds the public SQLite bundle and compatibility exports
5. publishes outputs into this repository

## Compatibility

SQLite is the preferred format. The CSV/JSONL artifacts remain for readers that have not migrated yet.
