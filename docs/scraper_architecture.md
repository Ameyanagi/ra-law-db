# Dataset Architecture

This repository stores the public outputs of the private regulatory scraping pipeline.

## Primary artifact

- `regulatory.sqlite3`

The SQLite bundle contains the data needed by downstream screening services:

- regulatory export rows
- parsed law entries
- mapping records
- unresolved entries
- law master datasets
- public-safe alias rows
- master coverage metadata

## Compatibility artifacts

The repo also publishes compatibility files for existing consumers:

- `exports/regulatory_substances.csv`
- `parsed/*.jsonl`
- `mappings/*.jsonl`
- `masters/*.csv`

## Publishing rules

- SQLite is the source of truth for new consumers.
- Compatibility files are generated from the same sanitized dataset.
- Alias rows derived from non-public enrichment inputs are excluded from the public bundle.
