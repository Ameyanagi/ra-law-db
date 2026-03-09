# ra-law-db

Public dataset workspace and installable runtime library for Japanese chemical law screening.

`ra-law-db` is now the canonical non-MCP API for direct law-screening consumers.
`ra-law-mcp` is a thin wrapper over this package.

## Direct package usage

Most consumers should install the package and use the bundled SQLite database directly:

```bash
uv add git+https://github.com/Ameyanagi/ra-law-db.git
```

```bash
pip install "ra-law-db @ git+https://github.com/Ameyanagi/ra-law-db.git"
```

```python
from ra_law_db import get_law_screening_database

db = get_law_screening_database()

lookup_result = db.lookup(cas_number="75-09-2", language="ja")
search_result = db.search(query="ジクロロメタン", mode="auto", limit=10, min_score=0.6)
```

Public runtime API:

- `get_law_screening_database(law_db_path: str | None = None)`
- `LawScreeningDatabase.lookup(cas_number=None, substance_name=None, language="ja")`
- `LawScreeningDatabase.search(query, mode="auto", law_id=None, limit=20, min_score=0.6)`

Default behavior:

- If `law_db_path` is omitted, `ra-law-db` uses the packaged bundled SQLite database.
- `law_db_path` may optionally point to:
  - a direct SQLite file
  - a checked-out `ra-law-db` directory containing `regulatory.sqlite3` and compatibility artifacts

## Runtime artifact

The canonical runtime artifact is the bundled SQLite database:

- packaged path: `src/ra_law_db/data/regulatory.sqlite3`
- published repo artifact: `regulatory.sqlite3`

Normal installed consumers should rely on the packaged bundled database. CSV/JSONL files remain published for compatibility, inspection, and debugging, but they are not required for normal runtime use.

At runtime, the bundled database is resolved safely via `importlib.resources` and opened through a real filesystem path using `importlib.resources.as_file(...)`.

## MCP usage

`ra-law-mcp` consumes this package directly. In the normal case it does not need a separate `ra-law-db` checkout.

`RA_LAW_DB_PATH` is still supported as an override when you want to point the MCP server at:

- a custom SQLite bundle
- a local development checkout of `ra-law-db`

## Directory layout

- `regulatory.sqlite3`: published SQLite bundle
- `src/ra_law_db/data/regulatory.sqlite3`: packaged SQLite bundle used by installs
- `parsed/`: normalized parsed entries and source snapshot manifests
  - `source_snapshots.jsonl`
  - `law_entries.jsonl`
- `mappings/`: CAS mapping outputs and unresolved entries
  - `cas_mappings.jsonl`
  - `unresolved_entries.jsonl`
- `exports/`: compatibility export files
  - `regulatory_substances.csv`
- `masters/`: public-safe law master datasets and alias rows

## Generation and releases

This repository is published by the private `ra-law-scraper` pipeline. The public repo contains the generated dataset and compatibility exports, not the private scraping workflow itself.

Release model:

- the package version moves with the bundled SQLite database
- a new bundled DB refresh should be released as a new `ra-law-db` package version
- direct consumers receive data updates by upgrading `ra-law-db`

## Notes

- SQLite is the canonical runtime format for downstream consumers.
- Alias data is published in `masters/substance_aliases.csv` and bundled into `regulatory.sqlite3`.
- Unresolved law entries are tracked in `mappings/unresolved_entries.jsonl` for manual review.
