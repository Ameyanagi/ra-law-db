# ra-law-db

<p align="center">
  <img src="assets/logo.png" alt="RA Suite logo" width="180">
</p>

Public dataset workspace and installable runtime library for Japanese chemical law screening.

`ra-law-db` is now the canonical non-MCP API for direct law-screening consumers.
`ra-law-mcp` is a thin wrapper over this package.

## Direct package usage

Most consumers should install the package and use the bundled SQLite database directly:

```bash
pip install ra-law-db
```

```bash
uv add ra-law-db
```

```python
from ra_law_db import get_law_screening_database

db = get_law_screening_database()

lookup_result = db.lookup(
    cas_number="75-09-2",
    language="ja",
    context={
        "material_form": "powder",
        "work_process": "weighing and mixing",
        "dust_generation": "medium",
    },
)
search_result = db.search(query="ジクロロメタン", mode="auto", limit=10, min_score=0.6)
```

Public runtime API:

- `get_law_screening_database(law_db_path: str | None = None)`
- `LawScreeningDatabase.lookup(cas_number=None, substance_name=None, language="ja", context=None, percent=None, substances=None)`
- `LawScreeningDatabase.search(query, mode="auto", law_id=None, limit=20, min_score=0.6)`
- `LawScreeningDatabase.dataset_status()`

Mixture screening (v0.5): pass `percent` (content in the mixture, 0–100) to evaluate
concentration thresholds, or `substances=[{"cas_number": ..., "percent": ...}]` to screen
several components at once; the latter returns `{"results_by_cas": {cas: <lookup payload>}}`.

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
  - `regulatory_index.csv`: authoritative NITE-CHRIP CAS positive-list rows
    (v0.5 adds `class_source`, `special_management`, `special_organic`, `threshold_pct`,
    `threshold_note`, `legal_number`, `legal_name`, plus `control_concentration` /
    `control_concentration_unit` / `control_concentration_basis` (管理濃度, 作業環境評価基準 別表,
    on 特化則 第一・二類 / 有機則 第一・二種 / 鉛則 rows), `oel_8h` / `oel_stel` / `oel_effective`
    (濃度基準値, MHLW 一覧, on `occupational_exposure_limit` rows) and the 皮膚等障害化学物質 裾切値 in
    `threshold_pct` / `threshold_note` of `skin_protection` rows; older bundles without them still load)
    - `women_rules` rows (女性労働基準規則 第2条第1項第18号, every female worker per 第3条) are derived from
      the 特化則/鉛則/有機則 group names of the ish rows (`mapping_method: legal_group_name_join`, law id
      361M50002000003); categories `women_18_i` (特定化学物質等), `women_18_ro` (鉛), `women_18_ha` (有機溶剤).
      They inherit the source ordinance's `threshold_pct`; `legal_number` ending in `（２）のみ` marks
      substances covered only through the 第三管理区分 branch. v0.5.1: `lookup(context=...)` with
      `respirator_required_work` (bool) and `workplace_control_class` (1/2/3) resolves the result to
      `applies` (`WOMEN_RULE_WORK_COVERED`) or `not_applies` (`WOMEN_RULE_WORK_NOT_COVERED`)
    - v0.5.2: an explicit `dust_generation=false` (the L3 answer) takes precedence over inferred
      powder/process hints and resolves `dust_rule` to `not_applies` (`DUST_RULE_WORK_NOT_COVERED`)
  - `obligations.csv`: per-(law_code, category_code) duties in Japanese — 法定管理措置マトリクス
    (健診種類・周期・記録年数、測定周期・記録年数、作業記録年数、作業主任者、掲示) and per-law
    checklists; `category_code='*'` is the per-law fallback, `special_management` /
    `special_organic` are virtual categories attached when a matched index row carries the flag
  - `dataset_coverage.json`: source-level update, hash, load, and mapping coverage
  - `master_coverage.json`: law-domain aggregate coverage and negative-conclusion capability

The runtime reads obligations from the `obligations` SQLite table when the bundle has one,
else from `masters/obligations.csv` in a repo checkout, else from the packaged copy
(`src/ra_law_db/data/obligations.csv`, kept byte-identical by a test).

## Status semantics

- `applies`: an authoritative positive match establishes application.
- `requires_context`: the substance is listed, but concentration, quantity,
  use, process, business, or facility conditions are needed.
- `not_listed` (リスト非該当, reason `NOT_ON_POSITIVE_LIST`): the domain list is loaded,
  the query CAS is syntactically valid and unambiguous, and no row matches. Salts,
  isomers, mixtures and group designations (〜及びその化合物) still need a separate check.
- `not_applies`: used only when the screening dataset demonstrably supports a
  negative conclusion, an explicit authoritative negative exists, or every matched
  category has a `threshold_pct` and the given `percent` is at or below it
  (reason `BELOW_THRESHOLD`, with `threshold: {pct, note, below_threshold}`).
- `unknown`: no CAS candidate, dataset not loaded, or identity ambiguity (a name resolving
  to several CAS, or a CAS whose index rows carry a NITE identity note `(※)` in any law).

A CAS that is absent from the NITE-CHRIP index but present in the legacy 琉球大 layer
(`regulatory_substances`) is still reported as `requires_context` for `ish`; its categories
carry `class_source: "legacy_regulatory_substances"` and the notes say the class is
legacy-derived. When one CAS matches both an unresolved family code (`tokka`,
`organic_solvent`, `poison_control_listed`, `prtr_current`, `cwc_listed`,
`hazardous_material`, `controlled_narcotic`) and a resolved class of the same family, the
unresolved code is dropped from the obligations lookup (`flags.tokka_class_unresolved`
still reports that the index itself did not resolve it).

Category payloads (`categories[]`) expose the index columns above verbatim: `control_concentration`
(float) with its unit and 換算基準, `oel_8h` / `oel_stel` as normalised text (`"2 ppm"`, `"5 mg/m³"`) with
`oel_effective` as an ISO date, and `threshold_pct` / `threshold_note`. `None` means the statute sets no
value for that row (e.g. 第三類物質 have no 管理濃度), not "unknown".

Category rows that differ only by 政令番号 (e.g. 硫酸 listed twice as 劇物) are merged into one item
carrying `legal_numbers`; `narcotics` items are labelled 麻薬向精神薬原料 vs 麻薬・向精神薬本体 from the
指定政令 citation; `waste` items use the category code `waste_listed`. For process-decided laws
(`dust_rule`, `occupational_health`, `waste`) an `unknown` result's `required_actions` ask for the
work or waste conditions rather than for the CAS (v0.5.1).

Each result also carries `polarity` (`regulated_list`, or `permitted_list` for the
food-contact positive list), `required_actions[]` built from `masters/obligations.csv`
(`{action_code, label, required, kind, basis, condition, owner_hint}`), a `management`
object (`health_checks`, `measurements`, `work_records`, `supervisors`, `notices`, each with
`basis` and `kind`), `required_context_items[]` (`{key, label_ja, label_en}`), ISH `flags`
derived from index categories (with `flags_source` = `index` or
`legacy_regulatory_substances`), `class_sources` (matched category code → `class_source`),
and `hard_duty`. Every `required_actions[]` item carries all seven keys; generic guidance
items (unknown / not_listed / not_applies) use `kind: "info"`. The payload adds `summary`
(status counts) and `hard_duty_laws` (laws whose matched class carries a mandatory
obligation: 特化則第一・二類, 有機則第一・二種, 鉛, 製造禁止・許可, 毒劇, CWC Schedule 1, 麻薬・向精神薬本体).

The narcotics index publishes one code (`controlled_narcotic`) for 麻薬・向精神薬 and for
麻薬向精神薬原料 alike, so the runtime resolves it from `legal_number`: 法別表第1〜3 /
指定政令第1〜4条 → virtual category `narcotic_scheduled` (mandatory licence duties, hard duty);
法別表第4 / 指定政令第5条 → `narcotic_precursor` (営業届出・輸出入届出・記録 only, e.g. acetone,
sulfuric acid); rows without a legal number keep the `controlled_narcotic` rows, which are
`conditional`.

No dataset hit is not automatically a legal non-applicability finding. Results
are screening output, not final legal determinations. Each result exposes its
status reason, notes, dataset name/version/load state/coverage, source/update
date, evidence, and manual-verification actions.

The runtime also returns process-context screening for the Dust Ordinance,
Pneumoconiosis Act, special health examinations, and pneumoconiosis medical
examinations. These cannot be determined from CAS alone; provide material form,
work process, dust generation, work frequency, assignment history, and facility
conditions.

## Generation and releases

This repository is published by the private `ra-law-scraper` pipeline. The public repo contains the generated dataset and compatibility exports, not the private scraping workflow itself.

Release model:

- the package version moves with the bundled SQLite database
- a new bundled DB refresh should be released as a new `ra-law-db` package version
- direct consumers receive data updates by upgrading `ra-law-db`
- the private scraper performs a scheduled official-source refresh and opens a
  review PR; schema drift or missing core snapshots stops release generation

## Development

```bash
uv sync --group dev
uv run pre-commit install
uv run pre-commit run --all-files
uv run pytest -q
```

## Release

PyPI publishing is handled by GitHub Actions only when a matching `v*` tag is
pushed. See [docs/release.md](docs/release.md) for the cleanup and release
checklist.

## Notes

- SQLite is the canonical runtime format for downstream consumers.
- Alias data is published in `masters/substance_aliases.csv` and bundled into `regulatory.sqlite3`.
- Unresolved law entries are tracked in `mappings/unresolved_entries.jsonl` for manual review.
- `ra-law-db` remains law-source-driven. It is not replaced by CREATE-SIMPLE workbook data when `ra-library` / `ra-mcp` adopt a newer CREATE-SIMPLE methodology version.
