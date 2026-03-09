"""Tests for the shared law-screening runtime package."""

from __future__ import annotations

import csv
import json
from pathlib import Path

from ra_law_db import LawScreeningDatabase


def _write_regulatory_export(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "cas_number",
        "name_ja",
        "name_en",
        "regulation_type",
        "regulation_class",
        "regulation_label",
        "law_name_ja",
        "law_name_en",
        "special_management",
        "special_organic",
        "carcinogen",
        "health_check_required",
        "health_check_type",
        "health_check_interval",
        "health_check_ref",
        "control_concentration",
        "control_concentration_unit",
        "threshold_pct",
        "record_retention_years",
        "work_env_measurement_required",
    ]
    rows = [
        {
            "cas_number": "75-09-2",
            "name_ja": "ジクロロメタン",
            "name_en": "Methylene chloride",
            "regulation_type": "prtr1",
            "regulation_class": 0,
            "regulation_label": "化管法 第一種指定化学物質",
            "law_name_ja": "化学物質排出把握管理促進法 第一種指定化学物質",
            "law_name_en": "PRTR Act First Class Designated Chemical Substance",
            "special_management": "False",
            "special_organic": "False",
            "carcinogen": "False",
            "health_check_required": "False",
            "health_check_type": "",
            "health_check_interval": "",
            "health_check_ref": "化管法施行令 412CO0000000138 別表第一",
            "control_concentration": "",
            "control_concentration_unit": "",
            "threshold_pct": "",
            "record_retention_years": 5,
            "work_env_measurement_required": "False",
        },
        {
            "cas_number": "75-09-2",
            "name_ja": "ジクロロメタン",
            "name_en": "Methylene chloride",
            "regulation_type": "tokka",
            "regulation_class": 2,
            "regulation_label": "特化則第2類",
            "law_name_ja": "特定化学物質障害予防規則",
            "law_name_en": "Ordinance on Prevention of Hazards Due to Specified Chemical Substances",
            "special_management": "False",
            "special_organic": "False",
            "carcinogen": "False",
            "health_check_required": "True",
            "health_check_type": "特定化学物質健康診断",
            "health_check_interval": "6ヶ月以内ごとに1回",
            "health_check_ref": "令22-2-15の3",
            "control_concentration": "",
            "control_concentration_unit": "",
            "threshold_pct": "",
            "record_retention_years": 30,
            "work_env_measurement_required": "True",
        },
    ]

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _write_jsonl(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record, ensure_ascii=False))
            f.write("\n")


def _write_master_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["cas_number", "category", "label_ja", "label_en", "law_name_ja", "law_name_en", "law_reference", "law_id"],
        )
        writer.writeheader()
        writer.writerows(rows)


def _write_alias_master(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["cas_number", "alias_name", "normalized_alias", "source_type", "source_ref"],
        )
        writer.writeheader()
        writer.writerow(
            {
                "cas_number": "75-09-2",
                "alias_name": "塩化メチレン",
                "normalized_alias": "塩化メチレン",
                "source_type": "fixture",
                "source_ref": "test",
            }
        )


def _prepare_fixture(tmp_path: Path) -> Path:
    law_db_path = tmp_path / "ra-law-db"
    _write_regulatory_export(law_db_path / "exports" / "regulatory_substances.csv")
    _write_alias_master(law_db_path / "masters" / "substance_aliases.csv")
    _write_master_csv(
        law_db_path / "masters" / "cscl_substances.csv",
        [
            {
                "cas_number": "75-09-2",
                "category": "priority_evaluation",
                "label_ja": "優先評価化学物質",
                "label_en": "Priority Assessment Chemical Substance",
                "law_name_ja": "化学物質の審査及び製造等の規制に関する法律",
                "law_name_en": "Act on the Regulation of Manufacture and Evaluation of Chemical Substances",
                "law_reference": "化審法 優先評価化学物質",
                "law_id": "000CSCL000000001",
            }
        ],
    )
    _write_master_csv(
        law_db_path / "masters" / "poison_control_substances.csv",
        [
            {
                "cas_number": "75-09-2",
                "category": "deleterious",
                "label_ja": "劇物",
                "label_en": "Deleterious Substance",
                "law_name_ja": "毒物及び劇物取締法",
                "law_name_en": "Poisonous and Deleterious Substances Control Act",
                "law_reference": "毒劇法別表",
                "law_id": "000POIS000000001",
            }
        ],
    )
    _write_jsonl(
        law_db_path / "parsed" / "source_snapshots.jsonl",
        [
            {
                "law_id": "412CO0000000138",
                "source_url": "https://laws.e-gov.go.jp/api/1/lawdata/412CO0000000138",
                "fetched_at": "2026-02-22T01:38:38Z",
                "content_hash": "hash-prtr",
            }
        ],
    )
    _write_jsonl(
        law_db_path / "parsed" / "law_entries.jsonl",
        [
            {
                "entry_id": "entry-1",
                "law_id": "412CO0000000138",
                "category": "第一種",
                "raw_name": "メチレンクロライド",
                "normalized_name": "メチレンクロライド",
            }
        ],
    )
    _write_jsonl(
        law_db_path / "mappings" / "cas_mappings.jsonl",
        [
            {
                "entry_id": "entry-1",
                "law_id": "412CO0000000138",
                "category": "第一種",
                "regulation_type": "prtr1",
                "cas_number": "75-09-2",
                "match_method": "alias",
                "confidence": 0.95,
            }
        ],
    )
    _write_jsonl(
        law_db_path / "mappings" / "unresolved_entries.jsonl",
        [
            {
                "law_id": "412CO0000000138",
                "category": "第一種",
                "raw_name": "アルキルフェノール",
                "normalized_name": "アルキルフェノール",
            }
        ],
    )
    return law_db_path


def test_search_returns_alias_match(tmp_path):
    """Search should resolve aliases from the shared dataset runtime."""
    law_db_path = _prepare_fixture(tmp_path)
    LawScreeningDatabase.reset_instance()
    db = LawScreeningDatabase.get_instance(law_db_path)

    payload = db.search(query="塩化メチレ", mode="name", limit=5, min_score=0.6)

    assert payload["substance_hits"][0]["cas_number"] == "75-09-2"
    assert "alias_master" in payload["substance_hits"][0]["match_sources"]


def test_lookup_returns_multi_law_statuses(tmp_path):
    """Lookup should assemble domain results from the shared runtime package."""
    law_db_path = _prepare_fixture(tmp_path)
    LawScreeningDatabase.reset_instance()
    db = LawScreeningDatabase.get_instance(law_db_path)

    payload = db.lookup(cas_number="75-09-2", language="ja")
    by_law = {item["law_code"]: item for item in payload["results"]}

    assert payload["matched"] is True
    assert by_law["cscl"]["status"] == "applies"
    assert by_law["poison_control"]["status"] == "applies"
    assert by_law["prtr"]["status"] == "applies"
    assert by_law["ish"]["status"] == "requires_context"
