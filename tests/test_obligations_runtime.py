"""v0.5 runtime contract: obligations, not_listed, thresholds, management, flags, summary."""

from __future__ import annotations

import csv
import json
import sqlite3
from importlib import resources
from pathlib import Path

import pytest

from ra_law_db import LawScreeningDatabase, __version__
from ra_law_db.obligations import apply_legacy_cas_override, build_management, load_obligations_csv

REPO_ROOT = Path(__file__).resolve().parents[1]

INDEX_COLUMNS = [
    "law_code",
    "category_code",
    "category_label_ja",
    "category_label_en",
    "cas_number",
    "substance_name",
    "chrip_id",
    "identity_note",
    "law_name_ja",
    "law_name_en",
    "law_id",
    "law_reference",
    "dataset_name",
    "dataset_version",
    "source_id",
    "source_url",
    "source_update_date",
    "snapshot_date",
    "mapping_method",
    "confidence",
    "required_context",
]
NEW_INDEX_COLUMNS = [
    "class_source",
    "special_management",
    "special_organic",
    "threshold_pct",
    "threshold_note",
    "legal_number",
    "legal_name",
]
OBLIGATION_COLUMNS = [
    "law_code",
    "category_code",
    "seq",
    "kind",
    "label_ja",
    "label_en",
    "basis_ja",
    "condition_ja",
    "threshold_note",
    "owner_hint",
    "health_check_type",
    "health_check_interval",
    "health_record_years",
    "measurement_interval",
    "measurement_record_years",
    "work_record_years",
    "supervisor_ja",
    "notice_ja",
]


def _index_row(law_code: str, category_code: str, label_ja: str, cas: str, name: str, **extra) -> dict:
    row = {
        "law_code": law_code,
        "category_code": category_code,
        "category_label_ja": label_ja,
        "category_label_en": category_code,
        "cas_number": cas,
        "substance_name": name,
        "chrip_id": "",
        "identity_note": "",
        "law_name_ja": "",
        "law_name_en": "",
        "law_id": "347AC0000000057",
        "law_reference": "NITE-CHRIP RJ_04_031",
        "dataset_name": "NITE-CHRIP official law lists",
        "dataset_version": "nite-chrip-test",
        "source_id": "RJ_04_031",
        "source_url": "https://www.chem-info.nite.go.jp/",
        "source_update_date": "2026-07-31",
        "snapshot_date": "2026-08-28",
        "mapping_method": "official_cas",
        "confidence": "1.0",
        "required_context": json.dumps(["concentration", "work_process"]),
    }
    row.update(extra)
    return row


INDEX_ROWS = [
    _index_row(
        "ish",
        "tokka_class_2",
        "特化則 第二類物質",
        "75-09-2",
        "ジクロロメタン",
        class_source="legal_row_join",
        special_management="1",
        special_organic="1",
        threshold_pct="1",
        threshold_note="1 %以下を含有するものを除く",
        legal_number="別表第3第2号",
        legal_name="ジクロロメタン",
    ),
    _index_row(
        "ish", "label_sds", "ラベル表示・SDS交付義務対象物質", "75-09-2", "ジクロロメタン", class_source="source_list"
    ),
    _index_row("ish", "manufacture_permission", "製造許可対象物質", "1336-36-3", "PCB", class_source="source_list"),
    _index_row("ish", "tokka", "特定化学物質等（特化則）", "1336-36-3", "PCB", class_source="unresolved"),
    _index_row(
        "poison_control",
        "deleterious",
        "劇物",
        "7664-93-9",
        "硫酸",
        class_source="legal_row_join",
        threshold_pct="10",
        threshold_note="10 %以下を含有するものを除く",
    ),
    _index_row(
        "food_contact", "food_contact_base_material", "食品接触材料", "7732-18-5", "水", class_source="source_list"
    ),
    # Unresolved index class for hydrochloric acid; the legacy layer knows it as 特化則第3類 (see tests below).
    _index_row("ish", "tokka", "特定化学物質等（特化則）", "7647-01-0", "塩酸", class_source="unresolved"),
    # NITE identity note: CAS-level identity is ambiguous for every law, not only cscl.
    _index_row(
        "cscl",
        "class_i_specified",
        "第一種特定化学物質",
        "104948-36-9",
        "PFOS塩",
        identity_note="(※1)",
        class_source="source_list",
    ),  # fmt: skip
    # Narcotics: one index code for 本体 and 原料; the runtime resolves it from legal_number.
    _index_row(
        "narcotics",
        "controlled_narcotic",
        "麻薬・向精神薬等",
        "67-64-1",
        "アセトン",
        class_source="source_list",
        legal_number="法別表第4第1号",
    ),  # fmt: skip
    _index_row(
        "narcotics",
        "controlled_narcotic",
        "麻薬・向精神薬等",
        "57-27-2",
        "モルヒネ",
        class_source="source_list",
        legal_number="法別表第1第13号",
    ),  # fmt: skip
    _index_row(
        "narcotics",
        "controlled_narcotic",
        "麻薬・向精神薬等",
        "1095-90-5",
        "メサドン塩酸塩",
        class_source="source_list",
    ),
]

LEGACY_COLUMNS = [
    "cas_number",
    "name_ja",
    "name_en",
    "regulation_type",
    "regulation_class",
    "regulation_label",
    "special_management",
    "health_check_required",
    "health_check_type",
    "health_check_interval",
    "record_retention_years",
]
LEGACY_ROWS = [
    {
        "cas_number": "7791-20-2",
        "name_ja": "ニッケル化合物（塩化ニッケル）",
        "regulation_type": "tokka",
        "regulation_class": 2,
        "regulation_label": "特化則第2類",
        "special_management": "True",
        "health_check_required": "True",
        "health_check_type": "特定化学物質健康診断",
        "health_check_interval": "6か月以内ごと",
        "record_retention_years": 30,
    },
    {
        "cas_number": "7647-01-0",
        "name_ja": "塩酸",
        "regulation_type": "tokka",
        "regulation_class": 3,
        "regulation_label": "特化則第3類",
        "special_management": "False",
        "health_check_required": "False",
    },
]

OBLIGATION_ROWS = [
    {
        "law_code": "ish",
        "category_code": "*",
        "seq": 1,
        "kind": "mandatory",
        "label_ja": "安衛法の区分を確認する",
        "basis_ja": "安衛法57",
    },
    {
        "law_code": "ish",
        "category_code": "tokka_class_2",
        "seq": 1,
        "kind": "mandatory",
        "label_ja": "特定化学物質健康診断を6か月以内ごとに実施する",
        "label_en": "Health check every 6 months",
        "basis_ja": "特化則39",
        "owner_hint": "産業保健",
        "health_check_type": "特定化学物質健康診断",
        "health_check_interval": "6か月以内ごと",
        "health_record_years": 5,
        "measurement_interval": "6か月以内ごと",
        "measurement_record_years": 3,
        "supervisor_ja": "特定化学物質作業主任者",
    },
    {
        "law_code": "ish",
        "category_code": "special_management",
        "seq": 1,
        "kind": "mandatory",
        "label_ja": "特別管理物質の掲示と記録30年",
        "basis_ja": "特化則38条の3",
        "health_check_type": "特定化学物質健康診断",
        "health_record_years": 30,
        "work_record_years": 30,
        "notice_ja": "特別管理物質の掲示",
    },
    {
        "law_code": "ish",
        "category_code": "label_sds",
        "seq": 1,
        "kind": "conditional",
        "label_ja": "濃度基準値を確認する",
        "basis_ja": "安衛則577条の2",
        "condition_ja": "屋内作業場",
    },
    {
        "law_code": "ish",
        "category_code": "manufacture_permission",
        "seq": 1,
        "kind": "mandatory",
        "label_ja": "製造許可を得る",
        "basis_ja": "安衛法56",
    },
    {
        "law_code": "ish",
        "category_code": "tokka",
        "seq": 1,
        "kind": "mandatory",
        "label_ja": "特化則の区分を確認する",
        "basis_ja": "安衛令別表第3",
    },
    {
        "law_code": "ish",
        "category_code": "tokka_class_3",
        "seq": 1,
        "kind": "conditional",
        "label_ja": "特定化学設備がある場合は特定化学物質作業主任者を選任する",
        "basis_ja": "特化則27",
        "condition_ja": "特定化学設備がある場合",
        "supervisor_ja": "特定化学物質作業主任者",
    },
    {
        "law_code": "poison_control",
        "category_code": "deleterious",
        "seq": 1,
        "kind": "mandatory",
        "label_ja": "「医薬用外劇物」を表示する",
        "basis_ja": "毒劇法12",
    },
    {
        "law_code": "narcotics",
        "category_code": "controlled_narcotic",
        "seq": 1,
        "kind": "conditional",
        "label_ja": "研究用途は麻薬研究者免許を得る",
        "basis_ja": "麻向法3",
        "condition_ja": "麻薬・向精神薬本体の場合",
    },
    {
        "law_code": "narcotics",
        "category_code": "narcotic_scheduled",
        "seq": 1,
        "kind": "mandatory",
        "label_ja": "研究用途は麻薬研究者免許を得る",
        "basis_ja": "麻向法3",
    },
    {
        "law_code": "narcotics",
        "category_code": "narcotic_precursor",
        "seq": 1,
        "kind": "conditional",
        "label_ja": "輸出入時は届け出る",
        "basis_ja": "麻向法第4章の3",
        "condition_ja": "輸出入する場合",
    },
    {
        "law_code": "poison_control",
        "category_code": "*",
        "seq": 1,
        "kind": "info",
        "label_ja": "区分を確認する",
        "basis_ja": "毒劇法2",
    },
    {
        "law_code": "food_contact",
        "category_code": "*",
        "seq": 1,
        "kind": "info",
        "label_ja": "研究室では原則対象外",
        "basis_ja": "食品衛生法18",
    },
]


def _write_csv(path: Path, columns: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in columns})


def _coverage(law_codes: list[str]) -> dict:
    return {
        law_code: {
            "dataset_loaded": True,
            "dataset_name": "NITE-CHRIP official law lists",
            "dataset_version": "nite-chrip-test",
            "negative_conclusion_supported": False,
            "source_urls": ["https://www.chem-info.nite.go.jp/"],
            "source_update_date": "2026-07-31",
        }
        for law_code in law_codes
    }


def _prepare_repo_fixture(tmp_path: Path, *, new_columns: bool = True, obligations: bool = True) -> Path:
    root = tmp_path / "ra-law-db"
    columns = INDEX_COLUMNS + (NEW_INDEX_COLUMNS if new_columns else [])
    _write_csv(root / "masters" / "regulatory_index.csv", columns, INDEX_ROWS)
    (root / "masters" / "master_coverage.json").write_text(
        json.dumps(_coverage(["ish", "poison_control", "food_contact", "cscl", "prtr", "cwc", "narcotics"])),
        encoding="utf-8",
    )
    if obligations:
        _write_csv(root / "masters" / "obligations.csv", OBLIGATION_COLUMNS, OBLIGATION_ROWS)
    return root


def _with_legacy_layer(root: Path) -> Path:
    _write_csv(root / "exports" / "regulatory_substances.csv", LEGACY_COLUMNS, LEGACY_ROWS)
    return root


CONTRACT_ACTION_KEYS = {"action_code", "label", "required", "kind", "basis", "condition", "owner_hint"}


def _db(path: Path) -> LawScreeningDatabase:
    LawScreeningDatabase.reset_instance()
    return LawScreeningDatabase.get_instance(path)


def _by_law(payload: dict) -> dict[str, dict]:
    return {item["law_code"]: item for item in payload["results"]}


def test_version_is_0_5_2():
    assert __version__ == "0.5.2"


def test_obligations_csv_covers_every_law_domain_and_matches_packaged_copy():
    rows = load_obligations_csv(REPO_ROOT / "masters" / "obligations.csv")
    laws = {row.law_code for row in rows}
    expected = {
        "cscl", "prtr", "poison_control", "ish", "waste", "cwc", "dust_rule", "occupational_health", "ozone_layer",
        "air_pollution", "water_pollution", "soil_contamination", "household_products", "high_pressure_gas",
        "explosives", "fire_service", "narcotics", "food_contact", "cosmetics", "women_rules",
    }  # fmt: skip
    assert expected <= laws
    for law_code in expected:
        assert any(row.category_code == "*" for row in rows if row.law_code == law_code), law_code
    keyed = {(row.law_code, row.category_code, row.seq) for row in rows}
    assert len(keyed) == len(rows)
    ish_codes = {row.category_code for row in rows if row.law_code == "ish"}
    assert {
        "tokka_class_1",
        "tokka_class_2",
        "tokka_class_3",
        "tokka",
        "organic_type_1",
        "lead",
        "label_sds",
    } <= ish_codes

    def matrix(code: str) -> dict:
        return build_management([row for row in rows if row.law_code == "ish" and row.category_code == code])

    assert matrix("tokka_class_2")["health_checks"][0]["interval"].startswith("6か月")
    assert matrix("tokka_class_2")["supervisors"][0]["title"] == "特定化学物質作業主任者"
    assert matrix("special_management")["health_checks"][0]["record_years"] == 30
    assert matrix("special_management")["work_records"][0]["years"] == 30
    assert matrix("lead")["measurements"][0]["interval"] == "1年以内ごと"
    assert matrix("organic_type_1")["notices"]
    assert matrix("tokka_class_3")["health_checks"] == []
    packaged = resources.files("ra_law_db").joinpath("data").joinpath("obligations.csv")
    assert packaged.read_bytes() == (REPO_ROOT / "masters" / "obligations.csv").read_bytes()


def test_obligation_loader_precedence_csv_then_builtin(tmp_path):
    db = _db(_prepare_repo_fixture(tmp_path))
    status = db.dataset_status()
    assert status["obligations_loaded"] is True
    assert status["obligations_source"] == "csv"
    assert status["obligation_rows"] == len(OBLIGATION_ROWS)
    assert status["class_coverage"]["ish"] == {"classed": 3, "unresolved": 2}  # PCB + 塩酸 unresolved

    db = _db(_prepare_repo_fixture(tmp_path / "b", obligations=False))
    status = db.dataset_status()
    assert status["obligations_source"] == "builtin"
    assert status["obligation_rows"] > 100


def test_sqlite_obligations_table_wins_when_present(tmp_path):
    sqlite_path = tmp_path / "regulatory.sqlite3"
    connection = sqlite3.connect(sqlite_path)
    connection.execute(
        "CREATE TABLE source_snapshots (law_id TEXT, source_url TEXT, fetched_at TEXT, content_hash TEXT)"
    )
    connection.execute("CREATE TABLE law_entries (entry_id TEXT, law_id TEXT, raw_name TEXT, normalized_name TEXT)")
    connection.execute("CREATE TABLE cas_mappings (entry_id TEXT, cas_number TEXT, regulation_type TEXT)")
    connection.execute("CREATE TABLE unresolved_entries (law_id TEXT, raw_name TEXT)")
    connection.execute(
        "CREATE TABLE regulatory_substances (cas_number TEXT, name_ja TEXT, name_en TEXT, regulation_type TEXT)"
    )
    connection.execute("CREATE TABLE master_coverage (law_code TEXT, coverage_json TEXT)")
    connection.execute(f"CREATE TABLE regulatory_index ({', '.join(f'{c} TEXT' for c in INDEX_COLUMNS)})")
    connection.execute(f"CREATE TABLE obligations ({', '.join(f'{c} TEXT' for c in OBLIGATION_COLUMNS)})")
    for law_code, payload in _coverage(["ish"]).items():
        connection.execute("INSERT INTO master_coverage VALUES (?, ?)", (law_code, json.dumps(payload)))
    row = INDEX_ROWS[1]
    connection.execute(
        f"INSERT INTO regulatory_index VALUES ({', '.join('?' for _ in INDEX_COLUMNS)})",
        [row[c] for c in INDEX_COLUMNS],
    )
    connection.execute(
        f"INSERT INTO obligations VALUES ({', '.join('?' for _ in OBLIGATION_COLUMNS)})",
        [
            "ish",
            "label_sds",
            "1",
            "mandatory",
            "SQLite由来の義務",
            "",
            "安衛法57",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
        ],
    )
    connection.commit()
    connection.close()

    db = _db(sqlite_path)
    assert db.dataset_status()["obligations_source"] == "sqlite"
    ish = _by_law(db.lookup(cas_number="75-09-2"))["ish"]
    assert ish["required_actions"][0]["label"] == "SQLite由来の義務"
    # Bundle without the new index columns: the new category fields are null, not missing.
    assert ish["categories"][0]["class_source"] is None
    assert ish["categories"][0]["threshold_pct"] is None


def test_not_listed_for_valid_cas_and_unknown_for_ambiguity(tmp_path):
    db = _db(_prepare_repo_fixture(tmp_path))
    by_law = _by_law(db.lookup(cas_number="7732-18-5", language="ja"))

    assert by_law["ish"]["status"] == "not_listed"
    assert by_law["ish"]["status_reason_code"] == "NOT_ON_POSITIVE_LIST"
    assert by_law["ish"]["notes"].startswith("CAS単位で公式リストに不在")
    assert [item["kind"] for item in by_law["ish"]["required_actions"]] == ["info"]
    assert by_law["ish"]["required_actions"][0]["label"] == "組成・用途変更時に再照合する"
    assert by_law["food_contact"]["status"] == "requires_context"
    assert by_law["food_contact"]["polarity"] == "permitted_list"
    assert by_law["ish"]["polarity"] == "regulated_list"

    # Invalid CAS text and an unresolved name stay unknown.
    assert _by_law(db.lookup(cas_number="not-a-cas"))["ish"]["status"] == "unknown"
    assert _by_law(db.lookup(substance_name="存在しない物質"))["ish"]["status"] == "unknown"


def test_threshold_below_for_every_category_gives_not_applies(tmp_path):
    db = _db(_prepare_repo_fixture(tmp_path))
    poison = _by_law(db.lookup(cas_number="7664-93-9", percent=5.0))["poison_control"]

    assert poison["status"] == "not_applies"
    assert poison["status_reason_code"] == "BELOW_THRESHOLD"
    assert poison["threshold"]["pct"] == 10.0
    assert poison["threshold"]["below_threshold"] is True
    assert poison["threshold"]["note"] == "10 %以下を含有するものを除く"
    assert poison["categories"][0]["threshold"] == {
        "pct": 10.0,
        "note": "10 %以下を含有するものを除く",
        "below_threshold": True,
    }
    assert poison["required_actions"]
    assert poison["management"]["health_checks"] == []

    above = _by_law(db.lookup(cas_number="7664-93-9", percent=50))["poison_control"]
    assert above["status"] == "requires_context"
    assert above["categories"][0]["threshold"]["below_threshold"] is False
    assert above["threshold"] is None


def test_threshold_partial_keeps_requires_context(tmp_path):
    db = _db(_prepare_repo_fixture(tmp_path))
    ish = _by_law(db.lookup(cas_number="75-09-2", percent=0.5))["ish"]

    assert ish["status"] == "requires_context"
    by_code = {item["code"]: item for item in ish["categories"]}
    assert by_code["tokka_class_2"]["threshold"] == {
        "pct": 1.0,
        "note": "1 %以下を含有するものを除く",
        "below_threshold": True,
    }
    assert by_code["label_sds"]["threshold"] == {"pct": None, "note": None, "below_threshold": None}


def test_substances_lookup_returns_results_by_cas(tmp_path):
    db = _db(_prepare_repo_fixture(tmp_path))
    payload = db.lookup(substances=[{"cas_number": "7664-93-9", "percent": 5}, {"cas_number": "75-09-2"}])

    assert set(payload["results_by_cas"]) == {"7664-93-9", "75-09-2"}
    assert _by_law(payload["results_by_cas"]["7664-93-9"])["poison_control"]["status"] == "not_applies"
    assert payload["results_by_cas"]["7664-93-9"]["query"]["percent"] == 5.0
    assert _by_law(payload["results_by_cas"]["75-09-2"])["ish"]["status"] == "requires_context"


def test_required_actions_management_and_flags_from_index_categories(tmp_path):
    db = _db(_prepare_repo_fixture(tmp_path))
    payload = db.lookup(cas_number="75-09-2", language="ja")
    ish = _by_law(payload)["ish"]

    categories = {item["code"]: item for item in ish["categories"]}
    tokka = categories["tokka_class_2"]
    assert tokka["class_source"] == "legal_row_join"
    assert tokka["special_management"] is True
    assert tokka["special_organic"] is True
    assert tokka["legal_number"] == "別表第3第2号"
    assert categories["label_sds"]["special_management"] is None

    codes = [item["action_code"] for item in ish["required_actions"]]
    assert codes == ["ish_tokka_class_2_1", "ish_label_sds_1", "ish_special_management_1"]
    assert len(set(codes)) == len(codes)
    first = ish["required_actions"][0]
    assert {"action_code", "label", "required", "kind", "basis", "condition", "owner_hint"} <= set(first)
    assert first == {**first, "required": True, "kind": "mandatory", "basis": "特化則39", "owner_hint": "産業保健"}
    conditional = ish["required_actions"][1]
    assert conditional["required"] is False
    assert conditional["condition"] == "屋内作業場"

    management = ish["management"]
    assert management["health_checks"] == [
        {
            "type": "特定化学物質健康診断",
            "interval": "6か月以内ごと",
            "record_years": 30,
            "basis": "特化則39；特化則38条の3",
            "kind": "mandatory",
        }
    ]
    assert management["measurements"] == [
        {"interval": "6か月以内ごと", "record_years": 3, "basis": "特化則39", "kind": "mandatory"}
    ]
    assert management["work_records"] == [{"years": 30, "basis": "特化則38条の3", "kind": "mandatory"}]
    assert management["supervisors"] == [{"title": "特定化学物質作業主任者", "basis": "特化則39", "kind": "mandatory"}]
    assert management["notices"] == [{"text": "特別管理物質の掲示", "basis": "特化則38条の3", "kind": "mandatory"}]
    assert ish["class_sources"] == {
        "tokka_class_2": "legal_row_join",
        "label_sds": "source_list",
        "special_management": "index",
        "special_organic": "index",
    }
    assert ish["flags_source"]["tokka_class_2"] == "index"

    flags = ish["flags"]
    assert flags["tokka_applicable"] is True
    assert flags["tokka_class_2"] is True
    assert flags["tokka_class_unresolved"] is False
    assert flags["special_management"] is True
    assert flags["label_sds"] is True
    assert flags["organic_applicable"] is False
    assert {"tokka_class_1", "tokka_class_3", "organic_type_1", "lead_applicable", "prohibited_substance",
            "manufacture_permission", "skin_protection", "carcinogen_record", "oel_set"} <= set(flags)  # fmt: skip

    items = {item["key"]: item for item in ish["required_context_items"]}
    assert items["concentration"] == {
        "key": "concentration",
        "label_ja": "含有率・濃度",
        "label_en": "Concentration",
        "label": "含有率・濃度",
    }
    assert set(items) == set(ish["required_context"])

    assert payload["summary"]["requires_context"] == 2  # ish + occupational_health
    assert payload["summary"]["not_listed"] == 4  # poison_control + food_contact + narcotics + cscl
    assert payload["hard_duty_laws"] == ["ish"]
    assert ish["hard_duty"] is True

    occupational = _by_law(payload)["occupational_health"]
    assert occupational["status"] == "requires_context"
    assert occupational["management"]["health_checks"][0]["record_years"] == 30


def test_english_labels_fall_back_to_japanese(tmp_path):
    db = _db(_prepare_repo_fixture(tmp_path))
    ish = _by_law(db.lookup(cas_number="75-09-2", language="en"))["ish"]
    labels = [item["label"] for item in ish["required_actions"]]
    assert labels[0] == "Health check every 6 months"
    assert labels[1] == "濃度基準値を確認する"
    assert ish["required_context_items"][0]["label"] == ish["required_context_items"][0]["label_en"]


def test_unresolved_class_and_star_fallback(tmp_path):
    db = _db(_prepare_repo_fixture(tmp_path))
    payload = db.lookup(cas_number="1336-36-3")
    ish = _by_law(payload)["ish"]
    assert ish["flags"]["tokka_class_unresolved"] is True
    assert ish["flags"]["manufacture_permission"] is True
    assert [item["action_code"] for item in ish["required_actions"]] == ["ish_manufacture_permission_1", "ish_tokka_1"]
    assert payload["hard_duty_laws"] == ["ish"]

    # No category-specific rows for food_contact_base_material -> per-law '*' rows.
    food = _by_law(db.lookup(cas_number="7732-18-5"))["food_contact"]
    assert [item["action_code"] for item in food["required_actions"]] == ["food_contact_*_1"]
    assert food["hard_duty"] is False


def test_hard_duty_requires_mandatory_obligation(tmp_path):
    root = _prepare_repo_fixture(tmp_path)
    rows = [dict(row) for row in OBLIGATION_ROWS]
    for row in rows:
        if row["category_code"] == "deleterious":
            row["kind"] = "conditional"
    _write_csv(root / "masters" / "obligations.csv", OBLIGATION_COLUMNS, rows)
    db = _db(root)
    assert "poison_control" not in db.lookup(cas_number="7664-93-9")["hard_duty_laws"]

    _write_csv(root / "masters" / "obligations.csv", OBLIGATION_COLUMNS, OBLIGATION_ROWS)
    db.reload()
    assert db.lookup(cas_number="7664-93-9")["hard_duty_laws"] == ["poison_control"]


@pytest.mark.parametrize(
    ("cas", "name", "expected"),
    [
        ("―", "塩素化ビフェニル", "1336-36-3"),
        ("―", "コールタール", "8007-45-2"),
        ("―", "コールタールナフサ（ソルベントナフサを含む。）", "8030-30-6"),
        ("―", "リフラクトリーセラミックファイバー", "142844-00-6"),
        ("―", "石油ベンジン", "8032-32-4"),
        ("7784-46- 5", "砒素及びその化合物", "7784-46-5"),
        ("409-21-2a", "炭化けい素", "409-21-2"),
        ("75-09-2", "削除", None),
        ("―", "黄りんマッチ", "―"),
        ("75-09-2", "ジクロロメタン", "75-09-2"),
    ],
)
def test_legacy_cas_overrides(cas, name, expected):
    assert apply_legacy_cas_override(cas, name) == expected


def test_legacy_export_overrides_are_applied_at_load(tmp_path):
    root = tmp_path / "ra-law-db"
    columns = ["cas_number", "name_ja", "name_en", "regulation_type", "regulation_class", "regulation_label"]
    _write_csv(
        root / "exports" / "regulatory_substances.csv",
        columns,
        [
            {
                "cas_number": "―",
                "name_ja": "塩素化ビフェニル",
                "regulation_type": "tokka",
                "regulation_class": 1,
                "regulation_label": "特化則第1類",
            },
            {
                "cas_number": "7784-46- 5",
                "name_ja": "砒素及びその化合物",
                "regulation_type": "tokka",
                "regulation_class": 2,
                "regulation_label": "特化則第2類",
            },
            {
                "cas_number": "67-56-1",
                "name_ja": "削除",
                "regulation_type": "organic",
                "regulation_class": 2,
                "regulation_label": "有機則第2種",
            },
        ],
    )
    db = _db(root)
    assert "1336-36-3" in db._rows_by_cas
    assert "7784-46-5" in db._rows_by_cas
    assert "―" not in db._rows_by_cas
    assert "67-56-1" not in db._rows_by_cas

    ish = _by_law(db.lookup(cas_number="1336-36-3"))["ish"]
    assert ish["status"] == "requires_context"
    assert ish["flags"]["tokka_class_1"] is True
    assert any(item["action_code"] == "ish_tokka_class_1_1" for item in ish["required_actions"])


def test_legacy_only_ish_cas_stays_listed_with_legacy_class_source(tmp_path):
    db = _db(_with_legacy_layer(_prepare_repo_fixture(tmp_path)))
    payload = db.lookup(cas_number="7791-20-2", language="ja")
    by_law = _by_law(payload)
    ish = by_law["ish"]

    assert ish["status"] == "requires_context"
    assert ish["status_reason_code"] == "MATCHED_CONTEXT_REQUIRED"
    assert "補助レイヤ" in ish["notes"]
    assert [item["code"] for item in ish["categories"]] == ["tokka_class_2"]
    assert ish["categories"][0]["class_source"] == "legacy_regulatory_substances"
    assert ish["categories"][0]["special_management"] is True
    assert ish["flags"]["tokka_class_2"] is True
    assert ish["flags"]["tokka_applicable"] is True
    assert ish["flags"]["special_management"] is True
    assert ish["flags"]["tokka_class_unresolved"] is False
    assert ish["flags_source"] == {
        "tokka_applicable": "legacy_regulatory_substances",
        "tokka_class_2": "legacy_regulatory_substances",
        "special_management": "legacy_regulatory_substances",
    }
    assert ish["class_sources"] == {
        "tokka_class_2": "legacy_regulatory_substances",
        "special_management": "legacy_regulatory_substances",
    }
    assert ish["evidence"]["class_source"] == "legacy_regulatory_substances"
    codes = [item["action_code"] for item in ish["required_actions"]]
    assert codes == ["ish_tokka_class_2_1", "ish_special_management_1"]
    assert ish["management"]["health_checks"][0]["record_years"] == 30
    assert payload["hard_duty_laws"] == ["ish"]

    occupational = by_law["occupational_health"]
    assert occupational["status"] == "requires_context"
    assert occupational["class_sources"]["tokka_class_2"] == "legacy_regulatory_substances"
    assert occupational["management"]["health_checks"][0]["type"] == "特定化学物質健康診断"


def test_identity_note_on_any_law_keeps_unknown_instead_of_not_listed(tmp_path):
    db = _db(_prepare_repo_fixture(tmp_path))
    by_law = _by_law(db.lookup(cas_number="104948-36-9", language="ja"))

    assert by_law["cscl"]["status"] == "requires_context"
    assert "substance_identity" in by_law["cscl"]["required_context"]
    for law_code in ("ish", "poison_control", "narcotics"):
        result = by_law[law_code]
        assert result["status"] == "unknown", law_code
        assert result["status_reason_code"] == "INCOMPLETE_MASTER_DATASET"
        assert "substance_identity" in result["required_context"]
        assert "同定注記" in result["notes"]
    # Without a note the same shape of miss is not_listed.
    assert _by_law(db.lookup(cas_number="7732-18-5"))["ish"]["status"] == "not_listed"


def test_resolved_legacy_class_supersedes_unresolved_index_code(tmp_path):
    db = _db(_with_legacy_layer(_prepare_repo_fixture(tmp_path)))
    payload = db.lookup(cas_number="7647-01-0", language="ja")
    ish = _by_law(payload)["ish"]

    assert ish["status"] == "requires_context"
    assert [item["code"] for item in ish["categories"]] == ["tokka", "tokka_class_3"]
    assert ish["categories"][1]["class_source"] == "legacy_regulatory_substances"
    assert ish["flags"]["tokka_class_unresolved"] is True  # the index itself did not resolve it
    assert ish["flags"]["tokka_class_3"] is True
    assert ish["flags_source"]["tokka_class_3"] == "legacy_regulatory_substances"
    assert ish["class_sources"] == {"tokka_class_3": "legacy_regulatory_substances"}
    codes = [item["action_code"] for item in ish["required_actions"]]
    assert codes == ["ish_tokka_class_3_1"]  # no contradictory unresolved-``tokka`` duties
    assert ish["management"]["supervisors"] == [
        {"title": "特定化学物質作業主任者", "basis": "特化則27", "kind": "conditional"}
    ]
    assert payload["hard_duty_laws"] == []
    assert _by_law(payload)["occupational_health"]["class_sources"] == {"tokka_class_3": "legacy_regulatory_substances"}


def test_narcotics_precursor_and_scheduled_are_resolved_from_legal_number(tmp_path):
    db = _db(_prepare_repo_fixture(tmp_path))

    acetone = db.lookup(cas_number="67-64-1", language="ja")
    narcotics = _by_law(acetone)["narcotics"]
    assert narcotics["status"] == "requires_context"
    assert narcotics["class_sources"] == {"narcotic_precursor": "legal_number"}
    assert [item["action_code"] for item in narcotics["required_actions"]] == ["narcotics_narcotic_precursor_1"]
    assert narcotics["required_actions"][0]["kind"] == "conditional"
    assert "narcotics" not in acetone["hard_duty_laws"]

    morphine = db.lookup(cas_number="57-27-2", language="ja")
    narcotics = _by_law(morphine)["narcotics"]
    assert narcotics["class_sources"] == {"narcotic_scheduled": "legal_number"}
    assert [item["action_code"] for item in narcotics["required_actions"]] == ["narcotics_narcotic_scheduled_1"]
    assert morphine["hard_duty_laws"] == ["narcotics"]

    unresolved = db.lookup(cas_number="1095-90-5", language="ja")
    narcotics = _by_law(unresolved)["narcotics"]
    assert narcotics["class_sources"] == {"controlled_narcotic": "source_list"}
    assert [item["action_code"] for item in narcotics["required_actions"]] == ["narcotics_controlled_narcotic_1"]
    assert narcotics["required_actions"][0]["condition"] == "麻薬・向精神薬本体の場合"
    assert unresolved["hard_duty_laws"] == []


@pytest.mark.parametrize("cas_number", ["75-09-2", "7732-18-5", "104948-36-9", "not-a-cas"])
def test_every_required_action_item_has_the_contract_keys(tmp_path, cas_number):
    db = _db(_prepare_repo_fixture(tmp_path))
    for percent in (None, 0.5):
        payload = db.lookup(cas_number=cas_number, language="ja", percent=percent)
        for result in payload["results"]:
            for item in result["required_actions"]:
                assert CONTRACT_ACTION_KEYS <= set(item), (result["law_code"], item)
                assert item["kind"] in {"mandatory", "conditional", "info"}
            if result["status"] in {"unknown", "not_listed", "not_applies"}:
                assert all(item["kind"] == "info" for item in result["required_actions"]), result["law_code"]


def test_below_threshold_reports_strictest_threshold_at_result_level(tmp_path):
    root = _prepare_repo_fixture(tmp_path)
    rows = list(INDEX_ROWS) + [
        _index_row(
            "poison_control",
            "poison",
            "毒物",
            "7664-93-9",
            "硫酸",
            class_source="legal_row_join",
            threshold_pct="20",
            threshold_note="20 %以下を含有するものを除く",
        ),  # fmt: skip
    ]
    _write_csv(root / "masters" / "regulatory_index.csv", INDEX_COLUMNS + NEW_INDEX_COLUMNS, rows)
    db = _db(root)
    poison = _by_law(db.lookup(cas_number="7664-93-9", percent=5.0))["poison_control"]
    assert poison["status"] == "not_applies"
    assert poison["threshold"] == {
        "pct": 20.0,
        "note": "20 %以下を含有するものを除く",
        "below_threshold": True,
        "percent": 5.0,
    }


def test_women_rules_domain_is_derived_from_ish_groups_with_verbatim_duties():
    """女性則 第2条第1項第18号: derived rows, per-sub-item duties, shared duties emitted once."""
    db = LawScreeningDatabase.get_instance()

    def women(cas: str) -> dict:
        payload = db.lookup(cas_number=cas, language="ja")
        return next(item for item in payload["results"] if item["law_code"] == "women_rules")

    toluene = women("108-88-3")
    assert toluene["status"] == "requires_context"
    assert [c["code"] for c in toluene["categories"]] == ["women_18_ha"]
    assert toluene["categories"][0]["threshold_pct"] == 5.0  # inherited from 有機則
    assert [a["kind"] for a in toluene["required_actions"]] == ["mandatory", "conditional", "info", "info"]
    assert "全ての女性労働者" in toluene["required_actions"][0]["label"]
    assert {i["key"] for i in toluene["required_context_items"]} == {
        "respirator_required_work",
        "workplace_control_class",
    }

    styrene = women("100-42-5")  # listed under both イ and ハ
    assert {c["code"] for c in styrene["categories"]} == {"women_18_i", "women_18_ha"}
    assert [a["kind"] for a in styrene["required_actions"]] == ["mandatory", "conditional", "info", "info", "info"]

    lead = women("7439-92-1")
    assert [c["code"] for c in lead["categories"]] == ["women_18_ro"]
    assert women("64-17-5")["status"] == "not_listed"  # エタノール: no 女性則 group


def test_women_rules_context_resolves_to_applies_or_not_applies():
    db = LawScreeningDatabase.get_instance()

    def women(cas: str, **context) -> dict:
        payload = db.lookup(cas_number=cas, language="ja", context=context or None)
        return next(item for item in payload["results"] if item["law_code"] == "women_rules")

    # 第三管理区分 → applies for every listed substance
    hit = women("108-88-3", workplace_control_class="第三管理区分")
    assert hit["status"] == "applies" and hit["status_reason_code"] == "WOMEN_RULE_WORK_COVERED"
    # 呼吸用保護具 work → applies (toluene: 有機則 branch)
    assert women("108-88-3", respirator_required_work=True, workplace_control_class=1)["status"] == "applies"
    # class 1 and no respirator mandate → not_applies with an explicit reason
    miss = women("108-88-3", respirator_required_work=False, workplace_control_class=2)
    assert miss["status"] == "not_applies" and miss["status_reason_code"] == "WOMEN_RULE_WORK_NOT_COVERED"
    assert miss["required_actions"] and all(a["kind"] == "info" for a in miss["required_actions"])
    # スチレン (（２）のみ): the respirator branch alone does not cover it
    styrene_i = db.lookup(cas_number="100-42-5", language="ja", context={"respirator_required_work": True})
    women_styrene = next(item for item in styrene_i["results"] if item["law_code"] == "women_rules")
    assert women_styrene["status"] == "applies"  # ハ (有機溶剤 branch) is not （２）のみ, so it still applies
    # only a control class given, and it is 1: undecided without the respirator answer → stays requires_context
    assert women("108-88-3", workplace_control_class=1)["status"] == "requires_context"
    # below the inherited 有機則 threshold → not_applies via percent screening
    below = db.lookup(cas_number="108-88-3", language="ja", percent=3)
    assert next(item for item in below["results"] if item["law_code"] == "women_rules")["status"] == "not_applies"


def test_review_fixes_duplicates_labels_and_context_actions():
    db = LawScreeningDatabase.get_instance()

    def law(payload: dict, code: str) -> dict:
        return next(item for item in payload["results"] if item["law_code"] == code)

    # 硫酸: two 劇物 rows (硫酸 / 硫酸を含有する製剤) become one chip carrying both 政令番号
    poison = law(db.lookup(cas_number="7664-93-9", language="ja"), "poison_control")
    assert [c["code"] for c in poison["categories"]] == ["deleterious"]
    assert len(poison["categories"][0]["legal_numbers"]) == 2

    # トルエン is a 麻薬向精神薬原料, never "麻薬・向精神薬等"
    narcotics = law(db.lookup(cas_number="108-88-3", language="ja"), "narcotics")
    assert narcotics["categories"][0]["label"].startswith("麻薬向精神薬原料")

    # メタノール: the statutory アルコール類 wins over the flash-point rule; one 第4類 chip only
    fire = law(
        db.lookup(cas_number="67-56-1", language="ja", context={"flash_point_c": 11, "boiling_point_c": 65}),
        "fire_service",
    )
    class4 = [c for c in fire["categories"] if c["code"] == "hazmat_class_4"]
    assert len(class4) == 1 and "アルコール類" in class4[0]["label"]

    # process-context laws ask for the process, not for the CAS
    payload = db.lookup(cas_number="7732-18-5", language="ja")
    assert "粉じん作業" in law(payload, "dust_rule")["required_actions"][0]["label"]
    assert "特別管理産業廃棄物" in law(payload, "waste")["required_actions"][0]["label"]

    # waste hits use a stable category code that the obligations matrix knows
    waste = law(db.lookup(cas_number="75-09-2", language="ja"), "waste")
    assert waste["categories"][0]["code"] == "waste_listed"
    assert any(a["kind"] == "mandatory" for a in waste["required_actions"])
