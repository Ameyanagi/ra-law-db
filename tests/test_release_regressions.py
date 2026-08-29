"""Release regressions for conservative law-status semantics and identity safety."""

from __future__ import annotations

import pytest

from ra_law_db import LawScreeningDatabase


REPRESENTATIVE_SUBSTANCES = {
    # Common gases
    "124-38-9": {"ish": "requires_context", "high_pressure_gas": "requires_context"},
    "1333-74-0": {"ish": "requires_context", "high_pressure_gas": "requires_context"},
    # Common laboratory solvent
    "67-56-1": {
        "poison_control": "requires_context",
        "ish": "requires_context",
        "fire_service": "requires_context",
    },
    # Elemental metal; deliberately separate from chromium(VI) compounds
    "7440-47-3": {
        "prtr": "requires_context",
        "ish": "requires_context",
        "air_pollution": "requires_context",
        "water_pollution": "requires_context",
    },
    # Common metal oxides and catalyst/support materials
    "1317-38-0": {
        "ish": "requires_context",
        "air_pollution": "requires_context",
        "water_pollution": "requires_context",
    },
    "1314-13-2": {
        "ish": "requires_context",
        "air_pollution": "requires_context",
        "water_pollution": "requires_context",
    },
    "1344-28-1": {"water_pollution": "requires_context"},
    # Multi-domain positive controls
    "108-88-3": {
        "cscl": "requires_context",
        "prtr": "requires_context",
        "poison_control": "requires_context",
        "ish": "requires_context",
    },
    "71-43-2": {
        "cscl": "requires_context",
        "prtr": "requires_context",
        "ish": "requires_context",
    },
    "67-64-1": {"ish": "requires_context", "fire_service": "requires_context"},
    "75-09-2": {"prtr": "requires_context", "ish": "requires_context"},
    "7439-92-1": {
        "prtr": "requires_context",
        "ish": "requires_context",
        "soil_contamination": "requires_context",
    },
    "7778-50-9": {
        "prtr": "requires_context",
        "poison_control": "requires_context",
        "ish": "requires_context",
    },
    "7664-93-9": {
        "poison_control": "requires_context",
        "ish": "requires_context",
        "narcotics": "requires_context",
    },
    # Conservative no-hit controls
    "7732-18-5": {"food_contact": "requires_context"},
    "7647-14-5": {},
}


CONTRACT_ACTION_KEYS = {"action_code", "label", "required", "kind", "basis", "condition", "owner_hint"}


@pytest.fixture(scope="module")
def bundled_db() -> LawScreeningDatabase:
    LawScreeningDatabase.reset_instance()
    return LawScreeningDatabase.get_instance()


@pytest.mark.parametrize(("cas_number", "positive_statuses"), REPRESENTATIVE_SUBSTANCES.items())
def test_representative_substances_are_conservative_and_evidenced(
    bundled_db: LawScreeningDatabase,
    cas_number: str,
    positive_statuses: dict[str, str],
) -> None:
    payload = bundled_db.lookup(cas_number=cas_number, language="ja")
    by_law = {result["law_code"]: result for result in payload["results"]}

    assert set(payload["summary"]) == {"applies", "requires_context", "not_listed", "unknown", "not_applies"}
    assert payload["summary"]["not_applies"] == 0
    assert isinstance(payload["hard_duty_laws"], list)

    for law_code in ("cscl", "prtr", "poison_control", "ish", "waste", "cwc"):
        result = by_law[law_code]
        # not_applies is only reachable through an explicit percent threshold (BELOW_THRESHOLD).
        assert result["status"] in {"applies", "requires_context", "not_listed", "unknown"}
        if result["status"] == "not_listed":
            assert result["status_reason_code"] == "NOT_ON_POSITIVE_LIST"
            assert result["required_actions"][0]["kind"] == "info"
        assert result["polarity"] in {"regulated_list", "permitted_list"}
        assert "management" in result
        assert "required_context_items" in result
        assert result["status_reason_code"]
        assert result["notes"]
        assert result["dataset_name"]
        assert result["dataset_version"]
        assert result["dataset_loaded"] is True
        assert result["dataset_coverage"]
        assert result["source"]
        assert result["update_date"]
        assert result["manual_verification_actions"]

    # Every result (positive, unknown, not_listed alike) carries the contract action-item shape.
    for result in payload["results"]:
        for item in result["required_actions"]:
            assert CONTRACT_ACTION_KEYS <= set(item), (result["law_code"], item)
            assert item["kind"] in {"mandatory", "conditional", "info"}
        if result["status"] in {"unknown", "not_listed"}:
            assert all(item["kind"] == "info" for item in result["required_actions"]), result["law_code"]

    for law_code, status in positive_statuses.items():
        result = by_law[law_code]
        assert result["status"] == status
        assert result["categories"]
        assert result["evidence"]["source_urls"]
        assert result["required_actions"], law_code


@pytest.mark.parametrize("cas_number", REPRESENTATIVE_SUBSTANCES)
def test_general_compounds_never_get_unsupported_negative_status(
    bundled_db: LawScreeningDatabase,
    cas_number: str,
) -> None:
    payload = bundled_db.lookup(cas_number=cas_number, language="ja")
    for result in payload["results"]:
        coverage = result["dataset_coverage"]
        if not coverage.get("negative_conclusion_supported", False):
            assert result["status"] != "not_applies"

    # A percentage may only turn a hit into not_applies via an explicit threshold reason.
    with_percent = bundled_db.lookup(cas_number=cas_number, language="ja", percent=0.01)
    for result in with_percent["results"]:
        if result["status"] == "not_applies":
            assert result["status_reason_code"] == "BELOW_THRESHOLD"
            assert result["threshold"]["below_threshold"] is True


def test_water_collapses_into_not_listed_rows(bundled_db: LawScreeningDatabase) -> None:
    payload = bundled_db.lookup(cas_number="7732-18-5", language="ja")
    by_law = {result["law_code"]: result for result in payload["results"]}
    assert by_law["ish"]["status"] == "not_listed"
    assert by_law["food_contact"]["polarity"] == "permitted_list"
    assert payload["summary"]["not_listed"] >= 10
    assert payload["hard_duty_laws"] == []


def test_pcb_gets_ish_flags_and_duties_from_index_categories(bundled_db: LawScreeningDatabase) -> None:
    ish = {result["law_code"]: result for result in bundled_db.lookup(cas_number="1336-36-3")["results"]}["ish"]
    assert ish["status"] == "requires_context"
    assert ish["flags"]["tokka_applicable"] is True
    assert ish["flags"]["manufacture_permission"] is True
    assert ish["management"]["health_checks"]
    assert any(item["basis"] for item in ish["required_actions"])
    assert "ish" in bundled_db.lookup(cas_number="1336-36-3")["hard_duty_laws"]


def test_common_lab_reagents_are_narcotic_precursors_not_narcotics(bundled_db: LawScreeningDatabase) -> None:
    # Acetone / sulfuric acid are 麻薬向精神薬原料 (法別表第4 / 指定政令第5条): no licence duty, never a hard duty.
    for cas_number in ("67-64-1", "7664-93-9"):
        payload = bundled_db.lookup(cas_number=cas_number, language="ja")
        narcotics = {result["law_code"]: result for result in payload["results"]}["narcotics"]
        assert narcotics["status"] == "requires_context"
        assert "narcotics" not in payload["hard_duty_laws"], cas_number
        assert set(narcotics["class_sources"]) == {"narcotic_precursor"}
        assert not any(item["kind"] == "mandatory" for item in narcotics["required_actions"])
        assert not any(
            "免許" in item["label"] and item["kind"] == "mandatory" for item in narcotics["required_actions"]
        )


def test_legacy_only_ish_substances_are_not_downgraded_to_not_listed(bundled_db: LawScreeningDatabase) -> None:
    ish_index = bundled_db._indexed_by_law.get("ish", {})
    legacy_only = sorted(
        cas
        for cas, rows in bundled_db._rows_by_cas.items()
        if cas not in ish_index
        and any(row.regulation_type in {"tokka", "organic", "lead", "prohibited"} for row in rows)
    )
    if not legacy_only:
        pytest.skip("every legacy ISH CAS is covered by the index in this bundle")
    for cas_number in legacy_only[:5]:
        payload = bundled_db.lookup(cas_number=cas_number, language="ja")
        by_law = {result["law_code"]: result for result in payload["results"]}
        ish = by_law["ish"]
        assert ish["status"] == "requires_context", cas_number
        assert ish["status_reason_code"] == "MATCHED_CONTEXT_REQUIRED"
        assert ish["categories"], cas_number
        assert all(item["class_source"] == "legacy_regulatory_substances" for item in ish["categories"])
        assert set(ish["class_sources"].values()) == {"legacy_regulatory_substances"}
        assert any(ish["flags"].values())
        assert all(value == "legacy_regulatory_substances" for value in ish["flags_source"].values())
        if by_law["occupational_health"]["management"]["health_checks"]:
            assert ish["management"]["health_checks"], cas_number


def test_identity_noted_cas_never_collapses_to_not_listed(bundled_db: LawScreeningDatabase) -> None:
    noted = sorted(bundled_db._identity_noted_cas)
    if not noted:
        pytest.skip("no identity-noted CAS in this bundle")
    payload = bundled_db.lookup(cas_number=noted[0], language="ja")
    assert payload["summary"]["not_listed"] == 0
    for result in payload["results"]:
        if result["status"] == "unknown" and result["law_code"] not in {"dust_rule", "occupational_health", "waste"}:
            assert "substance_identity" in result["required_context"], result["law_code"]


def test_management_entries_carry_kind(bundled_db: LawScreeningDatabase) -> None:
    ish = {result["law_code"]: result for result in bundled_db.lookup(cas_number="75-09-2")["results"]}["ish"]
    for section in ish["management"].values():
        for entry in section:
            assert entry["kind"] in {"mandatory", "conditional", "info"}, entry
    assert any(entry["kind"] == "mandatory" for entry in ish["management"]["health_checks"])


def test_bundle_reports_obligations_and_class_coverage(bundled_db: LawScreeningDatabase) -> None:
    status = bundled_db.dataset_status()
    assert status["obligations_loaded"] is True
    assert status["obligation_rows"] > 0
    assert status["obligations_source"] in {"sqlite", "csv", "builtin"}
    assert {"classed", "unresolved"} == set(status["class_coverage"]["ish"])


@pytest.mark.parametrize("alias", ["1,2-PDO", "propylene glycol", "プロピレングリコール"])
def test_propylene_glycol_aliases_resolve_exactly(
    bundled_db: LawScreeningDatabase,
    alias: str,
) -> None:
    payload = bundled_db.search(query=alias, mode="name", limit=3, min_score=0.6)
    assert payload["substance_hits"][0]["cas_number"] == "57-55-6"


@pytest.mark.parametrize(
    ("query", "expected_cas", "excluded_cas"),
    [
        ("Copper(II) oxide", "1317-38-0", "1317-39-1"),
        ("酸化銅(II)", "1317-38-0", "1317-39-1"),
        ("Copper(I) oxide", "1317-39-1", "1317-38-0"),
        ("酸化銅(I)", "1317-39-1", "1317-38-0"),
    ],
)
def test_copper_oxidation_state_is_not_conflated(
    bundled_db: LawScreeningDatabase,
    query: str,
    expected_cas: str,
    excluded_cas: str,
) -> None:
    payload = bundled_db.search(query=query, mode="name", limit=3, min_score=0.6)
    assert payload["substance_hits"][0]["cas_number"] == expected_cas
    exact_hits = [hit for hit in payload["substance_hits"] if hit["score"] == 1.0]
    assert {hit["cas_number"] for hit in exact_hits} == {expected_cas}
    assert excluded_cas not in {hit["cas_number"] for hit in exact_hits}


@pytest.mark.parametrize(
    ("query", "expected_cas"),
    [
        ("Chromium", "7440-47-3"),
        ("クロム", "7440-47-3"),
        ("Potassium dichromate", "7778-50-9"),
        ("重クロム酸カリウム", "7778-50-9"),
    ],
)
def test_elemental_chromium_and_chromium_vi_compound_are_distinct(
    bundled_db: LawScreeningDatabase,
    query: str,
    expected_cas: str,
) -> None:
    payload = bundled_db.search(query=query, mode="name", limit=5, min_score=0.6)
    assert payload["substance_hits"][0]["cas_number"] == expected_cas
    assert payload["substance_hits"][0]["score"] == 1.0


def test_powder_work_surfaces_dust_and_health_examination_screening(
    bundled_db: LawScreeningDatabase,
) -> None:
    payload = bundled_db.lookup(
        cas_number="7631-86-9",
        language="ja",
        context={
            "material_form": "powder",
            "work_process": "秤量・投入・混合",
            "dust_generation": "medium",
            "work_frequency": "daily",
            "facility": "laboratory",
        },
    )
    by_law = {result["law_code"]: result for result in payload["results"]}
    assert by_law["dust_rule"]["status"] == "requires_context"
    assert by_law["occupational_health"]["status"] == "requires_context"
    checks = by_law["occupational_health"]["evidence"]["health_checks"]
    assert any(check["type"] == "じん肺健康診断" for check in checks)
    for law_code in ("dust_rule", "occupational_health"):
        assert by_law[law_code]["dataset_coverage"]
        assert by_law[law_code]["update_date"] == "2026-08-28"
