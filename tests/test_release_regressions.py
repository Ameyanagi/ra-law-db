"""Release regressions for conservative law-status semantics and identity safety."""

from __future__ import annotations

import pytest

from ra_law_db import LawScreeningDatabase


SIX_SUBSTANCES = {
    "56-81-5": {},
    "57-55-6": {"cscl": "requires_context", "ish": "requires_context"},
    "1333-74-0": {"ish": "requires_context", "high_pressure_gas": "requires_context"},
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
}


@pytest.fixture(scope="module")
def bundled_db() -> LawScreeningDatabase:
    LawScreeningDatabase.reset_instance()
    return LawScreeningDatabase.get_instance()


@pytest.mark.parametrize(("cas_number", "positive_statuses"), SIX_SUBSTANCES.items())
def test_six_substances_are_conservative_and_evidenced(
    bundled_db: LawScreeningDatabase,
    cas_number: str,
    positive_statuses: dict[str, str],
) -> None:
    payload = bundled_db.lookup(cas_number=cas_number, language="ja")
    by_law = {result["law_code"]: result for result in payload["results"]}

    for law_code in ("cscl", "prtr", "poison_control", "ish", "waste", "cwc"):
        result = by_law[law_code]
        assert result["status"] != "not_applies"
        assert result["status_reason_code"]
        assert result["notes"]
        assert result["dataset_name"]
        assert result["dataset_version"]
        assert result["dataset_loaded"] is True
        assert result["dataset_coverage"]
        assert result["source"]
        assert result["update_date"]
        assert result["manual_verification_actions"]

    for law_code, status in positive_statuses.items():
        result = by_law[law_code]
        assert result["status"] == status
        assert result["categories"]
        assert result["evidence"]["source_urls"]


@pytest.mark.parametrize("cas_number", ["108-88-3", "71-43-2", "67-64-1", "7732-18-5", "7647-14-5"])
def test_general_compounds_never_get_unsupported_negative_status(
    bundled_db: LawScreeningDatabase,
    cas_number: str,
) -> None:
    payload = bundled_db.lookup(cas_number=cas_number, language="ja")
    for result in payload["results"]:
        coverage = result["dataset_coverage"]
        if not coverage.get("negative_conclusion_supported", False):
            assert result["status"] != "not_applies"


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
