"""lookup(context=SDS §9 properties) attaches the 消防法 第4類 品名 to the fire_service result."""

from __future__ import annotations

import pytest

from ra_law_db import LawScreeningDatabase


@pytest.fixture(scope="module")
def bundled_db() -> LawScreeningDatabase:
    LawScreeningDatabase.reset_instance()
    return LawScreeningDatabase.get_instance()


def _fire(payload: dict) -> dict:
    return next(item for item in payload["results"] if item["law_code"] == "fire_service")


def test_flash_point_context_classifies_acetone_as_petroleum_1(bundled_db: LawScreeningDatabase) -> None:
    payload = bundled_db.lookup(
        cas_number="67-64-1",
        language="ja",
        context={"flash_point_c": -20, "boiling_point_c": 56, "water_soluble": True},
    )
    fire = _fire(payload)
    class4 = next(item for item in fire["categories"] if item["code"] == "hazmat_class_4" and "item_code" in item)
    assert class4["item_code"] == "petroleum_1"
    assert class4["designated_quantity_l"] == 400
    # the statutory 品名 from the index is kept as the chip; the property rule corroborates it
    assert class4["class_source"] in {"reviewed_group_rule", "legal_row_join", "property_rule"}
    assert fire["property_screening"]["class_source"] == "property_rule"
    assert fire["status"] == "requires_context"
    assert fire["property_screening"]["confidence"] == "exact"
    assert any(item.get("kind") for item in fire["required_actions"])


def test_property_context_upgrades_a_not_listed_fire_result(bundled_db: LawScreeningDatabase) -> None:
    # Water is not on any 消防法 list; a (hypothetical) flammable product still gets classified.
    payload = bundled_db.lookup(cas_number="7732-18-5", language="ja", context={"flash_point_c": 45})
    fire = _fire(payload)
    assert fire["status"] == "requires_context"
    assert fire["status_reason_code"] == "PROPERTY_SCOPE_MATCH"
    class4 = next(item for item in fire["categories"] if item["code"] == "hazmat_class_4" and "item_code" in item)
    assert class4["item_code"] == "petroleum_2"
    assert class4["designated_quantity_l"] is None  # water solubility unknown → bracket
    assert class4["confidence"] == "bracket"


def test_high_flash_point_does_not_add_class4(bundled_db: LawScreeningDatabase) -> None:
    payload = bundled_db.lookup(cas_number="7732-18-5", language="ja", context={"flash_point_c": 300})
    fire = _fire(payload)
    assert all("item_code" not in item for item in fire["categories"])
    assert fire["property_screening"]["category_code"] == "not_class_4"


def test_ghs_category_fallback_when_no_flash_point(bundled_db: LawScreeningDatabase) -> None:
    payload = bundled_db.lookup(cas_number="7732-18-5", language="ja", context={"ghs_flammable_liquid_category": 3})
    fire = _fire(payload)
    class4 = next(item for item in fire["categories"] if item["code"] == "hazmat_class_4" and "item_code" in item)
    assert class4["item_code"] == "petroleum_2"
    assert class4["class_source"] == "ghs_category_bracket"


def test_no_property_context_leaves_result_untouched(bundled_db: LawScreeningDatabase) -> None:
    payload = bundled_db.lookup(cas_number="7732-18-5", language="ja")
    assert "property_screening" not in _fire(payload)


def test_index_rows_carry_official_control_concentration(bundled_db: LawScreeningDatabase) -> None:
    """管理濃度 (作業環境評価基準 別表) rides on the 特化則/有機則/鉛則 index rows."""
    payload = bundled_db.lookup(cas_number="108-88-3", language="ja")  # トルエン: 第二種有機溶剤, 20 ppm
    ish = next(item for item in payload["results"] if item["law_code"] == "ish")
    organic = next(item for item in ish["categories"] if item["code"] == "organic_type_2")
    assert organic["control_concentration"] == 20.0
    assert organic["control_concentration_unit"] == "ppm"

    payload = bundled_db.lookup(cas_number="7439-92-1", language="ja")  # 鉛: group row 鉛及びその化合物
    ish = next(item for item in payload["results"] if item["law_code"] == "ish")
    lead = next(item for item in ish["categories"] if item["code"] == "lead")
    assert lead["control_concentration"] == 0.05
    assert lead["control_concentration_unit"] == "mg/m³"
    assert lead["control_concentration_basis"] == "鉛として"


def test_oel_rows_carry_official_concentration_standard(bundled_db: LawScreeningDatabase) -> None:
    """濃度基準値 (MHLW 一覧) rides on the RJ_04_023 rows."""
    payload = bundled_db.lookup(cas_number="79-10-7", language="ja")  # アクリル酸: 八時間 2 ppm
    ish = next(item for item in payload["results"] if item["law_code"] == "ish")
    oel = next(item for item in ish["categories"] if item["code"] == "occupational_exposure_limit")
    assert oel["oel_8h"] == "2 ppm"
    assert oel["oel_stel"] is None
    assert oel["oel_effective"] == "2025-10-01"
