"""消防法 第4類 property classifier (別表第1 備考 thresholds, 危険物令 別表第3 指定数量)."""

from __future__ import annotations

import pytest

from ra_law_db.fire_service import classify_class4_from_ghs_category, classify_class4_from_properties


@pytest.mark.parametrize(
    ("kwargs", "item", "quantity"),
    [
        # アセトン: 引火点 −20 ℃, 沸点 56 ℃, 水溶性 → 第一石油類 水溶性 400 L (not 特殊引火物: 沸点 > 40)
        ({"flash_point_c": -20, "boiling_point_c": 56, "water_soluble": True}, "petroleum_1", 400),
        # ジエチルエーテル: 引火点 −45 ℃, 沸点 35 ℃ → 特殊引火物 50 L
        ({"flash_point_c": -45, "boiling_point_c": 35, "water_soluble": False}, "special_flammable", 50),
        # 二硫化炭素: 発火点 90 ℃ → 特殊引火物 regardless of flash point
        ({"flash_point_c": -30, "boiling_point_c": 46, "autoignition_c": 90}, "special_flammable", 50),
        # トルエン: 引火点 4 ℃, 非水溶性 → 第一石油類 200 L
        ({"flash_point_c": 4, "boiling_point_c": 111, "water_soluble": False}, "petroleum_1", 200),
        # 灯油: 引火点 40 ℃ → 第二石油類 非水溶性 1000 L
        ({"flash_point_c": 40, "water_soluble": False}, "petroleum_2", 1000),
        # 酢酸: 引火点 39 ℃, 水溶性 → 第二石油類 水溶性 2000 L
        ({"flash_point_c": 39, "water_soluble": True}, "petroleum_2", 2000),
        # 重油: 引火点 70 ℃ → 第三石油類 2000 L (boundary is inclusive at 70)
        ({"flash_point_c": 70, "water_soluble": False}, "petroleum_3", 2000),
        # グリセリン: 引火点 160 ℃, 水溶性 → 第三石油類 水溶性 4000 L
        ({"flash_point_c": 160, "water_soluble": True}, "petroleum_3", 4000),
        # ギヤー油: 引火点 220 ℃ → 第四石油類 6000 L
        ({"flash_point_c": 220}, "petroleum_4", 6000),
        # エタノール 95 % → アルコール類 400 L
        ({"flash_point_c": 13, "alcohol_c1_c3_pct": 95}, "alcohol", 400),
        # 動植物油
        ({"flash_point_c": 240, "animal_vegetable_oil": True}, "animal_vegetable_oil", 10000),
    ],
)
def test_thresholds_and_designated_quantities(kwargs: dict, item: str, quantity: int) -> None:
    result = classify_class4_from_properties(**kwargs)
    assert result.category_code == "hazmat_class_4"
    assert result.item_code == item
    assert result.designated_quantity_l == quantity
    assert result.class_source == "property_rule"
    assert "別表第1" in result.basis_ja


def test_flash_point_250_or_more_is_not_class_4() -> None:
    result = classify_class4_from_properties(flash_point_c=260)
    assert result.category_code == "not_class_4"
    assert result.designated_quantity_l is None


def test_solid_at_20c_is_not_class_4() -> None:
    result = classify_class4_from_properties(flash_point_c=100, liquid_at_20c=False)
    assert result.category_code == "not_class_4"


def test_unknown_water_solubility_gives_bracket_with_both_quantities() -> None:
    result = classify_class4_from_properties(flash_point_c=4)
    assert result.item_code == "petroleum_1"
    assert result.designated_quantity_l is None
    assert result.confidence == "bracket"
    assert any("200 L" in note and "400 L" in note for note in result.notes_ja)


def test_low_flash_point_without_boiling_point_flags_special_flammable_check() -> None:
    result = classify_class4_from_properties(flash_point_c=-30, water_soluble=False)
    assert result.item_code == "petroleum_1"
    assert any("特殊引火物" in note for note in result.notes_ja)


def test_missing_flash_point_is_undetermined() -> None:
    result = classify_class4_from_properties(None)
    assert result.confidence == "undetermined"
    assert result.item_code == ""


@pytest.mark.parametrize(
    ("category", "item", "confidence"),
    [(3, "petroleum_2", "bracket"), (2, "", "bracket"), (1, "", "bracket"), (None, "", "undetermined")],
)
def test_ghs_category_fallback(category: int | None, item: str, confidence: str) -> None:
    result = classify_class4_from_ghs_category(category)
    assert result.class_source == "ghs_category_bracket"
    assert result.item_code == item
    assert result.confidence == confidence
    assert result.to_dict()["category_code"] == "hazmat_class_4"
