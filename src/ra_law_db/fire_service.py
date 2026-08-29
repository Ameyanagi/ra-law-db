"""消防法 第4類 (引火性液体) 品名 classification from physical properties.

The 品名 of a 第4類 危険物 is defined by 消防法 別表第1 備考 (十一〜十八) through 引火点・沸点・
発火点・水溶性 — not by chemical identity — so it cannot come from a substance list. This module
encodes the statutory thresholds and 危険物令 別表第3 指定数量 so a product can be classified from
its SDS section 9 values (or, as a weaker fallback, from a GHS 引火性液体 区分).

Every result carries ``class_source`` (``property_rule`` or ``ghs_category_bracket``) and the
statutory basis; nothing here is a legal determination — 特殊引火物 also depends on the 発火点 and
第二石油類 has the 可燃性液体量40%以下 exclusion, both of which are surfaced as notes.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass

LAW_ID = "323AC1000000186"  # 消防法
ORDER_ID = "334CO0000000306"  # 危険物の規制に関する政令

# 危険物令 別表第3: 第4類 指定数量 (L)
DESIGNATED_QUANTITY_L: dict[tuple[str, bool | None], int] = {
    ("special_flammable", None): 50,
    ("petroleum_1", False): 200,
    ("petroleum_1", True): 400,
    ("alcohol", None): 400,
    ("petroleum_2", False): 1000,
    ("petroleum_2", True): 2000,
    ("petroleum_3", False): 2000,
    ("petroleum_3", True): 4000,
    ("petroleum_4", None): 6000,
    ("animal_vegetable_oil", None): 10000,
}

LABEL_JA = {
    "special_flammable": "特殊引火物",
    "petroleum_1": "第一石油類",
    "alcohol": "アルコール類",
    "petroleum_2": "第二石油類",
    "petroleum_3": "第三石油類",
    "petroleum_4": "第四石油類",
    "animal_vegetable_oil": "動植物油類",
}


@dataclass
class HazmatClass4Result:
    category_code: str  # hazmat_class_4 or not_class_4
    item_code: str  # special_flammable | petroleum_1 | alcohol | petroleum_2 | petroleum_3 | petroleum_4 | animal_vegetable_oil | ""
    item_label_ja: str
    water_soluble: bool | None
    designated_quantity_l: int | None
    class_source: str
    basis_ja: str
    confidence: str  # exact | bracket | undetermined
    notes_ja: list[str]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def classify_class4_from_properties(
    flash_point_c: float | None,
    *,
    boiling_point_c: float | None = None,
    autoignition_c: float | None = None,
    water_soluble: bool | None = None,
    alcohol_c1_c3_pct: float | None = None,
    liquid_at_20c: bool | None = True,
    animal_vegetable_oil: bool = False,
) -> HazmatClass4Result:
    """Classify a liquid by 消防法 別表第1 備考 thresholds.

    ``flash_point_c`` is the 引火点 (℃). ``alcohol_c1_c3_pct`` is the mass % of C1–C3 saturated
    monohydric alcohol when the product is an alcohol solution (アルコール類 ≥ 60 %). Returns
    ``not_class_4`` when the data rule the product out (引火点 ≥ 250 ℃ or not a liquid at 20 ℃).
    """
    notes: list[str] = []
    if liquid_at_20c is False:
        return HazmatClass4Result(
            "not_class_4",
            "",
            "",
            water_soluble,
            None,
            "property_rule",
            "消防法 別表第1 備考十一（一気圧において温度二十度で液体であるもの）",
            "exact",
            ["20 ℃で液体でないため第4類には該当しない（他の類の該当性は別途）"],
        )
    if animal_vegetable_oil:
        return HazmatClass4Result(
            "hazmat_class_4",
            "animal_vegetable_oil",
            LABEL_JA["animal_vegetable_oil"],
            water_soluble,
            DESIGNATED_QUANTITY_L[("animal_vegetable_oil", None)],
            "property_rule",
            "消防法 別表第1 備考十八；危険物令 別表第3",
            "exact",
            ["動植物油類は引火点250 ℃未満のものに限る（備考十八）"],
        )
    if alcohol_c1_c3_pct is not None and alcohol_c1_c3_pct >= 60:
        return HazmatClass4Result(
            "hazmat_class_4",
            "alcohol",
            LABEL_JA["alcohol"],
            True,
            DESIGNATED_QUANTITY_L[("alcohol", None)],
            "property_rule",
            "消防法 別表第1 備考十三；危険物令 別表第3",
            "exact",
            ["炭素数1〜3の飽和一価アルコール（含有率60 %以上）はアルコール類（指定数量400 L）"],
        )
    if flash_point_c is None:
        return HazmatClass4Result(
            "hazmat_class_4",
            "",
            "",
            water_soluble,
            None,
            "property_rule",
            "消防法 別表第1 備考十一〜十七",
            "undetermined",
            ["引火点が不明のため品名を判定できない。SDS第9項の引火点・沸点を確認"],
        )

    # 特殊引火物: 発火点 ≤ 100 ℃, or 引火点 ≤ −20 ℃ かつ 沸点 ≤ 40 ℃
    if (autoignition_c is not None and autoignition_c <= 100) or (
        flash_point_c <= -20 and boiling_point_c is not None and boiling_point_c <= 40
    ):
        return HazmatClass4Result(
            "hazmat_class_4",
            "special_flammable",
            LABEL_JA["special_flammable"],
            water_soluble,
            DESIGNATED_QUANTITY_L[("special_flammable", None)],
            "property_rule",
            "消防法 別表第1 備考十二；危険物令 別表第3",
            "exact",
            notes,
        )
    if flash_point_c <= -20 and boiling_point_c is None:
        notes.append("引火点−20 ℃以下：沸点が40 ℃以下なら特殊引火物（指定数量50 L）。沸点を確認")
    if flash_point_c >= 250:
        return HazmatClass4Result(
            "not_class_4",
            "",
            "",
            water_soluble,
            None,
            "property_rule",
            "消防法 別表第1 備考十七（引火点250 ℃未満）",
            "exact",
            ["引火点250 ℃以上のため第4類危険物に該当しない（指定可燃物の該当性は別途）"],
        )
    if flash_point_c < 21:
        item = "petroleum_1"
        basis = "消防法 別表第1 備考十二（第一石油類：引火点21 ℃未満）"
    elif flash_point_c < 70:
        item = "petroleum_2"
        basis = "消防法 別表第1 備考十四（第二石油類：引火点21 ℃以上70 ℃未満）"
        notes.append("可燃性液体量40 %以下で引火点40 ℃以上・燃焼点60 ℃以上のものは除外（備考十四ただし書）")
    elif flash_point_c < 200:
        item = "petroleum_3"
        basis = "消防法 別表第1 備考十五（第三石油類：引火点70 ℃以上200 ℃未満）"
        notes.append("可燃性液体量40 %以下のものは除外（備考十五ただし書）")
    else:
        item = "petroleum_4"
        basis = "消防法 別表第1 備考十六（第四石油類：引火点200 ℃以上250 ℃未満）"
        notes.append("可燃性液体量40 %以下のものは除外（備考十六ただし書）")

    quantity: int | None
    confidence = "exact"
    if item == "petroleum_4":
        quantity = DESIGNATED_QUANTITY_L[(item, None)]
    elif water_soluble is None:
        quantity = None
        confidence = "bracket"
        notes.append(
            f"水溶性か否かで指定数量が変わる（非水溶性 {DESIGNATED_QUANTITY_L[(item, False)]} L／"
            f"水溶性 {DESIGNATED_QUANTITY_L[(item, True)]} L）。SDS第9項の溶解性を確認"
        )
    else:
        quantity = DESIGNATED_QUANTITY_L[(item, bool(water_soluble))]
    return HazmatClass4Result(
        "hazmat_class_4",
        item,
        LABEL_JA[item],
        water_soluble,
        quantity,
        "property_rule",
        basis + "；危険物令 別表第3",
        confidence,
        notes,
    )


GHS_FLAMMABLE_LIQUID_BRACKETS: dict[int, tuple[list[str], str]] = {
    # GHS 引火性液体 区分 → candidate 消防法 品名 (thresholds differ: GHS 23/60/93 ℃ vs 消防法 21/70/200 ℃)
    1: (
        ["special_flammable", "petroleum_1"],
        "引火点23 ℃未満かつ沸点35 ℃以下：特殊引火物（沸点40 ℃以下・引火点−20 ℃以下）又は第一石油類",
    ),
    2: (["petroleum_1", "petroleum_2"], "引火点23 ℃未満かつ沸点35 ℃超：ほぼ第一石油類（引火点21〜23 ℃は第二石油類）"),
    3: (["petroleum_2"], "引火点23 ℃以上60 ℃以下：第二石油類"),
    4: (["petroleum_2", "petroleum_3"], "引火点60 ℃超93 ℃以下：第二石油類（70 ℃未満）又は第三石油類（70 ℃以上）"),
}


def classify_class4_from_ghs_category(category: int | None) -> HazmatClass4Result:
    """Weaker fallback when only the GHS 引火性液体 区分 is known (e.g. from NITE's GHS list)."""
    if category not in GHS_FLAMMABLE_LIQUID_BRACKETS:
        return HazmatClass4Result(
            "hazmat_class_4",
            "",
            "",
            None,
            None,
            "ghs_category_bracket",
            "消防法 別表第1 備考十一〜十七",
            "undetermined",
            ["GHS引火性液体区分が不明。SDS第9項の引火点を確認"],
        )
    candidates, note = GHS_FLAMMABLE_LIQUID_BRACKETS[category]
    item = candidates[0] if len(candidates) == 1 else ""
    quantity = None
    if item:
        quantity = DESIGNATED_QUANTITY_L.get((item, False))
    return HazmatClass4Result(
        "hazmat_class_4",
        item,
        LABEL_JA.get(item, "／".join(LABEL_JA[c] for c in candidates)),
        None,
        quantity,
        "ghs_category_bracket",
        "GHS 引火性液体 区分 → 消防法 別表第1 備考の閾値対応",
        "bracket",
        [note, "指定数量は非水溶性の値。水溶性なら2倍。SDS第9項で引火点・沸点・溶解性を確認して確定"],
    )
