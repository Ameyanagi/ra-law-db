"""Per-category legal obligations (義務・法定管理措置) and related runtime constants."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from importlib import resources
from pathlib import Path
from typing import Any

OBLIGATIONS_FILE = "obligations.csv"
OBLIGATION_KINDS = ("mandatory", "conditional", "info")

# Virtual category codes: attached to a result when any matched index category carries the flag.
SPECIAL_MANAGEMENT_CODE = "special_management"
SPECIAL_ORGANIC_CODE = "special_organic"
# Narcotics: the index publishes one code (``controlled_narcotic``) for 麻薬・向精神薬 and for
# 麻薬向精神薬原料 alike; the runtime resolves it from ``legal_number`` (法別表 / 指定政令の条) into
# ``narcotic_scheduled`` (本体: 別表第1〜3, 指定政令第1〜4条) or ``narcotic_precursor`` (原料: 別表第4,
# 指定政令第5条). Rows without a legal number stay on the unresolved ``controlled_narcotic`` code.
NARCOTIC_SCHEDULED_CODE = "narcotic_scheduled"
NARCOTIC_PRECURSOR_CODE = "narcotic_precursor"
NARCOTIC_PRECURSOR_MARKERS = ("別表第4", "別表第四", "指定政令第5条", "指定政令第五条")
NARCOTIC_SCHEDULED_MARKERS = (
    "別表第1",
    "別表第一",
    "別表第2",
    "別表第二",
    "別表第3",
    "別表第三",
    "指定政令第1条",
    "指定政令第一条",
    "指定政令第2条",
    "指定政令第二条",
    "指定政令第3条",
    "指定政令第三条",
    "指定政令第4条",
    "指定政令第四条",
)

# ``class_source`` value for classes inferred from the legacy 琉球大 layer (not from the index).
LEGACY_CLASS_SOURCE = "legacy_regulatory_substances"
# ``class_source`` value for the narcotics virtual codes above.
LEGAL_NUMBER_CLASS_SOURCE = "legal_number"

# Category codes meaning "listed, but class unresolved" (contract §1) and the resolved codes of the
# same family that supersede them when both are matched for one CAS.
UNRESOLVED_CLASS_FAMILIES: dict[str, tuple[str, ...]] = {
    "tokka": ("tokka_class_1", "tokka_class_2", "tokka_class_3"),
    "organic_solvent": ("organic_type_1", "organic_type_2", "organic_type_3"),
    "poison_control_listed": ("poison", "deleterious", "specific_poison"),
    "prtr_current": ("first_class", "specific_first_class", "second_class"),
    "cwc_listed": ("schedule_1", "schedule_2", "schedule_3"),
    "hazardous_material": tuple(f"hazmat_class_{index}" for index in range(1, 7)),
    "controlled_narcotic": (NARCOTIC_SCHEDULED_CODE, NARCOTIC_PRECURSOR_CODE),
}
UNRESOLVED_CLASS_CODES = set(UNRESOLVED_CLASS_FAMILIES) - {"controlled_narcotic"}

# Classes whose mandatory obligations make a law a "hard duty" law (contract §4).
HARD_DUTY_CLASSES = {
    "tokka_class_1",
    "tokka_class_2",
    "tokka",
    "organic_type_1",
    "organic_type_2",
    "lead",
    "prohibited",
    "manufacture_permission",
    "poison",
    "deleterious",
    "specific_poison",
    "poison_control_listed",
    "schedule_1",
    "controlled_narcotic",
    NARCOTIC_SCHEDULED_CODE,
}

ACTION_ITEM_KEYS = ("action_code", "label", "required", "kind", "basis", "condition", "owner_hint")
KIND_RANK = {"mandatory": 2, "conditional": 1, "info": 0}

PERMITTED_LIST_LAWS = {"food_contact"}

# Static fixes for the legacy (琉球大) layer; key = (raw cas, name prefix). None = drop the row.
LEGACY_CAS_OVERRIDES: dict[tuple[str, str], str | None] = {
    ("―", "塩素化ビフェニル"): "1336-36-3",
    ("―", "コールタールナフサ"): "8030-30-6",
    ("―", "コールタール"): "8007-45-2",
    ("―", "リフラクトリーセラミックファイバー"): "142844-00-6",
    ("―", "石油ベンジン"): "8032-32-4",
    ("7784-46- 5", ""): "7784-46-5",
    ("409-21-2a", ""): "409-21-2",
}

REQUIRED_CONTEXT_LABELS: dict[str, tuple[str, str]] = {
    "handling_amount": ("取扱量", "Handling amount"),
    "handling_quantity": ("取扱量", "Handling quantity"),
    "work_process": ("作業工程", "Work process"),
    "exposure_route": ("ばく露経路", "Exposure route"),
    "ventilation_control": ("換気・局所排気の有無", "Ventilation control"),
    "workplace": ("作業場所（屋内・屋外）", "Workplace"),
    "concentration": ("含有率・濃度", "Concentration"),
    "annual_quantity": ("年間数量", "Annual quantity"),
    "annual_handling_quantity": ("年間取扱量", "Annual handling quantity"),
    "annual_handling_tons": ("年間取扱量（t）", "Annual handling (t)"),
    "business_type": ("事業区分・業種", "Business type"),
    "release_or_transfer": ("排出・移動の有無", "Release or transfer"),
    "manufacture_or_import": ("製造・輸入の有無", "Manufacture or import"),
    "manufacture": ("製造の有無", "Manufacture"),
    "import": ("輸入の有無", "Import"),
    "export": ("輸出の有無", "Export"),
    "import_or_export": ("輸出入の有無", "Import or export"),
    "production_or_use": ("製造・使用の別", "Production or use"),
    "use": ("用途", "Use"),
    "intended_use": ("使用目的", "Intended use"),
    "end_use": ("最終用途", "End use"),
    "quantity": ("数量", "Quantity"),
    "substance_identity": ("物質同一性（塩・異性体・群指定）", "Substance identity"),
    "formulation": ("製剤形態", "Formulation"),
    "salt_or_isomer": ("塩・異性体の別", "Salt or isomer"),
    "exemption_conditions": ("除外条件（除外濃度等）", "Exemption conditions"),
    "material_form": ("材料形態（粉体・液体等）", "Material form"),
    "dust_generation": ("粉じん発生の有無", "Dust generation"),
    "work_frequency": ("作業頻度", "Work frequency"),
    "facility": ("施設・設備", "Facility"),
    "covered_work": ("対象業務への従事", "Covered work"),
    "assignment_history": ("配置歴", "Assignment history"),
    "medical_exam_history": ("健診受診歴", "Medical examination history"),
    "worker_category": ("労働者区分", "Worker category"),
    "waste_form": ("廃棄物の性状", "Waste form"),
    "hazardous_characteristics": ("有害特性", "Hazardous characteristics"),
    "disposal_method": ("処理方法", "Disposal method"),
    "process": ("工程", "Process"),
    "emission_process": ("排出工程", "Emission process"),
    "emission_quantity": ("排出量", "Emission quantity"),
    "local_rule": ("自治体条例・上乗せ基準", "Local rule"),
    "local_ordinance": ("市町村条例", "Local ordinance"),
    "physical_properties": ("物性（引火点等）", "Physical properties"),
    "physical_state": ("状態（気体・液体）", "Physical state"),
    "storage_quantity": ("貯蔵量", "Storage quantity"),
    "storage": ("貯蔵方法", "Storage"),
    "notification": ("届出の要否", "Notification"),
    "pressure": ("圧力", "Pressure"),
    "temperature": ("温度", "Temperature"),
    "refrigerant": ("冷媒の種類", "Refrigerant"),
    "refrigeration_capacity": ("冷凍能力", "Refrigeration capacity"),
    "composition": ("組成", "Composition"),
    "license": ("免許・許可", "Licence"),
    "research_or_medical": ("研究用・医療用の別", "Research or medical use"),
    "cosmetic_product_type": ("化粧品の種類", "Cosmetic product type"),
    "ingredient_role": ("配合目的", "Ingredient role"),
    "product_claims": ("効能表示", "Product claims"),
    "product_type": ("製品種類", "Product type"),
    "intended_consumer_use": ("家庭用品としての用途", "Intended consumer use"),
    "food_contact_use": ("食品接触用途", "Food-contact use"),
    "material_category": ("材質区分", "Material category"),
    "maximum_temperature": ("最高使用温度", "Maximum temperature"),
    "food_type": ("食品の種類", "Food type"),
    "migration_conditions": ("溶出条件", "Migration conditions"),
    "land_use": ("土地利用", "Land use"),
    "facility_history": ("施設履歴", "Facility history"),
    "soil_concentration": ("土壌中濃度", "Soil concentration"),
    "excavation_or_transfer": ("掘削・搬出の有無", "Excavation or transfer"),
    "effluent": ("排水の有無", "Effluent"),
    "discharge_destination": ("排出先（公共用水域・下水道）", "Discharge destination"),
}


@dataclass(frozen=True)
class ObligationRow:
    """One obligation (措置) attached to a (law_code, category_code) pair."""

    law_code: str
    category_code: str
    seq: int
    kind: str
    label_ja: str
    label_en: str
    basis_ja: str
    condition_ja: str
    threshold_note: str
    owner_hint: str
    health_check_type: str
    health_check_interval: str
    health_record_years: int | None
    measurement_interval: str
    measurement_record_years: int | None
    work_record_years: int | None
    supervisor_ja: str
    notice_ja: str

    @property
    def action_code(self) -> str:
        return f"{self.law_code}_{self.category_code}_{self.seq}"

    def label(self, language: str) -> str:
        if language == "ja":
            return self.label_ja
        return self.label_en or self.label_ja


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _int_or_none(value: Any) -> int | None:
    text = _text(value)
    if not text:
        return None
    try:
        return int(float(text))
    except ValueError:
        return None


def obligation_row_from_mapping(record: dict[str, Any]) -> ObligationRow | None:
    """Build a row from a CSV/SQLite mapping; returns None for unusable rows."""
    law_code = _text(record.get("law_code"))
    category_code = _text(record.get("category_code"))
    label_ja = _text(record.get("label_ja"))
    if not law_code or not category_code or not label_ja:
        return None
    kind = _text(record.get("kind")) or "mandatory"
    if kind not in OBLIGATION_KINDS:
        kind = "conditional"
    return ObligationRow(
        law_code=law_code,
        category_code=category_code,
        seq=_int_or_none(record.get("seq")) or 0,
        kind=kind,
        label_ja=label_ja,
        label_en=_text(record.get("label_en")),
        basis_ja=_text(record.get("basis_ja")),
        condition_ja=_text(record.get("condition_ja")),
        threshold_note=_text(record.get("threshold_note")),
        owner_hint=_text(record.get("owner_hint")),
        health_check_type=_text(record.get("health_check_type")),
        health_check_interval=_text(record.get("health_check_interval")),
        health_record_years=_int_or_none(record.get("health_record_years")),
        measurement_interval=_text(record.get("measurement_interval")),
        measurement_record_years=_int_or_none(record.get("measurement_record_years")),
        work_record_years=_int_or_none(record.get("work_record_years")),
        supervisor_ja=_text(record.get("supervisor_ja")),
        notice_ja=_text(record.get("notice_ja")),
    )


def load_obligations_csv(path: Path) -> list[ObligationRow]:
    with open(path, encoding="utf-8", newline="") as handle:
        rows = [obligation_row_from_mapping(record) for record in csv.DictReader(handle)]
    return [row for row in rows if row is not None]


def load_builtin_obligations() -> list[ObligationRow]:
    """Packaged copy of masters/obligations.csv (last-resort fallback)."""
    resource = resources.files("ra_law_db").joinpath("data").joinpath(OBLIGATIONS_FILE)
    if not resource.is_file():
        return []
    with resources.as_file(resource) as path:
        return load_obligations_csv(Path(path))


def index_obligations(rows: list[ObligationRow]) -> dict[str, dict[str, list[ObligationRow]]]:
    indexed: dict[str, dict[str, list[ObligationRow]]] = {}
    for row in sorted(rows, key=lambda item: (item.law_code, item.category_code, item.seq)):
        indexed.setdefault(row.law_code, {}).setdefault(row.category_code, []).append(row)
    return indexed


def select_obligations(
    by_category: dict[str, list[ObligationRow]] | None,
    category_codes: list[str],
) -> list[ObligationRow]:
    """Rows for the matched categories in match order; the '*' rows when nothing matches."""
    if not by_category:
        return []
    selected: list[ObligationRow] = []
    seen_codes: set[str] = set()
    for code in category_codes:
        if code in seen_codes:
            continue
        seen_codes.add(code)
        selected.extend(by_category.get(code, []))
    if not selected:
        selected = list(by_category.get("*", []))
    return selected


def narcotic_class_code(legal_text: str | None) -> str | None:
    """Resolve a narcotics index row into 本体 / 原料 from its 法別表 / 指定政令 citation."""
    text = legal_text or ""
    if any(marker in text for marker in NARCOTIC_PRECURSOR_MARKERS):
        return NARCOTIC_PRECURSOR_CODE
    if any(marker in text for marker in NARCOTIC_SCHEDULED_MARKERS):
        return NARCOTIC_SCHEDULED_CODE
    return None


def drop_superseded_unresolved(codes: list[str]) -> list[str]:
    """Drop an unresolved family code when a resolved class of the same family is also matched."""
    present = set(codes)
    superseded = {
        unresolved
        for unresolved, resolved in UNRESOLVED_CLASS_FAMILIES.items()
        if unresolved in present and present & set(resolved)
    }
    return [code for code in codes if code not in superseded]


def normalize_action_item(item: dict[str, Any]) -> dict[str, Any]:
    """Give every ``required_actions`` item the contract keys (generic guidance items are ``info``)."""
    item.setdefault("kind", "info")
    item.setdefault("required", item["kind"] == "mandatory")
    for key in ("basis", "condition", "owner_hint"):
        item.setdefault(key, None)
    return item


def build_action_items(rows: list[ObligationRow], language: str) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        code = row.action_code
        if code in seen:
            continue
        seen.add(code)
        items.append(
            {
                "action_code": code,
                "label": row.label(language),
                "required": row.kind == "mandatory",
                "kind": row.kind,
                "basis": row.basis_ja or None,
                "condition": row.condition_ja or None,
                "owner_hint": row.owner_hint or None,
                "category_code": row.category_code,
                "threshold_note": row.threshold_note or None,
            }
        )
    return items


def empty_management() -> dict[str, list[dict[str, Any]]]:
    return {"health_checks": [], "measurements": [], "work_records": [], "supervisors": [], "notices": []}


def _merge_kind(existing: str | None, addition: str) -> str:
    if existing is None or KIND_RANK.get(addition, 0) > KIND_RANK.get(existing, 0):
        return addition
    return existing


def _join_basis(existing: str | None, addition: str) -> str | None:
    if not addition:
        return existing
    if not existing:
        return addition
    if addition in existing:
        return existing
    return f"{existing}；{addition}"


def build_management(rows: list[ObligationRow]) -> dict[str, list[dict[str, Any]]]:
    """Aggregate 健診・測定・記録・作業主任者・掲示 from obligation rows (dedupe, max record years).

    Each entry carries ``kind`` (strongest of the contributing rows) so conditional duties
    (e.g. 第三類物質の特定化学設備がある場合の作業主任者) are distinguishable from mandatory ones.
    """
    management = empty_management()
    health_by_type: dict[str, dict[str, Any]] = {}
    measurement_by_interval: dict[str, dict[str, Any]] = {}
    work_records: dict[tuple[int, str], dict[str, Any]] = {}
    supervisors: dict[str, dict[str, Any]] = {}
    notices: dict[str, dict[str, Any]] = {}

    for row in rows:
        if row.health_check_type:
            entry = health_by_type.setdefault(
                row.health_check_type,
                {"type": row.health_check_type, "interval": "", "record_years": None, "basis": None, "kind": None},
            )
            entry["interval"] = entry["interval"] or row.health_check_interval
            if row.health_record_years is not None:
                entry["record_years"] = max(entry["record_years"] or 0, row.health_record_years)
            entry["basis"] = _join_basis(entry["basis"], row.basis_ja)
            entry["kind"] = _merge_kind(entry["kind"], row.kind)
        if row.measurement_interval:
            entry = measurement_by_interval.setdefault(
                row.measurement_interval,
                {"interval": row.measurement_interval, "record_years": None, "basis": None, "kind": None},
            )
            if row.measurement_record_years is not None:
                entry["record_years"] = max(entry["record_years"] or 0, row.measurement_record_years)
            entry["basis"] = _join_basis(entry["basis"], row.basis_ja)
            entry["kind"] = _merge_kind(entry["kind"], row.kind)
        if row.work_record_years is not None:
            key = (row.work_record_years, row.basis_ja)
            entry = work_records.setdefault(
                key, {"years": row.work_record_years, "basis": row.basis_ja or None, "kind": None}
            )
            entry["kind"] = _merge_kind(entry["kind"], row.kind)
        if row.supervisor_ja:
            entry = supervisors.setdefault(row.supervisor_ja, {"title": row.supervisor_ja, "basis": None, "kind": None})
            entry["basis"] = _join_basis(entry["basis"], row.basis_ja)
            entry["kind"] = _merge_kind(entry["kind"], row.kind)
        if row.notice_ja:
            entry = notices.setdefault(row.notice_ja, {"text": row.notice_ja, "basis": None, "kind": None})
            entry["basis"] = _join_basis(entry["basis"], row.basis_ja)
            entry["kind"] = _merge_kind(entry["kind"], row.kind)

    management["health_checks"] = list(health_by_type.values())
    management["measurements"] = list(measurement_by_interval.values())
    management["work_records"] = list(work_records.values())
    management["supervisors"] = list(supervisors.values())
    management["notices"] = list(notices.values())
    return management


def management_is_empty(management: dict[str, list[Any]]) -> bool:
    return not any(management.values())


def required_context_items(keys: list[str], language: str) -> list[dict[str, str]]:
    items = []
    for key in keys:
        label_ja, label_en = REQUIRED_CONTEXT_LABELS.get(key, (key, key.replace("_", " ")))
        items.append(
            {
                "key": key,
                "label_ja": label_ja,
                "label_en": label_en,
                "label": label_ja if language == "ja" else label_en,
            }
        )
    return items


def apply_legacy_cas_override(cas_number: str, name_ja: str) -> str | None:
    """Return the corrected CAS for a legacy row, or None when the row must be dropped."""
    name = (name_ja or "").strip()
    if "削除" in name:
        return None
    raw = (cas_number or "").strip()
    for (raw_key, prefix), replacement in LEGACY_CAS_OVERRIDES.items():
        if raw == raw_key and (not prefix or name.startswith(prefix)):
            return replacement
    return raw.replace(" ", "")
