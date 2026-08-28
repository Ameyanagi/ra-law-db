"""Domain models for law screening."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

LawStatus = Literal["applies", "not_applies", "requires_context", "unknown"]


LAW_LABELS = {
    "ja": {
        "cscl": "化審法",
        "prtr": "化管法(PRTR)",
        "poison_control": "毒劇法",
        "ish": "労働安全衛生法関連規制",
        "waste": "廃棄物処理法",
        "cwc": "化学兵器禁止法",
        "dust_rule": "粉じん障害防止規則・じん肺法",
        "occupational_health": "特殊健康診断・じん肺健康診断",
        "ozone_layer": "オゾン層保護法",
        "air_pollution": "大気汚染防止法",
        "water_pollution": "水質汚濁防止法",
        "soil_contamination": "土壌汚染対策法",
        "household_products": "家庭用品規制法",
        "high_pressure_gas": "高圧ガス保安法",
        "explosives": "火薬類取締法",
        "fire_service": "消防法",
        "narcotics": "麻薬及び向精神薬取締法",
        "food_contact": "食品衛生法（器具・容器包装）",
        "cosmetics": "薬機法・化粧品基準",
    },
    "en": {
        "cscl": "CSCL",
        "prtr": "PRTR",
        "poison_control": "Poison Control Act",
        "ish": "Industrial Safety and Health Law related",
        "waste": "Waste Management Act",
        "cwc": "Chemical Weapons Convention Law",
        "dust_rule": "Dust Ordinance and Pneumoconiosis Act",
        "occupational_health": "Occupational and pneumoconiosis health examinations",
        "ozone_layer": "Ozone Layer Protection Act",
        "air_pollution": "Air Pollution Control Act",
        "water_pollution": "Water Pollution Control Act",
        "soil_contamination": "Soil Contamination Countermeasures Act",
        "household_products": "Household Products Regulation",
        "high_pressure_gas": "High Pressure Gas Safety Act",
        "explosives": "Explosives Control Act",
        "fire_service": "Fire Service Act",
        "narcotics": "Narcotics and Psychotropics Control Act",
        "food_contact": "Food Sanitation Act (food-contact materials)",
        "cosmetics": "PMD Act / Standards for Cosmetics",
    },
}

LAW_STANDARD_NAMES = {
    "cscl": {
        "ja": "化学物質の審査及び製造等の規制に関する法律",
        "en": "Act on the Regulation of Manufacture and Evaluation of Chemical Substances",
    },
    "prtr": {
        "ja": "特定化学物質の環境への排出量の把握等及び管理の改善の促進に関する法律",
        "en": "Act on Confirmation, etc. of Release Amounts of Specific Chemical Substances in the Environment and Promotion of Improvements to the Management Thereof",
    },
    "poison_control": {
        "ja": "毒物及び劇物取締法",
        "en": "Poisonous and Deleterious Substances Control Act",
    },
    "ish": {
        "ja": "労働安全衛生法関連規制",
        "en": "Industrial Safety and Health Law related regulations",
    },
    "waste": {
        "ja": "廃棄物の処理及び清掃に関する法律",
        "en": "Waste Management and Public Cleansing Act",
    },
    "cwc": {
        "ja": "化学兵器の禁止及び特定物質の規制等に関する法律",
        "en": "Act on the Prohibition of Chemical Weapons and the Regulation of Specific Chemicals",
    },
    "dust_rule": {
        "ja": "粉じん障害防止規則及びじん肺法",
        "en": "Ordinance on Prevention of Hazards Due to Dust and Pneumoconiosis Act",
    },
    "occupational_health": {
        "ja": "労働安全衛生法に基づく特殊健康診断及びじん肺法に基づく健康診断",
        "en": "Special medical examinations under the ISH Act and pneumoconiosis examinations",
    },
    "ozone_layer": {"ja": "特定物質等の規制等によるオゾン層の保護に関する法律", "en": "Ozone Layer Protection Act"},
    "air_pollution": {"ja": "大気汚染防止法", "en": "Air Pollution Control Act"},
    "water_pollution": {"ja": "水質汚濁防止法", "en": "Water Pollution Control Act"},
    "soil_contamination": {"ja": "土壌汚染対策法", "en": "Soil Contamination Countermeasures Act"},
    "household_products": {
        "ja": "有害物質を含有する家庭用品の規制に関する法律",
        "en": "Act on Control of Household Products Containing Harmful Substances",
    },
    "high_pressure_gas": {"ja": "高圧ガス保安法", "en": "High Pressure Gas Safety Act"},
    "explosives": {"ja": "火薬類取締法", "en": "Explosives Control Act"},
    "fire_service": {"ja": "消防法", "en": "Fire Service Act"},
    "narcotics": {"ja": "麻薬及び向精神薬取締法", "en": "Narcotics and Psychotropics Control Act"},
    "food_contact": {"ja": "食品衛生法", "en": "Food Sanitation Act"},
    "cosmetics": {
        "ja": "医薬品、医療機器等の品質、有効性及び安全性の確保等に関する法律",
        "en": "Pharmaceuticals and Medical Devices Act",
    },
}


@dataclass
class SnapshotMetadata:
    """Law source snapshot metadata."""

    law_id: str
    source_url: str
    fetched_at: str
    content_hash: str


@dataclass
class RegulatoryRow:
    """Regulatory export row from ra-law-db/exports/regulatory_substances.csv."""

    cas_number: str
    name_ja: str
    name_en: str
    regulation_type: str
    regulation_class: int
    regulation_label: str
    law_name_ja: str
    law_name_en: str
    health_check_required: bool
    health_check_type: str
    health_check_interval: str
    health_check_ref: str
    record_retention_years: int
    special_management: bool = False
    special_organic: bool = False
    carcinogen: bool = False
    control_concentration: float | None = None
    control_concentration_unit: str = ""
    threshold_pct: str = ""
    work_env_measurement_required: bool = False


@dataclass
class MappingQuality:
    """Mapping quality metadata linked from ra-law-db/mappings/cas_mappings.jsonl."""

    cas_number: str
    regulation_type: str
    match_method: str
    confidence: float


@dataclass
class MasterLawRow:
    """Optional law master row (CSCL/Poison/CWC)."""

    cas_number: str
    category: str
    label_ja: str
    label_en: str
    law_name_ja: str
    law_name_en: str
    law_reference: str
    law_id: str


@dataclass
class IndexedLawRow:
    """Authoritative positive-list row published from NITE-CHRIP."""

    law_code: str
    category_code: str
    category_label_ja: str
    category_label_en: str
    cas_number: str
    substance_name: str
    chrip_id: str
    identity_note: str
    law_name_ja: str
    law_name_en: str
    law_id: str
    law_reference: str
    dataset_name: str
    dataset_version: str
    source_id: str
    source_url: str
    source_update_date: str
    snapshot_date: str
    mapping_method: str
    confidence: float
    required_context: tuple[str, ...]
