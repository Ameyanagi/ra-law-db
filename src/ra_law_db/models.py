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
        "cwc": "化学兵器禁止法",
    },
    "en": {
        "cscl": "CSCL",
        "prtr": "PRTR",
        "poison_control": "Poison Control Act",
        "ish": "Industrial Safety and Health Law related",
        "cwc": "Chemical Weapons Convention Law",
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
    "cwc": {
        "ja": "化学兵器の禁止及び特定物質の規制等に関する法律",
        "en": "Act on the Prohibition of Chemical Weapons and the Regulation of Specific Chemicals",
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
