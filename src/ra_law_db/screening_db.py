"""Law-screening data access and lookup logic."""

from __future__ import annotations

import csv
import json
import re
import sqlite3
import unicodedata
from contextlib import ExitStack
from difflib import SequenceMatcher
from importlib import resources
from pathlib import Path
from typing import Any

from .models import LAW_LABELS, LAW_STANDARD_NAMES, MappingQuality, MasterLawRow, RegulatoryRow, SnapshotMetadata

SUPPORTED_LAW_CODES = ("cscl", "prtr", "poison_control", "ish", "cwc")
SEARCH_MODES = {"auto", "name", "cas"}
CAS_PATTERN = re.compile(r"^\d{2,7}-\d{2}-\d$")

MASTER_DATASET_FILES = {
    "cscl": "cscl_substances.csv",
    "poison_control": "poison_control_substances.csv",
    "cwc": "cwc_substances.csv",
}
ALIAS_MASTER_FILE = "substance_aliases.csv"
MASTER_COVERAGE_FILE = "master_coverage.json"

PRTR_CATEGORY_MAP = {
    "prtr1": {
        "code": "first_class",
        "ja": "第一種指定化学物質",
        "en": "First Class Designated Chemical Substance",
    },
    "prtr2": {
        "code": "second_class",
        "ja": "第二種指定化学物質",
        "en": "Second Class Designated Chemical Substance",
    },
}

REGULATION_STANDARD_NAMES = {
    "tokka": {
        "ja": "特定化学物質障害予防規則",
        "en": "Ordinance on Prevention of Hazards Due to Specified Chemical Substances",
    },
    "organic": {
        "ja": "有機溶剤中毒予防規則",
        "en": "Ordinance on Prevention of Organic Solvent Poisoning",
    },
    "lead": {
        "ja": "鉛中毒予防規則",
        "en": "Ordinance on Prevention of Lead Poisoning",
    },
    "prohibited": {
        "ja": "製造等が禁止される有害物",
        "en": "Prohibited Hazardous Substances for Manufacture etc.",
    },
    "waste": {
        "ja": "廃棄物の処理及び清掃に関する法律関連",
        "en": "Waste Management and Public Cleansing Act related controls",
    },
    "prtr1": {
        "ja": "化学物質排出把握管理促進法 第一種指定化学物質",
        "en": "PRTR Act First Class Designated Chemical Substance",
    },
    "prtr2": {
        "ja": "化学物質排出把握管理促進法 第二種指定化学物質",
        "en": "PRTR Act Second Class Designated Chemical Substance",
    },
}

ISH_CONTEXT_FIELDS = [
    "handling_amount",
    "work_process",
    "exposure_route",
    "ventilation_control",
]

ISH_TYPES = {"tokka", "organic", "lead", "prohibited"}

STATUS_REASON_CODES = {
    "missing_input": "MISSING_LOOKUP_INPUT",
    "no_cas_candidate": "NO_CAS_CANDIDATE",
    "missing_master_dataset": "MISSING_MASTER_DATASET",
    "incomplete_master_dataset": "INCOMPLETE_MASTER_DATASET",
    "no_dataset_hit": "NO_DATASET_HIT",
    "matched_master": "MATCHED_MASTER",
    "matched_context_required": "MATCHED_CONTEXT_REQUIRED",
}

MISSING_MASTER_ACTIONS = {
    "ja": [
        "未搭載マスタを ra-law-db/masters に追加する",
        "追加完了まではSDS第15項と法令別表でCASを手動照合する",
        "判定完了まで当該法令を要確認扱いにする",
    ],
    "en": [
        "Add the missing master dataset under ra-law-db/masters",
        "Until then, cross-check CAS manually using SDS section 15 and legal tables",
        "Treat this law as pending verification until data is loaded",
    ],
}

MISSING_MASTER_LAW_ACTIONS = {
    "cscl": {
        "ja": [
            "第一種特定・第二種特定・監視・優先評価をCASで照合する",
            "該当区分をSDS第15項と社内管理票へ反映する",
        ],
        "en": [
            "Cross-check CAS against Class I/II specified, monitoring, and priority assessment categories",
            "Reflect the confirmed category in SDS section 15 and internal control records",
        ],
    },
    "poison_control": {
        "ja": [
            "毒物・劇物・特定毒物をCASで照合する",
            "該当区分に応じて表示・施錠保管・譲渡記録を実施する",
        ],
        "en": [
            "Cross-check CAS against poison, deleterious, and specific poison categories",
            "Apply labeling, locked storage, and transfer-record controls for the matched category",
        ],
    },
    "cwc": {
        "ja": [
            "Schedule 1/2/3 をCASで照合する",
            "該当時は届出/許可要件と輸出入の用途確認を実施する",
        ],
        "en": [
            "Cross-check CAS against CWC Schedules 1/2/3",
            "If matched, confirm permit/notification requirements and end-use checks for trade",
        ],
    },
}

MISSING_MASTER_TARGETS = {
    "cscl": [
        {"code": "class_i_specified", "ja": "第一種特定化学物質", "en": "Class I Specified Chemical Substance"},
        {"code": "class_ii_specified", "ja": "第二種特定化学物質", "en": "Class II Specified Chemical Substance"},
        {"code": "monitoring", "ja": "監視化学物質", "en": "Monitoring Chemical Substance"},
        {"code": "priority_evaluation", "ja": "優先評価化学物質", "en": "Priority Assessment Chemical Substance"},
    ],
    "poison_control": [
        {"code": "poison", "ja": "毒物", "en": "Poison"},
        {"code": "deleterious", "ja": "劇物", "en": "Deleterious Substance"},
        {"code": "specific_poison", "ja": "特定毒物", "en": "Specific Poison"},
    ],
    "cwc": [
        {"code": "schedule_1", "ja": "Schedule 1", "en": "Schedule 1"},
        {"code": "schedule_2", "ja": "Schedule 2", "en": "Schedule 2"},
        {"code": "schedule_3", "ja": "Schedule 3", "en": "Schedule 3"},
    ],
}

MISSING_MASTER_SOURCE_HINTS = {
    "cscl": {
        "ja": ["NITE CHRIP 化審法分類", "e-Gov 化審法関連別表"],
        "en": ["NITE CHRIP CSCL classification", "e-Gov CSCL appendix tables"],
    },
    "poison_control": {
        "ja": ["厚労省 毒劇物リスト", "e-Gov 毒劇法別表"],
        "en": ["MHLW poison/deleterious lists", "e-Gov poison-control appendices"],
    },
    "cwc": {
        "ja": ["経産省 CWC Schedule リスト", "e-Gov 化学兵器禁止法関連別表"],
        "en": ["METI CWC schedule lists", "e-Gov CWC-law appendices"],
    },
}

NO_CANDIDATE_ACTIONS = {
    "ja": ["CAS番号または物質名を確認し再照合する"],
    "en": ["Confirm the CAS number or substance name and retry lookup"],
}

NOT_APPLIES_ACTIONS = {
    "ja": ["非該当判定を記録し、組成・用途変更時に再判定する"],
    "en": ["Record non-applicability and re-screen when composition/use changes"],
}

LAW_APPLIES_ACTIONS = {
    "cscl": {
        "ja": [
            "区分(第一種特定・第二種特定・監視・優先評価)を確定する",
            "製造・輸入前に化審法の届出/許可要件を確認する",
            "SDSと社内管理票に化審法区分を反映する",
        ],
        "en": [
            "Determine the CSCL category (Class I/II specified, monitoring, priority assessment)",
            "Confirm notification/permit obligations before manufacture or import",
            "Reflect the CSCL category in SDS and internal control records",
        ],
    },
    "prtr": {
        "ja": [
            "年間取扱量と事業区分を確認する",
            "届出要件に該当する場合はPRTR届出を実施する",
            "SDSと排出・移動量管理台帳を更新する",
        ],
        "en": [
            "Confirm annual handling volume and business category",
            "Submit PRTR reports when reporting thresholds are met",
            "Update SDS and release/transfer management records",
        ],
    },
    "poison_control": {
        "ja": [
            "毒物/劇物/特定毒物の区分を確定する",
            "表示・施錠保管・譲渡記録など毒劇法要件を実施する",
            "管理責任者と緊急時対応手順を整備する",
        ],
        "en": [
            "Determine poison/deleterious/specific poison category",
            "Implement labeling, locked storage, and transfer record controls",
            "Assign responsible personnel and emergency handling procedures",
        ],
    },
    "ish": {
        "ja": [
            "作業条件(取扱量・工程・曝露経路)を確定する",
            "該当区分に応じた特殊健診・作業環境測定を計画する",
            "教育、保護具、記録保存要件を実施する",
        ],
        "en": [
            "Confirm work conditions (handled amount, process, exposure route)",
            "Plan required special health checks and workplace measurements",
            "Implement training, PPE controls, and record-retention obligations",
        ],
    },
    "cwc": {
        "ja": [
            "Schedule区分(1/2/3)を確定する",
            "製造・使用・移転の届出/許可要件を確認する",
            "輸出入時は相手国・用途確認と記録保存を実施する",
        ],
        "en": [
            "Determine CWC schedule classification (1/2/3)",
            "Confirm notification/permit obligations for production, use, or transfer",
            "For export/import, verify destination/use and keep records",
        ],
    },
}


def normalize_name(value: str) -> str:
    """Normalize Japanese/English names for deterministic lookup."""
    normalized = unicodedata.normalize("NFKC", value or "")
    normalized = normalized.replace(" ", "").replace("\u3000", "")
    normalized = normalized.replace("\uFF08", "(").replace("\uFF09", ")")
    normalized = re.sub(r"[\-\u2010\u2011\u2012\u2013\u2014\u2015\u30FC\u2212\uFF0D]", "", normalized)
    normalized = re.sub(r"\([^)]*\)", "", normalized)
    return normalized.strip().lower()


def normalize_cas(value: str) -> str:
    """Normalize CAS to digits-only for partial and exact matching."""
    return re.sub(r"[^0-9]", "", value or "")


def looks_like_cas_query(value: str) -> bool:
    """Detect whether query should be treated as CAS search input."""
    raw = (value or "").strip()
    if not raw:
        return False
    if CAS_PATTERN.fullmatch(raw):
        return True
    digits = normalize_cas(raw)
    return "-" in raw and len(digits) >= 4


def _name_similarity(query_raw: str, query_normalized: str, text_raw: str, text_normalized: str) -> tuple[float, str]:
    """Return a bounded similarity score and match strategy for name search."""
    query_raw_norm = (query_raw or "").strip().casefold()
    text_raw_norm = (text_raw or "").strip().casefold()
    query_key = (query_normalized or "").strip()
    text_key = (text_normalized or "").strip()

    if query_raw_norm and query_raw_norm == text_raw_norm:
        return 1.0, "exact_raw"
    if query_key and query_key == text_key:
        return 1.0, "exact_normalized"
    if query_raw_norm and query_raw_norm in text_raw_norm:
        coverage = len(query_raw_norm) / max(len(text_raw_norm), 1)
        return max(0.8, min(0.98, 0.72 + coverage * 0.26)), "contains_raw"
    if query_key and query_key in text_key:
        coverage = len(query_key) / max(len(text_key), 1)
        return max(0.78, min(0.96, 0.7 + coverage * 0.24)), "contains_normalized"
    if text_key and text_key in query_key:
        coverage = len(text_key) / max(len(query_key), 1)
        return max(0.72, min(0.9, 0.66 + coverage * 0.2)), "contains_inverse"

    ratio_left = query_key or query_raw_norm
    ratio_right = text_key or text_raw_norm
    if not ratio_left or not ratio_right:
        return 0.0, "none"
    return SequenceMatcher(None, ratio_left, ratio_right).ratio(), "sequence_ratio"


def _cas_similarity(query_digits: str, cas_value: str) -> tuple[float, str]:
    """Return a bounded similarity score and match strategy for CAS search."""
    normalized_query = normalize_cas(query_digits)
    normalized_cas = normalize_cas(cas_value)

    if not normalized_query or not normalized_cas:
        return 0.0, "none"
    if normalized_query == normalized_cas:
        return 1.0, "cas_exact"
    if normalized_cas.startswith(normalized_query):
        coverage = len(normalized_query) / len(normalized_cas)
        return max(0.82, min(0.95, 0.78 + coverage * 0.18)), "cas_prefix"
    if normalized_query in normalized_cas:
        coverage = len(normalized_query) / len(normalized_cas)
        return max(0.7, min(0.9, 0.64 + coverage * 0.22)), "cas_contains"
    if normalized_query.endswith(normalized_cas):
        coverage = len(normalized_cas) / len(normalized_query)
        return max(0.55, min(0.8, 0.5 + coverage * 0.25)), "cas_inverse_contains"
    return SequenceMatcher(None, normalized_query, normalized_cas).ratio(), "cas_sequence_ratio"


def split_name_tokens(value: str) -> list[str]:
    """Split multi-name fields into deterministic lookup tokens."""
    raw = (value or "").replace("\r", "\n")
    return [token.strip() for token in re.split(r"[|\n]+", raw) if token.strip()]


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off", ""}:
        return False
    return default


class LawScreeningDatabase:
    """In-memory screening database loaded from ra-law-db artifacts."""

    _instance: LawScreeningDatabase | None = None

    def __init__(self, law_db_path: str | Path | None = None):
        self._resource_stack = ExitStack()
        self._instance_key = self._normalize_instance_key(law_db_path)
        self.law_db_path: Path
        self.sqlite_path: Path
        self.export_csv_path: Path
        self.snapshots_jsonl_path: Path
        self.parsed_entries_jsonl_path: Path
        self.mappings_jsonl_path: Path
        self.unresolved_jsonl_path: Path
        self.masters_dir: Path
        self._configure_paths(law_db_path)
        self.alias_master_path = self.masters_dir / ALIAS_MASTER_FILE
        self.master_coverage_path = self.masters_dir / MASTER_COVERAGE_FILE

        self._loaded = False
        self._rows_by_cas: dict[str, list[RegulatoryRow]] = {}
        self._cas_by_normalized_name: dict[str, set[str]] = {}
        self._cas_by_alias_name: dict[str, set[str]] = {}
        self._snapshot_by_law_id: dict[str, SnapshotMetadata] = {}
        self._mapping_quality: dict[tuple[str, str], MappingQuality] = {}
        self._masters: dict[str, dict[str, MasterLawRow]] = {law_code: {} for law_code in MASTER_DATASET_FILES}
        self._master_available: dict[str, bool] = dict.fromkeys(MASTER_DATASET_FILES, False)
        self._master_coverage: dict[str, dict[str, Any]] = {}
        self._alias_master_loaded = False
        self._aliases_by_cas: dict[str, set[str]] = {}
        self._resolved_entries: list[dict[str, Any]] = []
        self._unresolved_entries: list[dict[str, Any]] = []
        self._uses_sqlite_bundle = False
        self._regulatory_dataset_loaded = False

    @classmethod
    def get_instance(cls, law_db_path: str | Path | None = None) -> LawScreeningDatabase:
        """Get singleton instance bound to a law-db path or the bundled DB."""
        path_key = cls._normalize_instance_key(law_db_path)
        if cls._instance is None or cls._instance._instance_key != path_key:
            if cls._instance is not None:
                cls._instance.close()
            cls._instance = cls(law_db_path)
            cls._instance._load_data()
        return cls._instance

    @classmethod
    def reset_instance(cls) -> None:
        """Reset singleton instance (mainly for tests)."""
        if cls._instance is not None:
            cls._instance.close()
        cls._instance = None

    def reload(self) -> None:
        """Reload all data artifacts from disk."""
        self._loaded = False
        self._load_data()

    def close(self) -> None:
        """Release any packaged-resource extraction state."""
        self._resource_stack.close()

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass

    @staticmethod
    def _normalize_instance_key(law_db_path: str | Path | None) -> str:
        if law_db_path is None:
            return "__bundled__"
        text = str(law_db_path).strip()
        if not text:
            return "__bundled__"
        return str(Path(text))

    def _configure_paths(self, law_db_path: str | Path | None) -> None:
        if law_db_path is None or not str(law_db_path).strip():
            bundled_path = self._resolve_bundled_sqlite_path()
            self._configure_sqlite_only_paths(bundled_path)
            return

        candidate = Path(law_db_path)
        if candidate.suffix == ".sqlite3" or candidate.is_file():
            if not candidate.exists():
                raise FileNotFoundError(f"Law DB SQLite not found: {candidate}")
            self._configure_sqlite_only_paths(candidate)
            return

        self._configure_repo_layout_paths(candidate)

    def _resolve_bundled_sqlite_path(self) -> Path:
        resource = resources.files("ra_law_db").joinpath("data").joinpath("regulatory.sqlite3")
        return Path(self._resource_stack.enter_context(resources.as_file(resource)))

    def _configure_repo_layout_paths(self, law_db_path: Path) -> None:
        self.law_db_path = law_db_path
        self.sqlite_path = self.law_db_path / "regulatory.sqlite3"
        self.export_csv_path = self.law_db_path / "exports" / "regulatory_substances.csv"
        self.snapshots_jsonl_path = self.law_db_path / "parsed" / "source_snapshots.jsonl"
        self.parsed_entries_jsonl_path = self.law_db_path / "parsed" / "law_entries.jsonl"
        self.mappings_jsonl_path = self.law_db_path / "mappings" / "cas_mappings.jsonl"
        self.unresolved_jsonl_path = self.law_db_path / "mappings" / "unresolved_entries.jsonl"
        self.masters_dir = self.law_db_path / "masters"

    def _configure_sqlite_only_paths(self, sqlite_path: Path) -> None:
        self.law_db_path = sqlite_path
        self.sqlite_path = sqlite_path
        placeholder_root = sqlite_path.parent
        self.export_csv_path = placeholder_root / "exports" / "regulatory_substances.csv"
        self.snapshots_jsonl_path = placeholder_root / "parsed" / "source_snapshots.jsonl"
        self.parsed_entries_jsonl_path = placeholder_root / "parsed" / "law_entries.jsonl"
        self.mappings_jsonl_path = placeholder_root / "mappings" / "cas_mappings.jsonl"
        self.unresolved_jsonl_path = placeholder_root / "mappings" / "unresolved_entries.jsonl"
        self.masters_dir = placeholder_root / "masters"

    def _load_data(self) -> None:
        self._rows_by_cas.clear()
        self._cas_by_normalized_name.clear()
        self._cas_by_alias_name.clear()
        self._snapshot_by_law_id.clear()
        self._mapping_quality.clear()
        self._aliases_by_cas.clear()
        self._resolved_entries = []
        self._unresolved_entries = []
        self._uses_sqlite_bundle = False
        self._regulatory_dataset_loaded = False

        if self.sqlite_path.exists():
            self._load_sqlite_bundle()
        else:
            self._load_snapshots()
            self._load_resolved_entries()
            self._load_unresolved_entries()
            self._load_mapping_quality()
            self._load_regulatory_export()
            self._load_alias_master()
            self._load_master_datasets()
            self._load_master_coverage()

        self._loaded = True

    def _law_names_for_code(self, law_code: str) -> tuple[str, str]:
        names = LAW_STANDARD_NAMES.get(law_code, {})
        return names.get("ja", ""), names.get("en", "")

    def _regulatory_source_path(self) -> str:
        if self._uses_sqlite_bundle and self.sqlite_path.exists():
            return str(self.sqlite_path)
        return str(self.export_csv_path)

    def _law_names_for_regulation_type(self, regulation_type: str) -> tuple[str, str]:
        names = REGULATION_STANDARD_NAMES.get(regulation_type, {})
        return names.get("ja", ""), names.get("en", "")

    def _unique_matched_laws(self, rows: list[RegulatoryRow]) -> list[dict[str, str]]:
        seen: set[tuple[str, str]] = set()
        matched_laws: list[dict[str, str]] = []
        for row in rows:
            law_name_ja = row.law_name_ja
            law_name_en = row.law_name_en
            if not law_name_ja and not law_name_en:
                law_name_ja, law_name_en = self._law_names_for_regulation_type(row.regulation_type)
            key = (law_name_ja, law_name_en)
            if key in seen or key == ("", ""):
                continue
            seen.add(key)
            matched_laws.append({"law_name_ja": law_name_ja, "law_name_en": law_name_en})
        return matched_laws

    def _resolved_entry_category(self, entry: dict[str, Any], record: dict[str, Any]) -> str:
        direct = (entry.get("category") or record.get("category") or "").strip()
        if direct:
            return direct

        table_title = (entry.get("table_title") or "").strip()
        if table_title:
            return table_title

        regulation_type = (entry.get("regulation_type") or record.get("regulation_type") or "").strip()
        regulation_class = entry.get("regulation_class") or record.get("regulation_class")
        if regulation_type and regulation_class not in {None, ""}:
            return f"{regulation_type}:{regulation_class}"
        return regulation_type

    def _unresolved_entry_category(self, record: dict[str, Any]) -> str:
        direct = (record.get("category") or "").strip()
        if direct:
            return direct

        regulation_type = (record.get("regulation_type") or "").strip()
        reason = (record.get("reason") or "").strip()
        if regulation_type and reason:
            return f"{regulation_type}:{reason}"
        return regulation_type or reason

    def _load_sqlite_bundle(self) -> None:
        self._uses_sqlite_bundle = True
        connection = sqlite3.connect(self.sqlite_path)
        connection.row_factory = sqlite3.Row
        try:
            for row in connection.execute("SELECT * FROM source_snapshots"):
                law_id = (row["law_id"] or "").strip()
                if not law_id:
                    continue
                snapshot = SnapshotMetadata(
                    law_id=law_id,
                    source_url=(row["source_url"] or "").strip(),
                    fetched_at=(row["fetched_at"] or "").strip(),
                    content_hash=(row["content_hash"] or "").strip(),
                )
                existing = self._snapshot_by_law_id.get(law_id)
                if existing is None or snapshot.fetched_at >= existing.fetched_at:
                    self._snapshot_by_law_id[law_id] = snapshot

            entries_by_id: dict[str, dict[str, Any]] = {}
            for row in connection.execute("SELECT * FROM law_entries"):
                record = {key: row[key] for key in row.keys()}
                entry_id = (record.get("entry_id") or "").strip()
                if entry_id:
                    entries_by_id[entry_id] = record

            for row in connection.execute("SELECT * FROM cas_mappings"):
                record = {key: row[key] for key in row.keys()}
                cas_number = (record.get("cas_number") or "").strip()
                entry_id = (record.get("entry_id") or "").strip()
                regulation_type = (record.get("regulation_type") or "").strip()
                if not cas_number or not entry_id:
                    continue

                quality = MappingQuality(
                    cas_number=cas_number,
                    regulation_type=regulation_type,
                    match_method=record.get("match_method", ""),
                    confidence=_safe_float(record.get("confidence"), 0.0),
                )
                key = (cas_number, regulation_type)
                existing = self._mapping_quality.get(key)
                if existing is None or quality.confidence >= existing.confidence:
                    self._mapping_quality[key] = quality

                entry = entries_by_id.get(entry_id)
                if not entry:
                    continue
                raw_name = (entry.get("raw_name") or "").strip()
                normalized_name = normalize_name((entry.get("normalized_name") or raw_name).strip())
                self._resolved_entries.append(
                    {
                        "law_id": (entry.get("law_id") or record.get("law_id") or "").strip(),
                        "category": self._resolved_entry_category(entry, record),
                        "raw_name": raw_name,
                        "normalized_name": normalized_name,
                        "cas_number": cas_number,
                        "match_method": (record.get("match_method") or "").strip(),
                        "confidence": _safe_float(record.get("confidence"), 0.0),
                        "regulation_type": regulation_type,
                    }
                )

            for row in connection.execute("SELECT * FROM unresolved_entries"):
                record = {key: row[key] for key in row.keys()}
                raw_name = (record.get("raw_name") or "").strip()
                if not raw_name:
                    continue
                self._unresolved_entries.append(
                    {
                        "law_id": (record.get("law_id") or "").strip(),
                        "category": self._unresolved_entry_category(record),
                        "raw_name": raw_name,
                        "normalized_name": normalize_name((record.get("normalized_name") or raw_name).strip()),
                    }
                )

            for row in connection.execute("SELECT * FROM regulatory_substances"):
                record_row = {key: row[key] for key in row.keys()}
                cas_number = (record_row.get("cas_number") or "").strip()
                if not cas_number:
                    continue
                self._regulatory_dataset_loaded = True
                record = RegulatoryRow(
                    cas_number=cas_number,
                    name_ja=(record_row.get("name_ja") or "").strip(),
                    name_en=(record_row.get("name_en") or "").strip(),
                    regulation_type=(record_row.get("regulation_type") or "").strip(),
                    regulation_class=_safe_int(record_row.get("regulation_class")),
                    regulation_label=(record_row.get("regulation_label") or "").strip(),
                    law_name_ja=((record_row.get("law_name_ja") or "").strip() or self._law_names_for_regulation_type((record_row.get("regulation_type") or "").strip())[0]),
                    law_name_en=((record_row.get("law_name_en") or "").strip() or self._law_names_for_regulation_type((record_row.get("regulation_type") or "").strip())[1]),
                    health_check_required=_safe_bool(record_row.get("health_check_required")),
                    health_check_type=(record_row.get("health_check_type") or "").strip(),
                    health_check_interval=(record_row.get("health_check_interval") or "").strip(),
                    health_check_ref=(record_row.get("health_check_ref") or "").strip(),
                    record_retention_years=_safe_int(record_row.get("record_retention_years")),
                )
                self._rows_by_cas.setdefault(cas_number, []).append(record)
                for candidate in (record.name_ja, record.name_en):
                    for token in split_name_tokens(candidate):
                        key = normalize_name(token)
                        if key:
                            self._cas_by_normalized_name.setdefault(key, set()).add(cas_number)

            try:
                alias_rows = connection.execute("SELECT * FROM substance_aliases")
            except sqlite3.OperationalError:
                alias_rows = []
            else:
                self._alias_master_loaded = True
            for row in alias_rows:
                cas_number = (row["cas_number"] or "").strip()
                if not cas_number:
                    continue
                normalized_alias = (row["normalized_alias"] or "").strip()
                if not normalized_alias:
                    normalized_alias = normalize_name((row["alias_name"] or "").strip())
                else:
                    normalized_alias = normalize_name(normalized_alias)
                if not normalized_alias:
                    continue
                self._cas_by_alias_name.setdefault(normalized_alias, set()).add(cas_number)
                alias_name = (row["alias_name"] or "").strip()
                if alias_name:
                    self._aliases_by_cas.setdefault(cas_number, set()).add(alias_name)

            for law_code in MASTER_DATASET_FILES:
                self._masters[law_code] = {}
                table_name = f"{law_code}_substances"
                try:
                    rows = connection.execute(f"SELECT * FROM {table_name}")
                except sqlite3.OperationalError:
                    self._master_available[law_code] = False
                    continue
                self._master_available[law_code] = True
                for row in rows:
                    record_row = {key: row[key] for key in row.keys()}
                    cas_number = (record_row.get("cas_number") or "").strip()
                    if not cas_number:
                        continue
                    self._masters[law_code][cas_number] = MasterLawRow(
                        cas_number=cas_number,
                        category=(record_row.get("category") or "").strip(),
                        label_ja=(record_row.get("label_ja") or "").strip(),
                        label_en=(record_row.get("label_en") or "").strip(),
                        law_name_ja=((record_row.get("law_name_ja") or "").strip() or self._law_names_for_code(law_code)[0]),
                        law_name_en=((record_row.get("law_name_en") or "").strip() or self._law_names_for_code(law_code)[1]),
                        law_reference=(record_row.get("law_reference") or "").strip(),
                        law_id=(record_row.get("law_id") or "").strip(),
                    )

            try:
                coverage_rows = connection.execute("SELECT * FROM master_coverage")
            except sqlite3.OperationalError:
                coverage_rows = []
            for row in coverage_rows:
                law_code = (row["law_code"] or "").strip()
                if not law_code:
                    continue
                self._master_coverage[law_code] = {
                    "entries": _safe_int(row["entries"], 0),
                    "mapped_entries": _safe_int(row["mapped_entries"], 0),
                    "master_rows": _safe_int(row["master_rows"], 0),
                    "unresolved_entries": _safe_int(row["unresolved_entries"], 0),
                    "is_complete": _safe_bool(row["is_complete"], False),
                }
        finally:
            connection.close()

    def _load_snapshots(self) -> None:
        if not self.snapshots_jsonl_path.exists():
            return

        with open(self.snapshots_jsonl_path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                record = json.loads(line)
                law_id = (record.get("law_id") or "").strip()
                if not law_id:
                    continue
                snapshot = SnapshotMetadata(
                    law_id=law_id,
                    source_url=record.get("source_url", ""),
                    fetched_at=record.get("fetched_at", ""),
                    content_hash=record.get("content_hash", ""),
                )
                existing = self._snapshot_by_law_id.get(law_id)
                if existing is None or snapshot.fetched_at >= existing.fetched_at:
                    self._snapshot_by_law_id[law_id] = snapshot

    def _load_resolved_entries(self) -> None:
        if not self.mappings_jsonl_path.exists() or not self.parsed_entries_jsonl_path.exists():
            return

        entries_by_id: dict[str, dict[str, Any]] = {}
        with open(self.parsed_entries_jsonl_path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                record = json.loads(line)
                entry_id = (record.get("entry_id") or "").strip()
                if entry_id:
                    entries_by_id[entry_id] = record

        with open(self.mappings_jsonl_path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                record = json.loads(line)
                cas_number = (record.get("cas_number") or "").strip()
                entry_id = (record.get("entry_id") or "").strip()
                if not cas_number or not entry_id:
                    continue

                entry = entries_by_id.get(entry_id)
                if not entry:
                    continue

                raw_name = (entry.get("raw_name") or "").strip()
                normalized_name = normalize_name((entry.get("normalized_name") or raw_name).strip())
                self._resolved_entries.append(
                    {
                        "law_id": (entry.get("law_id") or record.get("law_id") or "").strip(),
                        "category": self._resolved_entry_category(entry, record),
                        "raw_name": raw_name,
                        "normalized_name": normalized_name,
                        "cas_number": cas_number,
                        "match_method": (record.get("match_method") or "").strip(),
                        "confidence": _safe_float(record.get("confidence"), 0.0),
                        "regulation_type": (record.get("regulation_type") or "").strip(),
                    }
                )

    def _load_unresolved_entries(self) -> None:
        if not self.unresolved_jsonl_path.exists():
            return

        with open(self.unresolved_jsonl_path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                record = json.loads(line)
                raw_name = (record.get("raw_name") or "").strip()
                if not raw_name:
                    continue
                self._unresolved_entries.append(
                    {
                        "law_id": (record.get("law_id") or "").strip(),
                        "category": self._unresolved_entry_category(record),
                        "raw_name": raw_name,
                        "normalized_name": normalize_name((record.get("normalized_name") or raw_name).strip()),
                    }
                )

    def _load_mapping_quality(self) -> None:
        if not self.mappings_jsonl_path.exists():
            return

        with open(self.mappings_jsonl_path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                record = json.loads(line)
                cas_number = (record.get("cas_number") or "").strip()
                regulation_type = (record.get("regulation_type") or "").strip()
                if not cas_number or not regulation_type:
                    continue

                quality = MappingQuality(
                    cas_number=cas_number,
                    regulation_type=regulation_type,
                    match_method=record.get("match_method", ""),
                    confidence=_safe_float(record.get("confidence"), 0.0),
                )
                key = (cas_number, regulation_type)
                existing = self._mapping_quality.get(key)
                if existing is None or quality.confidence >= existing.confidence:
                    self._mapping_quality[key] = quality

    def _load_regulatory_export(self) -> None:
        if not self.export_csv_path.exists():
            return

        with open(self.export_csv_path, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                cas_number = (row.get("cas_number") or "").strip()
                if not cas_number:
                    continue

                record = RegulatoryRow(
                    cas_number=cas_number,
                    name_ja=(row.get("name_ja") or "").strip(),
                    name_en=(row.get("name_en") or "").strip(),
                    regulation_type=(row.get("regulation_type") or "").strip(),
                    regulation_class=_safe_int(row.get("regulation_class")),
                    regulation_label=(row.get("regulation_label") or "").strip(),
                    law_name_ja=((row.get("law_name_ja") or "").strip() or self._law_names_for_regulation_type((row.get("regulation_type") or "").strip())[0]),
                    law_name_en=((row.get("law_name_en") or "").strip() or self._law_names_for_regulation_type((row.get("regulation_type") or "").strip())[1]),
                    health_check_required=_safe_bool(row.get("health_check_required")),
                    health_check_type=(row.get("health_check_type") or "").strip(),
                    health_check_interval=(row.get("health_check_interval") or "").strip(),
                    health_check_ref=(row.get("health_check_ref") or "").strip(),
                    record_retention_years=_safe_int(row.get("record_retention_years")),
                )
                self._regulatory_dataset_loaded = True
                self._rows_by_cas.setdefault(cas_number, []).append(record)

                for candidate in (record.name_ja, record.name_en):
                    for token in split_name_tokens(candidate):
                        key = normalize_name(token)
                        if key:
                            self._cas_by_normalized_name.setdefault(key, set()).add(cas_number)

    def _load_alias_master(self) -> None:
        self._alias_master_loaded = self.alias_master_path.exists()
        if not self._alias_master_loaded:
            return

        with open(self.alias_master_path, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                cas_number = (row.get("cas_number") or "").strip()
                if not cas_number:
                    continue

                normalized_alias = (row.get("normalized_alias") or "").strip()
                if not normalized_alias:
                    normalized_alias = normalize_name((row.get("alias_name") or "").strip())
                else:
                    normalized_alias = normalize_name(normalized_alias)
                if not normalized_alias:
                    continue
                self._cas_by_alias_name.setdefault(normalized_alias, set()).add(cas_number)

                alias_name = (row.get("alias_name") or "").strip()
                if alias_name:
                    self._aliases_by_cas.setdefault(cas_number, set()).add(alias_name)

    def _load_master_datasets(self) -> None:
        for law_code, file_name in MASTER_DATASET_FILES.items():
            dataset_path = self.masters_dir / file_name
            self._masters[law_code] = {}
            self._master_available[law_code] = dataset_path.exists()
            if not dataset_path.exists():
                continue

            with open(dataset_path, encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    cas_number = (row.get("cas_number") or "").strip()
                    if not cas_number:
                        continue
                    self._masters[law_code][cas_number] = MasterLawRow(
                        cas_number=cas_number,
                        category=(row.get("category") or "").strip(),
                        label_ja=(row.get("label_ja") or "").strip(),
                        label_en=(row.get("label_en") or "").strip(),
                        law_name_ja=((row.get("law_name_ja") or "").strip() or self._law_names_for_code(law_code)[0]),
                        law_name_en=((row.get("law_name_en") or "").strip() or self._law_names_for_code(law_code)[1]),
                        law_reference=(row.get("law_reference") or "").strip(),
                        law_id=(row.get("law_id") or "").strip(),
                    )

    def _load_master_coverage(self) -> None:
        self._master_coverage = {}
        if not self.master_coverage_path.exists():
            return
        with open(self.master_coverage_path, encoding="utf-8") as f:
            loaded = json.load(f)
        if isinstance(loaded, dict):
            for law_code, payload in loaded.items():
                if isinstance(payload, dict):
                    self._master_coverage[law_code] = payload

    def _is_master_dataset_complete(self, law_code: str) -> bool:
        payload = self._master_coverage.get(law_code, {})
        unresolved = _safe_int(payload.get("unresolved_entries"), 0)
        return unresolved == 0

    def _extract_law_id(self, reference: str) -> str | None:
        match = re.search(r"([0-9]{3}[A-Z]{2}[0-9]{10})", reference)
        return match.group(1) if match else None

    def _snapshot_payload(self, law_id: str | None) -> dict[str, str] | None:
        if not law_id:
            return None
        snapshot = self._snapshot_by_law_id.get(law_id)
        if snapshot is None:
            return None
        return {
            "fetched_at": snapshot.fetched_at,
            "content_hash": snapshot.content_hash,
            "source_url": snapshot.source_url,
        }

    def _resolve_candidates(
        self,
        cas_number: str | None,
        substance_name: str | None,
    ) -> tuple[set[str], list[dict[str, Any]]]:
        cas_candidates: set[str] = set()
        matched_substances: list[dict[str, Any]] = []

        cas_query = (cas_number or "").strip()
        if cas_query:
            cas_candidates.add(cas_query)

        name_query = normalize_name(substance_name or "")
        name_matches = set(self._cas_by_normalized_name.get(name_query, set())) if name_query else set()
        alias_matches = set(self._cas_by_alias_name.get(name_query, set())) if name_query else set()
        cas_candidates.update(name_matches)
        cas_candidates.update(alias_matches)

        for cas in sorted(cas_candidates):
            rows = self._rows_by_cas.get(cas, [])
            name_ja = rows[0].name_ja if rows else ""
            name_en = rows[0].name_en if rows else ""

            methods: list[str] = []
            if cas_query and cas_query == cas:
                methods.append("cas_exact")
            if name_query and cas in name_matches:
                methods.append("name_normalized")
            if name_query and cas in alias_matches:
                methods.append("alias_master")
            if not methods:
                methods.append("derived")

            matched_substances.append(
                {
                    "cas_number": cas,
                    "name_ja": name_ja,
                    "name_en": name_en,
                    "match_sources": methods,
                }
            )

        return cas_candidates, matched_substances

    def _localize(self, language: str, ja_text: str, en_text: str) -> str:
        return ja_text if language == "ja" else en_text

    def _localized_actions(self, language: str, ja_actions: list[str], en_actions: list[str]) -> list[str]:
        return ja_actions if language == "ja" else en_actions

    def _build_action_items(
        self,
        language: str,
        law_code: str,
        action_group: str,
        ja_actions: list[str],
        en_actions: list[str],
    ) -> list[dict[str, Any]]:
        labels = self._localized_actions(language, ja_actions, en_actions)
        return [
            {
                "action_code": f"{law_code}_{action_group}_{index + 1}",
                "label": label,
                "required": True,
            }
            for index, label in enumerate(labels)
        ]

    def _default_actions_for_status(self, law_code: str, status: str, language: str) -> list[dict[str, Any]]:
        if status == "applies":
            actions = LAW_APPLIES_ACTIONS.get(law_code, {})
            return self._build_action_items(
                language,
                law_code,
                "applies",
                actions.get("ja", []),
                actions.get("en", []),
            )
        if status == "requires_context":
            actions = LAW_APPLIES_ACTIONS.get(law_code, {})
            return self._build_action_items(
                language,
                law_code,
                "requires_context",
                actions.get("ja", []),
                actions.get("en", []),
            )
        if status == "not_applies":
            return self._build_action_items(
                language,
                law_code,
                "not_applies",
                NOT_APPLIES_ACTIONS["ja"],
                NOT_APPLIES_ACTIONS["en"],
            )
        return self._build_action_items(
            language,
            law_code,
            "unknown",
            NO_CANDIDATE_ACTIONS["ja"],
            NO_CANDIDATE_ACTIONS["en"],
        )

    def _missing_master_actions(
        self,
        law_code: str,
        language: str,
        dataset_name: str,
    ) -> list[dict[str, Any]]:
        law_actions = MISSING_MASTER_LAW_ACTIONS.get(law_code)
        if law_actions:
            ja_actions = [f"{dataset_name} を追加する", *law_actions["ja"]]
            en_actions = [f"Add {dataset_name}", *law_actions["en"]]
            items = self._build_action_items(language, law_code, "missing_master", ja_actions, en_actions)
            items[0]["metadata"] = {"dataset": dataset_name}
            return items

        ja_actions = [f"{dataset_name} を追加する", *MISSING_MASTER_ACTIONS["ja"][1:]]
        en_actions = [f"Add {dataset_name}", *MISSING_MASTER_ACTIONS["en"][1:]]
        return self._build_action_items(language, law_code, "missing_master", ja_actions, en_actions)

    def _missing_master_targets(self, law_code: str, language: str) -> list[dict[str, str]]:
        labels = MISSING_MASTER_TARGETS.get(law_code, [])
        if not labels:
            return []

        localized_key = "ja" if language == "ja" else "en"
        return [{"code": row["code"], "label": row[localized_key]} for row in labels]

    def _missing_master_source_hints(self, law_code: str, language: str) -> list[str]:
        source_hints = MISSING_MASTER_SOURCE_HINTS.get(law_code, {})
        if not source_hints:
            return []
        return source_hints["ja"] if language == "ja" else source_hints["en"]

    def _base_result(
        self,
        law_code: str,
        language: str,
        status: str,
        reason_code: str,
        ja_notes: str,
        en_notes: str,
    ) -> dict[str, Any]:
        law_name_ja, law_name_en = self._law_names_for_code(law_code)
        return {
            "law_code": law_code,
            "law_label": LAW_LABELS.get(language, LAW_LABELS["en"]).get(law_code, law_code),
            "law_name_ja": law_name_ja,
            "law_name_en": law_name_en,
            "status": status,
            "status_reason_code": reason_code,
            "categories": [],
            "flags": {},
            "notes": self._localize(language, ja_notes, en_notes),
            "required_actions": self._default_actions_for_status(law_code, status, language),
            "evidence": {
                "source": self._regulatory_source_path(),
            },
        }

    def _result_from_master(self, law_code: str, cas_candidates: set[str], language: str) -> dict[str, Any]:
        if not cas_candidates:
            return self._base_result(
                law_code,
                language,
                "unknown",
                STATUS_REASON_CODES["no_cas_candidate"],
                "照合対象のCASを特定できませんでした。",
                "No CAS candidate was resolved from the query.",
            )

        if not self._master_available[law_code]:
            result = self._base_result(
                law_code,
                language,
                "unknown",
                STATUS_REASON_CODES["missing_master_dataset"],
                f"{MASTER_DATASET_FILES[law_code]} が未配置です。",
                f"Dataset {MASTER_DATASET_FILES[law_code]} is not available.",
            )
            result["categories"] = self._missing_master_targets(law_code, language)
            result["required_actions"] = self._missing_master_actions(
                law_code,
                language,
                MASTER_DATASET_FILES[law_code],
            )
            result["evidence"]["dataset"] = MASTER_DATASET_FILES[law_code]
            result["evidence"]["dataset_loaded"] = False
            result["evidence"]["manual_sources"] = self._missing_master_source_hints(law_code, language)
            return result

        hits = [self._masters[law_code][cas] for cas in sorted(cas_candidates) if cas in self._masters[law_code]]
        if not hits:
            is_complete = self._is_master_dataset_complete(law_code)
            if is_complete:
                result = self._base_result(
                    law_code,
                    language,
                    "not_applies",
                    STATUS_REASON_CODES["no_dataset_hit"],
                    "読み込み済みデータセット上で該当なしです。",
                    "No hit in loaded law dataset.",
                )
            else:
                result = self._base_result(
                    law_code,
                    language,
                    "unknown",
                    STATUS_REASON_CODES["incomplete_master_dataset"],
                    "マスタのCAS対応が未完了のため、非該当を確定できません。",
                    "Master CAS mapping is incomplete; non-applicability cannot be confirmed.",
                )
                result["required_actions"] = self._missing_master_actions(
                    law_code,
                    language,
                    MASTER_DATASET_FILES[law_code],
                )
            result["evidence"]["dataset"] = MASTER_DATASET_FILES[law_code]
            result["evidence"]["dataset_loaded"] = True
            coverage = self._master_coverage.get(law_code)
            if coverage:
                result["evidence"]["dataset_coverage"] = coverage
            return result

        categories = []
        for hit in hits:
            categories.append(
                {
                    "code": hit.category,
                    "label": hit.label_ja if language == "ja" else hit.label_en,
                    "cas_number": hit.cas_number,
                    "law_name_ja": hit.law_name_ja,
                    "law_name_en": hit.law_name_en,
                }
            )

        primary = hits[0]
        result = self._base_result(
            law_code,
            language,
            "applies",
            STATUS_REASON_CODES["matched_master"],
            "法令マスタに一致しました。",
            "Matched in law master dataset.",
        )
        result["categories"] = categories
        result["evidence"].update(
            {
                "dataset": MASTER_DATASET_FILES[law_code],
                "dataset_loaded": True,
                "law_id": primary.law_id or None,
                "law_name_ja": primary.law_name_ja or result["law_name_ja"],
                "law_name_en": primary.law_name_en or result["law_name_en"],
                "reference": primary.law_reference or None,
            }
        )
        coverage = self._master_coverage.get(law_code)
        if coverage:
            result["evidence"]["dataset_coverage"] = coverage
        snapshot = self._snapshot_payload(primary.law_id)
        if snapshot:
            result["evidence"]["snapshot"] = snapshot
        return result

    def _result_prtr(self, cas_candidates: set[str], language: str) -> dict[str, Any]:
        if not cas_candidates:
            return self._base_result(
                "prtr",
                language,
                "unknown",
                STATUS_REASON_CODES["no_cas_candidate"],
                "照合対象のCASを特定できませんでした。",
                "No CAS candidate was resolved from the query.",
            )

        hits: list[tuple[str, RegulatoryRow]] = []
        for cas in sorted(cas_candidates):
            for row in self._rows_by_cas.get(cas, []):
                if row.regulation_type in PRTR_CATEGORY_MAP:
                    hits.append((cas, row))

        if not hits:
            return self._base_result(
                "prtr",
                language,
                "not_applies",
                STATUS_REASON_CODES["no_dataset_hit"],
                "現在のデータセットでPRTR該当はありません。",
                "No PRTR category hit in current dataset.",
            )

        categories = []
        for cas, row in hits:
            category = PRTR_CATEGORY_MAP[row.regulation_type]
            quality = self._mapping_quality.get((cas, row.regulation_type))
            categories.append(
                {
                    "code": category["code"],
                    "label": category["ja"] if language == "ja" else category["en"],
                    "cas_number": cas,
                    "law_name_ja": row.law_name_ja,
                    "law_name_en": row.law_name_en,
                    "match_method": quality.match_method if quality else None,
                    "confidence": quality.confidence if quality else None,
                }
            )

        first_cas, first_row = hits[0]
        law_id = self._extract_law_id(first_row.health_check_ref)
        result = self._base_result(
            "prtr",
            language,
            "applies",
            STATUS_REASON_CODES["matched_context_required"],
            "PRTR届出要否の確定には年間取扱量など追加情報が必要です。",
            "PRTR reporting obligations may require annual handling data.",
        )
        result["categories"] = categories
        result["required_context"] = ["annual_handling_tons", "business_type"]
        result["evidence"].update(
            {
                "law_id": law_id,
                "law_name_ja": first_row.law_name_ja or result["law_name_ja"],
                "law_name_en": first_row.law_name_en or result["law_name_en"],
                "reference": first_row.health_check_ref,
                "matched_laws": self._unique_matched_laws([row for _, row in hits]),
                "match_quality": self._mapping_quality.get((first_cas, first_row.regulation_type)).__dict__
                if self._mapping_quality.get((first_cas, first_row.regulation_type))
                else None,
            }
        )
        snapshot = self._snapshot_payload(law_id)
        if snapshot:
            result["evidence"]["snapshot"] = snapshot
        return result

    def _ish_category_code(self, row: RegulatoryRow) -> str:
        if row.regulation_type == "tokka":
            return f"tokka_class_{row.regulation_class}"
        if row.regulation_type == "organic":
            return f"organic_type_{row.regulation_class}"
        return row.regulation_type

    def _build_ish_categories_and_references(
        self,
        hits: list[tuple[str, RegulatoryRow]],
    ) -> tuple[list[dict[str, str]], list[str]]:
        categories: list[dict[str, str]] = []
        references: list[str] = []
        seen_codes: set[tuple[str, int]] = set()

        for cas, row in hits:
            code_key = (row.regulation_type, row.regulation_class)
            if code_key in seen_codes:
                continue
            seen_codes.add(code_key)

            categories.append(
                {
                    "code": self._ish_category_code(row),
                    "label": row.regulation_label,
                    "cas_number": cas,
                    "law_name_ja": row.law_name_ja,
                    "law_name_en": row.law_name_en,
                }
            )
            if row.health_check_ref:
                references.append(row.health_check_ref)

        return categories, references

    def _build_ish_health_checks(self, hits: list[tuple[str, RegulatoryRow]]) -> list[dict[str, Any]]:
        health_checks: list[dict[str, Any]] = []
        seen: set[tuple[str, str, int]] = set()

        for _, row in hits:
            if not row.health_check_required and not row.health_check_type:
                continue

            check_type = row.health_check_type or "健康診断"
            interval = row.health_check_interval or ""
            retention = row.record_retention_years
            key = (check_type, interval, retention)
            if key in seen:
                continue
            seen.add(key)

            health_checks.append(
                {
                    "type": check_type,
                    "interval": interval,
                    "record_retention_years": retention,
                }
            )

        return health_checks

    def _build_ish_flags(self, hits: list[tuple[str, RegulatoryRow]]) -> dict[str, bool]:
        tokka_classes = {
            row.regulation_class
            for _, row in hits
            if row.regulation_type == "tokka" and row.regulation_class in {1, 2, 3}
        }
        return {
            "tokka_applicable": bool(tokka_classes),
            "tokka_class_1": 1 in tokka_classes,
            "tokka_class_2": 2 in tokka_classes,
            "tokka_class_3": 3 in tokka_classes,
            "organic_applicable": any(row.regulation_type == "organic" for _, row in hits),
            "lead_applicable": any(row.regulation_type == "lead" for _, row in hits),
            "prohibited_substance": any(row.regulation_type == "prohibited" for _, row in hits),
        }

    def _append_required_actions(self, result: dict[str, Any], actions: list[dict[str, Any]]) -> None:
        existing = list(result.get("required_actions", []))
        seen = {item.get("action_code") for item in existing}
        for action in actions:
            code = action.get("action_code")
            if code and code not in seen:
                existing.append(action)
                seen.add(code)
        result["required_actions"] = existing

    def _build_health_check_actions(self, health_checks: list[dict[str, Any]], language: str) -> list[dict[str, Any]]:
        extra_actions: list[dict[str, Any]] = []
        for index, check in enumerate(health_checks, start=1):
            check_type = check["type"]
            interval = check["interval"]
            retention = check["record_retention_years"]
            if language == "ja":
                if interval:
                    extra_actions.append(
                        {
                            "action_code": f"ish_health_check_{index}",
                            "label": f"{check_type}を{interval}で実施する",
                            "required": True,
                            "metadata": {"type": check_type, "interval": interval},
                        }
                    )
                else:
                    extra_actions.append(
                        {
                            "action_code": f"ish_health_check_{index}",
                            "label": f"{check_type}を実施する",
                            "required": True,
                            "metadata": {"type": check_type},
                        }
                    )
                if retention > 0:
                    extra_actions.append(
                        {
                            "action_code": f"ish_health_record_retention_{index}",
                            "label": f"健診記録を{retention}年保存する",
                            "required": True,
                            "metadata": {"record_retention_years": retention},
                        }
                    )
            else:
                if interval:
                    extra_actions.append(
                        {
                            "action_code": f"ish_health_check_{index}",
                            "label": f"Conduct {check_type} every {interval}",
                            "required": True,
                            "metadata": {"type": check_type, "interval": interval},
                        }
                    )
                else:
                    extra_actions.append(
                        {
                            "action_code": f"ish_health_check_{index}",
                            "label": f"Conduct {check_type}",
                            "required": True,
                            "metadata": {"type": check_type},
                        }
                    )
                if retention > 0:
                    extra_actions.append(
                        {
                            "action_code": f"ish_health_record_retention_{index}",
                            "label": f"Retain health-check records for {retention} years",
                            "required": True,
                            "metadata": {"record_retention_years": retention},
                        }
                    )
        return extra_actions

    def _apply_ish_obligations(
        self,
        result: dict[str, Any],
        hits: list[tuple[str, RegulatoryRow]],
        language: str,
    ) -> None:
        ish_flags = self._build_ish_flags(hits)
        result["flags"] = ish_flags

        if ish_flags["tokka_applicable"]:
            if language == "ja":
                self._append_required_actions(
                    result,
                    [
                        {
                            "action_code": "ish_tokka_obligations",
                            "label": "特化則区分に応じた追加義務(作業主任者、測定、掲示、記録)を確認する",
                            "required": True,
                        }
                    ],
                )
            else:
                self._append_required_actions(
                    result,
                    [
                        {
                            "action_code": "ish_tokka_obligations",
                            "label": "Confirm Tokka-specific obligations (supervisor assignment, measurement, notices, records)",
                            "required": True,
                        }
                    ],
                )

        health_checks = self._build_ish_health_checks(hits)
        if not health_checks:
            return

        result["evidence"]["health_checks"] = health_checks
        self._append_required_actions(result, self._build_health_check_actions(health_checks, language))

    def _result_ish(self, cas_candidates: set[str], language: str) -> dict[str, Any]:
        if not cas_candidates:
            return self._base_result(
                "ish",
                language,
                "unknown",
                STATUS_REASON_CODES["no_cas_candidate"],
                "照合対象のCASを特定できませんでした。",
                "No CAS candidate was resolved from the query.",
            )

        hits = [
            (cas, row)
            for cas in sorted(cas_candidates)
            for row in self._rows_by_cas.get(cas, [])
            if row.regulation_type in ISH_TYPES
        ]

        if not hits:
            return self._base_result(
                "ish",
                language,
                "not_applies",
                STATUS_REASON_CODES["no_dataset_hit"],
                "現在のデータセットで安衛法系の該当はありません。",
                "No ISH category hit in current dataset.",
            )

        categories, references = self._build_ish_categories_and_references(hits)

        result = self._base_result(
            "ish",
            language,
            "requires_context",
            STATUS_REASON_CODES["matched_context_required"],
            "物質は法令リストに該当します。具体義務の判定には作業条件の追加情報が必要です。",
            "Substance is listed. Specific duties depend on work conditions and handling context.",
        )
        result["categories"] = categories
        result["required_context"] = ISH_CONTEXT_FIELDS
        result["evidence"].update(
            {
                "source_file": self._regulatory_source_path(),
                "references": sorted(set(references))[:10],
                "matched_laws": self._unique_matched_laws([row for _, row in hits]),
            }
        )
        self._apply_ish_obligations(result, hits, language)
        return result

    def _display_names(self, cas_number: str) -> tuple[str, str]:
        rows = self._rows_by_cas.get(cas_number, [])
        if rows:
            return rows[0].name_ja, rows[0].name_en
        return "", ""

    def _add_substance_hit(
        self,
        hits_by_cas: dict[str, dict[str, Any]],
        *,
        cas_number: str,
        score: float,
        match_type: str,
        matched_value: str,
        source: str,
        max_terms: int = 5,
    ) -> None:
        if score <= 0:
            return

        name_ja, name_en = self._display_names(cas_number)
        hit = hits_by_cas.setdefault(
            cas_number,
            {
                "cas_number": cas_number,
                "name_ja": name_ja,
                "name_en": name_en,
                "score": 0.0,
                "match_type": "none",
                "matched_value": "",
                "match_sources": set(),
                "matched_terms": [],
            },
        )

        if score > hit["score"]:
            hit["score"] = score
            hit["match_type"] = match_type
            hit["matched_value"] = matched_value

        hit["match_sources"].add(source)
        existing_terms = {(item["value"], item["source"]) for item in hit["matched_terms"]}
        term_key = (matched_value, source)
        if term_key not in existing_terms:
            hit["matched_terms"].append(
                {
                    "value": matched_value,
                    "source": source,
                    "score": round(score, 4),
                    "match_type": match_type,
                }
            )
            hit["matched_terms"].sort(key=lambda item: item["score"], reverse=True)
            if len(hit["matched_terms"]) > max_terms:
                hit["matched_terms"] = hit["matched_terms"][:max_terms]

    def _search_substance_hits(
        self,
        query: str,
        mode: str,
        *,
        limit: int,
        min_score: float,
    ) -> list[dict[str, Any]]:
        hits_by_cas: dict[str, dict[str, Any]] = {}
        if mode == "cas":
            self._collect_substance_hits_by_cas(query, min_score=min_score, hits_by_cas=hits_by_cas)
        else:
            self._collect_substance_hits_by_name(query, min_score=min_score, hits_by_cas=hits_by_cas)

        return self._rank_substance_hits(hits_by_cas, limit)

    def _collect_substance_hits_by_cas(
        self,
        query: str,
        *,
        min_score: float,
        hits_by_cas: dict[str, dict[str, Any]],
    ) -> None:
        known_cas = set(self._rows_by_cas.keys())
        for master in self._masters.values():
            known_cas.update(master.keys())

        for cas_number in sorted(known_cas):
            score, match_type = _cas_similarity(query, cas_number)
            if score >= min_score:
                self._add_substance_hit(
                    hits_by_cas,
                    cas_number=cas_number,
                    score=score,
                    match_type=match_type,
                    matched_value=cas_number,
                    source="cas_number",
                )

    def _collect_substance_hits_by_name(
        self,
        query: str,
        *,
        min_score: float,
        hits_by_cas: dict[str, dict[str, Any]],
    ) -> None:
        query_normalized = normalize_name(query)

        for cas_number, rows in self._rows_by_cas.items():
            seen_tokens: set[tuple[str, str]] = set()
            for row in rows:
                self._collect_row_token_hits(
                    cas_number,
                    split_name_tokens(row.name_ja),
                    token_source="name_ja",
                    query=query,
                    query_normalized=query_normalized,
                    min_score=min_score,
                    hits_by_cas=hits_by_cas,
                    seen_tokens=seen_tokens,
                )
                self._collect_row_token_hits(
                    cas_number,
                    split_name_tokens(row.name_en),
                    token_source="name_en",
                    query=query,
                    query_normalized=query_normalized,
                    min_score=min_score,
                    hits_by_cas=hits_by_cas,
                    seen_tokens=seen_tokens,
                )

        for cas_number, aliases in self._aliases_by_cas.items():
            for alias_name in aliases:
                score, match_type = _name_similarity(query, query_normalized, alias_name, normalize_name(alias_name))
                if score >= min_score:
                    self._add_substance_hit(
                        hits_by_cas,
                        cas_number=cas_number,
                        score=score,
                        match_type=match_type,
                        matched_value=alias_name,
                        source="alias_master",
                    )

    def _collect_row_token_hits(
        self,
        cas_number: str,
        tokens: list[str],
        *,
        token_source: str,
        query: str,
        query_normalized: str,
        min_score: float,
        hits_by_cas: dict[str, dict[str, Any]],
        seen_tokens: set[tuple[str, str]],
    ) -> None:
        for token in tokens:
            token_key = (token, token_source)
            if token_key in seen_tokens:
                continue
            seen_tokens.add(token_key)
            score, match_type = _name_similarity(query, query_normalized, token, normalize_name(token))
            if score >= min_score:
                self._add_substance_hit(
                    hits_by_cas,
                    cas_number=cas_number,
                    score=score,
                    match_type=match_type,
                    matched_value=token,
                    source=token_source,
                )

    def _rank_substance_hits(
        self,
        hits_by_cas: dict[str, dict[str, Any]],
        limit: int,
    ) -> list[dict[str, Any]]:
        ranked = sorted(hits_by_cas.values(), key=lambda item: (-item["score"], item["cas_number"]))

        payload: list[dict[str, Any]] = []
        for item in ranked[:limit]:
            payload.append(
                {
                    "cas_number": item["cas_number"],
                    "name_ja": item["name_ja"],
                    "name_en": item["name_en"],
                    "score": round(item["score"], 4),
                    "match_type": item["match_type"],
                    "matched_value": item["matched_value"],
                    "match_sources": sorted(item["match_sources"]),
                    "matched_terms": item["matched_terms"],
                }
            )
        return payload

    def _search_resolved_hits(
        self,
        query: str,
        mode: str,
        *,
        law_id: str | None,
        limit: int,
        min_score: float,
    ) -> list[dict[str, Any]]:
        hits: list[dict[str, Any]] = []
        query_normalized = normalize_name(query)
        law_filter = (law_id or "").strip()

        for row in self._resolved_entries:
            if law_filter and row["law_id"] != law_filter:
                continue
            if mode == "cas":
                score, match_type = _cas_similarity(query, row["cas_number"])
            else:
                score, match_type = _name_similarity(query, query_normalized, row["raw_name"], row["normalized_name"])
            if score < min_score:
                continue
            hits.append(
                {
                    "law_id": row["law_id"],
                    "category": row["category"],
                    "raw_name": row["raw_name"],
                    "cas_number": row["cas_number"],
                    "match_method": row["match_method"],
                    "confidence": row["confidence"],
                    "regulation_type": row["regulation_type"],
                    "score": round(score, 4),
                    "match_type": match_type,
                }
            )

        hits.sort(key=lambda item: (-item["score"], item["cas_number"], item["law_id"]))
        return hits[:limit]

    def _search_unresolved_hits(
        self,
        query: str,
        mode: str,
        *,
        law_id: str | None,
        limit: int,
        min_score: float,
    ) -> list[dict[str, Any]]:
        if mode == "cas":
            return []

        hits: list[dict[str, Any]] = []
        query_normalized = normalize_name(query)
        law_filter = (law_id or "").strip()

        for row in self._unresolved_entries:
            if law_filter and row["law_id"] != law_filter:
                continue
            score, match_type = _name_similarity(query, query_normalized, row["raw_name"], row["normalized_name"])
            if score < min_score:
                continue
            hits.append(
                {
                    "law_id": row["law_id"],
                    "category": row["category"],
                    "raw_name": row["raw_name"],
                    "score": round(score, 4),
                    "match_type": match_type,
                }
            )

        hits.sort(key=lambda item: (-item["score"], item["law_id"], item["raw_name"]))
        return hits[:limit]

    def search(
        self,
        query: str,
        mode: str = "auto",
        *,
        law_id: str | None = None,
        limit: int = 20,
        min_score: float = 0.6,
    ) -> dict[str, Any]:
        """Run similarity search over law-screening names and CAS mappings."""
        if not self._loaded:
            self._load_data()

        query_value = (query or "").strip()
        requested_mode = (mode or "auto").strip().lower()
        if requested_mode not in SEARCH_MODES:
            requested_mode = "auto"

        effective_mode = requested_mode
        if effective_mode == "auto":
            effective_mode = "cas" if looks_like_cas_query(query_value) else "name"

        bounded_limit = min(max(_safe_int(limit, 20), 1), 100)
        bounded_min_score = min(max(_safe_float(min_score, 0.6), 0.0), 1.0)

        if not query_value:
            return {
                "query": {"value": "", "requested_mode": requested_mode, "effective_mode": effective_mode},
                "substance_hits": [],
                "resolved_hits": [],
                "unresolved_hits": [],
                "total_hits": {"substance_hits": 0, "resolved_hits": 0, "unresolved_hits": 0},
            }

        substance_hits = self._search_substance_hits(
            query_value,
            effective_mode,
            limit=bounded_limit,
            min_score=bounded_min_score,
        )
        resolved_hits = self._search_resolved_hits(
            query_value,
            effective_mode,
            law_id=law_id,
            limit=bounded_limit,
            min_score=bounded_min_score,
        )
        unresolved_hits = self._search_unresolved_hits(
            query_value,
            effective_mode,
            law_id=law_id,
            limit=bounded_limit,
            min_score=bounded_min_score,
        )

        return {
            "query": {
                "value": query_value,
                "requested_mode": requested_mode,
                "effective_mode": effective_mode,
                "normalized_name": normalize_name(query_value),
                "normalized_cas": normalize_cas(query_value),
                "law_id_filter": (law_id or "").strip() or None,
                "limit": bounded_limit,
                "min_score": bounded_min_score,
            },
            "substance_hits": substance_hits,
            "resolved_hits": resolved_hits,
            "unresolved_hits": unresolved_hits,
            "total_hits": {
                "substance_hits": len(substance_hits),
                "resolved_hits": len(resolved_hits),
                "unresolved_hits": len(unresolved_hits),
            },
        }

    def lookup(
        self,
        cas_number: str | None = None,
        substance_name: str | None = None,
        language: str = "ja",
    ) -> dict[str, Any]:
        """Look up multi-law screening results by CAS and/or substance name."""
        if not self._loaded:
            self._load_data()

        language = "ja" if language == "ja" else "en"
        query = {
            "cas_number": cas_number,
            "substance_name": substance_name,
        }

        if not (cas_number or substance_name):
            return {
                "query": query,
                "matched": False,
                "matched_substances": [],
                "results": [
                    self._base_result(
                        law_code,
                        language,
                        "unknown",
                        STATUS_REASON_CODES["missing_input"],
                        "cas_number か substance_name の少なくとも一方が必要です。",
                        "Either cas_number or substance_name is required.",
                    )
                    for law_code in SUPPORTED_LAW_CODES
                ],
                "available_law_domains": self._available_law_domains(),
            }

        cas_candidates, matched_substances = self._resolve_candidates(cas_number, substance_name)

        results = [
            self._result_from_master("cscl", cas_candidates, language),
            self._result_prtr(cas_candidates, language),
            self._result_from_master("poison_control", cas_candidates, language),
            self._result_ish(cas_candidates, language),
            self._result_from_master("cwc", cas_candidates, language),
        ]

        matched = any(result["status"] in {"applies", "requires_context"} for result in results)

        return {
            "query": query,
            "matched": matched,
            "matched_substances": matched_substances,
            "results": results,
            "available_law_domains": self._available_law_domains(),
        }

    def _available_law_domains(self) -> dict[str, bool]:
        return {
            "cscl": self._master_available["cscl"],
            "cscl_master_complete": self._is_master_dataset_complete("cscl"),
            "prtr": self._regulatory_dataset_loaded,
            "poison_control": self._master_available["poison_control"],
            "poison_master_complete": self._is_master_dataset_complete("poison_control"),
            "ish": self._regulatory_dataset_loaded,
            "cwc": self._master_available["cwc"],
            "cwc_master_complete": self._is_master_dataset_complete("cwc"),
            "alias_master": self._alias_master_loaded,
        }
