"""Runtime law-screening APIs backed by the published dataset."""

from .models import LAW_LABELS, LAW_STANDARD_NAMES, LawStatus
from .screening_db import LawScreeningDatabase

__version__ = "0.3.1"


def get_law_screening_database(law_db_path: str | None = None):
    """Get the singleton law screening database.

    When ``law_db_path`` is omitted, the packaged bundled SQLite database is
    used by default.
    """
    return LawScreeningDatabase.get_instance(law_db_path)


def lookup_law_screening(
    cas_number: str | None = None,
    substance_name: str | None = None,
    language: str = "ja",
    law_db_path: str | None = None,
):
    """Screen a substance across every supported law domain."""
    return get_law_screening_database(law_db_path).lookup(
        cas_number=cas_number,
        substance_name=substance_name,
        language=language,
    )


def search_law_substances(
    query: str,
    mode: str = "auto",
    law_id: str | None = None,
    limit: int = 20,
    min_score: float = 0.6,
    law_db_path: str | None = None,
):
    """Search the published law dataset by CAS number or name."""
    return get_law_screening_database(law_db_path).search(
        query=query,
        mode=mode,
        law_id=law_id,
        limit=limit,
        min_score=min_score,
    )


def list_regulated_substances(
    regulation_type: str | None = None,
    regulation_class: int | None = None,
    special_management_only: bool = False,
    language: str = "ja",
    offset: int = 0,
    limit: int = 100,
    law_db_path: str | None = None,
):
    """List regulatory records using the canonical library service."""
    return get_law_screening_database(law_db_path).list_regulated_substances(
        regulation_type=regulation_type,
        regulation_class=regulation_class,
        special_management_only=special_management_only,
        language=language,
        offset=offset,
        limit=limit,
    )


def lookup_regulatory_info(
    cas_number: str,
    language: str = "ja",
    law_db_path: str | None = None,
):
    """Return the restored detailed compatibility projection."""
    return get_law_screening_database(law_db_path).lookup_regulatory_info(
        cas_number=cas_number,
        language=language,
    )


__all__ = [
    "LAW_LABELS",
    "LAW_STANDARD_NAMES",
    "LawStatus",
    "LawScreeningDatabase",
    "get_law_screening_database",
    "list_regulated_substances",
    "lookup_law_screening",
    "lookup_regulatory_info",
    "search_law_substances",
    "__version__",
]
