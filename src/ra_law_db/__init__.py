"""Runtime law-screening APIs backed by the published dataset."""

from .models import LAW_LABELS, LAW_STANDARD_NAMES, LawStatus
from .screening_db import LawScreeningDatabase


def get_law_screening_database(law_db_path: str | None = None):
    """Get the singleton law screening database.

    When ``law_db_path`` is omitted, the packaged bundled SQLite database is
    used by default.
    """
    return LawScreeningDatabase.get_instance(law_db_path)


__all__ = [
    "LAW_LABELS",
    "LAW_STANDARD_NAMES",
    "LawStatus",
    "LawScreeningDatabase",
    "get_law_screening_database",
]
