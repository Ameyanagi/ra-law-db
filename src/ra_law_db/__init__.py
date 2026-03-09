"""Runtime law-screening APIs backed by the published dataset."""

from .models import LAW_LABELS, LAW_STANDARD_NAMES, LawStatus
from .screening_db import LawScreeningDatabase


def get_law_screening_database(law_db_path: str):
    """Get singleton law screening database for a given dataset path."""
    return LawScreeningDatabase.get_instance(law_db_path)


__all__ = [
    "LAW_LABELS",
    "LAW_STANDARD_NAMES",
    "LawStatus",
    "LawScreeningDatabase",
    "get_law_screening_database",
]
