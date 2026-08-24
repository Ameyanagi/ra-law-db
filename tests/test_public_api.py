"""Tests for the package-level service façade."""

import ra_law_db


class KeywordOnlyDatabase:
    """Record façade calls while enforcing keyword-only service arguments."""

    def __init__(self):
        self.calls = []

    def lookup(self, *, cas_number=None, substance_name=None, language="ja"):
        self.calls.append(("lookup", cas_number, substance_name, language))
        return {"matched": True}

    def search(self, *, query, mode="auto", law_id=None, limit=20, min_score=0.6):
        self.calls.append(("search", query, mode, law_id, limit, min_score))
        return {"total_hits": 0}

    def list_regulated_substances(self, **kwargs):
        self.calls.append(("list", kwargs))
        return {"total": 0}

    def lookup_regulatory_info(self, *, cas_number, language="ja"):
        self.calls.append(("regulatory", cas_number, language))
        return {"regulated": False}


def test_public_facade_uses_stable_keyword_calls(monkeypatch):
    database = KeywordOnlyDatabase()
    monkeypatch.setattr(ra_law_db, "get_law_screening_database", lambda _path=None: database)

    assert ra_law_db.lookup_law_screening("75-09-2")["matched"] is True
    assert ra_law_db.search_law_substances("methylene", law_id="law-1")["total_hits"] == 0
    assert ra_law_db.list_regulated_substances(regulation_type="waste")["total"] == 0
    assert ra_law_db.lookup_regulatory_info("75-09-2")["regulated"] is False
    assert [call[0] for call in database.calls] == ["lookup", "search", "list", "regulatory"]
