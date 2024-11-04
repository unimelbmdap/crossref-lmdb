import pathlib
import math

import pytest

import requests

import crossref_lmdb.web


def test_web(monkeypatch, test_data_dir):

    def mock_set_rate_limit(self):
        return

    monkeypatch.setattr(
        crossref_lmdb.web.CrossRefWebAPI,
        "set_rate_limit",
        mock_set_rate_limit,
    )

    def mock_call_for_total(self, query):

        content = test_data_dir / "eg_total_response.json"

        response = requests.Response()
        response._content = content.read_bytes()

        return response

    monkeypatch.setattr(
        crossref_lmdb.web.CrossRefWebAPI,
        "call",
        mock_call_for_total,
    )

    web_source = crossref_lmdb.web.WebSource(
        email_address="test@test.com",
        from_date="2024-11-01",
        show_progress=True,
        filter_func=None,
        filter_arg=None,
    )

    assert web_source.total_items == math.ceil(120131 / web_source.n_rows)

    assert web_source.show_progress
    assert web_source.total_units == "pages"
    assert web_source.filter_func is None

    n_calls = 0

    def mock_call(self, query):

        nonlocal n_calls

        content = test_data_dir / f"eg_response_{n_calls + 1}.json"

        response = requests.Response()
        response._content = content.read_bytes()

        n_calls += 1

        return response

    monkeypatch.setattr(
        crossref_lmdb.web.CrossRefWebAPI,
        "call",
        mock_call,
    )

    items = [item for item in web_source.iter_unfiltered_items_data()]

    assert len(items) == 3

    def mock_call_bad(self, query):

        bad_response = b"[1, 2, 3]"

        response = requests.Response()
        response._content = bad_response

        return response

    monkeypatch.setattr(
        crossref_lmdb.web.CrossRefWebAPI,
        "call",
        mock_call_bad,
    )

    with pytest.raises(ValueError):
        items = [item for item in web_source.iter_unfiltered_items_data()]

    def mock_call_bad(self, query):

        bad_response = b'{"message": [1, 2, 3]}'

        response = requests.Response()
        response._content = bad_response

        return response

    monkeypatch.setattr(
        crossref_lmdb.web.CrossRefWebAPI,
        "call",
        mock_call_bad,
    )

    with pytest.raises(ValueError):
        items = [item for item in web_source.iter_unfiltered_items_data()]

    def mock_call_bad(self, query):

        bad_response = (
            b'{"message": {"test": [1, 2, 3], "items": {"a": 1}, "next-cursor": "a"}}'
        )

        response = requests.Response()
        response._content = bad_response

        return response

    monkeypatch.setattr(
        crossref_lmdb.web.CrossRefWebAPI,
        "call",
        mock_call_bad,
    )

    with pytest.raises(ValueError):
        items = [item for item in web_source.iter_unfiltered_items_data()]

    query = web_source.form_query(
        from_date="2024-01-01",
        filter_arg="type:journal-article",
    )

    assert query == (
        "works?filter=from-index-date:2024-01-01,type:journal-article"
        + "&rows=1000&cursor=*&sort=indexed&order=asc"
    )
