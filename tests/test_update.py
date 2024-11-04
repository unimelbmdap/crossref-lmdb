import logging
import os

import requests

import crossref_lmdb.web
import crossref_lmdb.params
import crossref_lmdb.main
import crossref_lmdb


LOGGER = logging.getLogger()


def test_update(db_dir, db, test_data_dir, monkeypatch):

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

    max_db_size_gb = 1 if os.name == "nt" else 2000

    params = crossref_lmdb.params.UpdateParams(
        email_address="test@test.com",
        from_date=None,
        filter_arg=None,
        db_dir=db_dir,
        max_db_size_gb=max_db_size_gb,
        filter_path=None,
        compression_level=-1,
        show_progress=False,
        commit_frequency=1_000,
    )

    n_calls = 0

    def mock_call(self, query):

        nonlocal n_calls

        if n_calls == 0:
            content = test_data_dir / "eg_total_response.json"

            response = requests.Response()
            response._content = content.read_bytes()

        else:

            content = test_data_dir / f"eg_response_{n_calls}.json"

            LOGGER.debug(f"Returning from 'eg_response_{n_calls}.json'")

            response = requests.Response()
            response._content = content.read_bytes()

        n_calls += 1

        return response

    monkeypatch.setattr(
        crossref_lmdb.web.CrossRefWebAPI,
        "call",
        mock_call,
    )

    crossref_lmdb.main.run(params=params)

    with crossref_lmdb.DBReader(db_dir=db_dir) as reader:

        # 4 original + 4 updated
        assert len(reader) == 8

        assert reader.most_recent_indexed == "2024-11-02T04:13:48"
