import json

import pytest

import simdjson

import lmdb

import crossref_lmdb


def test_db_reader(db_dir, db, monkeypatch):

    reader = crossref_lmdb.DBReader(db_dir=db_dir)

    with reader:
        pass

    with pytest.raises(lmdb.Error):
        len(reader)

    with crossref_lmdb.DBReader(db_dir=db_dir) as reader:

        assert len(reader) == 4

        with pytest.raises(KeyError):
            reader["__most_recent_indexed"]

        for _ in reader.values():
            pass

        for _ in reader:
            pass

        for _ in reader.items():
            pass

        reader.most_recent_indexed

        reader._extract_value(raw_value=b"test")

        def mock_extract_value(raw_value):
            mock_array = simdjson.Parser().parse(json.dumps([1, 2])).mini
            return mock_array

        monkeypatch.setattr(
            reader,
            "_extract_value",
            mock_extract_value,
        )

        with pytest.raises(ValueError):
            reader[list(reader)[0]]
