import json

import pytest

import simdjson

import crossref_lmdb.file


def test_file_source(shared_datadir, monkeypatch):

    public_data_dir = shared_datadir

    source = crossref_lmdb.file.FileSource(
        public_data_dir=public_data_dir,
        show_progress=False,
        filter_func=None,
        start_from_file_num=0,
    )

    assert source.total_items == 2
    assert source.total_units == "gz_files"
    assert source.show_progress == False
    assert source.filter_func is None

    item_iter = source.iter_unfiltered_items_data()

    items = [item for item in item_iter]

    assert len(items) == 2

    source = crossref_lmdb.file.FileSource(
        public_data_dir=public_data_dir,
        show_progress=False,
        filter_func=None,
        start_from_file_num=1,
    )

    assert source.total_items == 1

    item_iter = source.iter_unfiltered_items_data()

    (item_two,) = [item for item in item_iter]

    assert item_two == items[1]

    iter_items = [item for item in source]

    assert len(iter_items) == 2

    def filter_func(item):
        if item["DOI"] == "10.1108/01443589510076070":
            return False
        return True

    source = crossref_lmdb.file.FileSource(
        public_data_dir=public_data_dir,
        show_progress=False,
        filter_func=filter_func,
        start_from_file_num=0,
    )

    filtered_items = [item for item in source]

    assert len(filtered_items) == 4 - 1

    def mock_prepare_json_items(data):
        mock_array = simdjson.Parser().parse(json.dumps([1, 2]))
        return mock_array

    monkeypatch.setattr(
        crossref_lmdb.items,
        "prepare_json_items",
        mock_prepare_json_items,
    )

    i = iter(source)

    with pytest.raises(ValueError):
        next(i)

    def mock_prepare_json_items(data):
        mock_array = simdjson.Parser().parse(json.dumps([{"notDOI": "test"}]))
        return mock_array

    monkeypatch.setattr(
        crossref_lmdb.items,
        "prepare_json_items",
        mock_prepare_json_items,
    )

    values = [value for value in source]

    assert len(values) == 0
