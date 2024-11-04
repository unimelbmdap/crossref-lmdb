import json

import pytest

import simdjson

import crossref_lmdb.items


def test_prepare_json_items():

    data_dict = {"items": [1, 2, 3]}

    data_json = json.dumps(data_dict)

    data = simdjson.Parser().parse(data_json)

    assert (
        data["items"].mini
        == crossref_lmdb.items.prepare_json_items(data=data_json.encode()).mini
    )

    data_dict["items"] = {"test": 1}
    data_json = json.dumps(data_dict)
    with pytest.raises(ValueError):
        crossref_lmdb.items.prepare_json_items(data=data_json.encode())

    data_dict = ["test", 1]
    data_json = json.dumps(data_dict)
    with pytest.raises(ValueError):
        crossref_lmdb.items.prepare_json_items(data=data_json.encode())
