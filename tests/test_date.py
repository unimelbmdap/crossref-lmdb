import json
import datetime

import pytest

import simdjson

import crossref_lmdb.date


def test_get_published_date():

    eg_dict = {
        "published": {
            "date-parts": [
                [1994, 2, 1],
                [1994, 1, 1],
            ],
        },
    }

    eg = json.dumps(eg_dict)
    item = simdjson.Parser().parse(eg)
    date = crossref_lmdb.date.get_published_date(item=item)
    assert date == datetime.date(year=1994, month=2, day=1)

    eg_dict["published"]["date-parts"].pop()
    eg = json.dumps(eg_dict)
    item = simdjson.Parser().parse(eg)
    date = crossref_lmdb.date.get_published_date(item=item)
    assert date == datetime.date(year=1994, month=2, day=1)

    eg_dict["published"]["date-parts"][0].pop()
    eg = json.dumps(eg_dict)
    item = simdjson.Parser().parse(eg)
    date = crossref_lmdb.date.get_published_date(item=item)
    assert date == datetime.date(year=1994, month=2, day=1)

    eg_dict["published"]["date-parts"][0].pop()
    eg = json.dumps(eg_dict)
    item = simdjson.Parser().parse(eg)
    date = crossref_lmdb.date.get_published_date(item=item)
    assert date == datetime.date(year=1994, month=1, day=1)

    eg_dict["published"]["date-parts"][0].pop()
    eg = json.dumps(eg_dict)
    item = simdjson.Parser().parse(eg)
    with pytest.raises(ValueError):
        date = crossref_lmdb.date.get_published_date(item=item)

    eg_dict["published"]["date-parts"] = [{"test": [1, 2, 3]}]
    eg = json.dumps(eg_dict)
    item = simdjson.Parser().parse(eg)
    with pytest.raises(ValueError):
        date = crossref_lmdb.date.get_published_date(item=item)

    eg_dict["published"]["date-parts"] = {"test": [1, 2, 3]}
    eg = json.dumps(eg_dict)
    item = simdjson.Parser().parse(eg)
    with pytest.raises(ValueError):
        date = crossref_lmdb.date.get_published_date(item=item)

    del eg_dict["published"]["date-parts"]
    eg = json.dumps(eg_dict)
    item = simdjson.Parser().parse(eg)
    date = crossref_lmdb.date.get_published_date(item=item)
    assert date is None

    eg_dict["published"] = [1, 2, 3]
    eg = json.dumps(eg_dict)
    item = simdjson.Parser().parse(eg)
    with pytest.raises(ValueError):
        date = crossref_lmdb.date.get_published_date(item=item)

    del eg_dict["published"]
    eg = json.dumps(eg_dict)
    item = simdjson.Parser().parse(eg)
    date = crossref_lmdb.date.get_published_date(item=item)
    assert date is None

    eg_dict["published"] = [1, 2, 3]
    with pytest.raises(ValueError):
        date = crossref_lmdb.date.get_published_date(item=eg_dict)


def test_get_indexed_datetime():

    eg_dict = {
        "indexed": {
            "date-time": "2023-09-13T15:51:07Z",
        },
    }

    eg = json.dumps(eg_dict)

    item = simdjson.Parser().parse(eg)

    indexed_datetime = crossref_lmdb.date.get_indexed_datetime(item=item)

    assert indexed_datetime == datetime.datetime(
        year=2023,
        month=9,
        day=13,
        hour=15,
        minute=51,
        second=7,
    )

    eg_dict["indexed"]["date-time"] = eg_dict["indexed"]["date-time"][:-1]

    eg = json.dumps(eg_dict)

    item = simdjson.Parser().parse(eg)

    with pytest.raises(ValueError):
        indexed_datetime = crossref_lmdb.date.get_indexed_datetime(item=item)

    eg_dict["indexed"]["date-time"] = 1

    eg = json.dumps(eg_dict)

    item = simdjson.Parser().parse(eg)

    with pytest.raises(ValueError):
        indexed_datetime = crossref_lmdb.date.get_indexed_datetime(item=item)

    del eg_dict["indexed"]["date-time"]

    eg = json.dumps(eg_dict)

    item = simdjson.Parser().parse(eg)

    indexed_datetime = crossref_lmdb.date.get_indexed_datetime(item=item)

    assert indexed_datetime is None

    eg_dict["indexed"] = [1]

    eg = json.dumps(eg_dict)

    item = simdjson.Parser().parse(eg)

    with pytest.raises(ValueError):
        indexed_datetime = crossref_lmdb.date.get_indexed_datetime(item=item)

    del eg_dict["indexed"]

    eg = json.dumps(eg_dict)

    item = simdjson.Parser().parse(eg)

    indexed_datetime = crossref_lmdb.date.get_indexed_datetime(item=item)

    assert indexed_datetime is None
