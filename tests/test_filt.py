import pytest

import crossref_lmdb.filt


def test_get_filter_func(tmp_path):

    code = """
a = 1
    """

    tmp_file = tmp_path / "test.py"

    tmp_file.write_text(code)

    with pytest.raises(ValueError):
        _ = crossref_lmdb.filt.get_filter_func(tmp_file)

    code = "def filter_func(): return 2"

    tmp_file.write_text(code)

    filter_func = crossref_lmdb.filt.get_filter_func(tmp_file)

    assert filter_func() == 2


def test_run_code_from_text(tmp_path):

    code = """
a = 1
    """

    result = crossref_lmdb.filt.run_code_from_text(code=code)

    assert result == {"a": 1}
