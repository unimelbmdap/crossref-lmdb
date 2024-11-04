import os
import logging

import pytest

import crossref_lmdb.log


def test_get_log_level():

    assert crossref_lmdb.log.get_log_level() == logging.INFO

    os.environ["CROSSREF_LMDB_LOG_LEVEL"] = "warning"

    assert crossref_lmdb.log.get_log_level() == logging.WARNING

    os.environ["CROSSREF_LMDB_LOG_LEVEL"] = "yada"

    with pytest.raises(SystemExit):
        crossref_lmdb.log.get_log_level()
