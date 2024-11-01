
import pathlib

import pytest

import crossref_lmdb.params
import crossref_lmdb.main


@pytest.fixture(scope="session")
def public_data_dir():
    return pathlib.Path(__file__).parent / "data"



@pytest.fixture(scope="session")
def db_dir(tmp_path_factory):
    return tmp_path_factory.mktemp("db")



@pytest.fixture(scope="session")
def db(db_dir, public_data_dir):

    params = crossref_lmdb.params.CreateParams(
        public_data_dir=public_data_dir,
        start_from_file_num=0,
        db_dir=db_dir,
        max_db_size_gb=2_000,
        filter_path=None,
        compression_level=-1,
        show_progress=False,
        commit_frequency=1_000,
    )

    crossref_lmdb.main.run(params=params)
