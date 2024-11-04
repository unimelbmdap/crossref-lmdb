import pathlib

import pytest

import crossref_lmdb.params


def test_create_params(db_dir, public_data_dir, tmp_path_factory):

    tmp_dir = tmp_path_factory.mktemp("cparams")

    params = crossref_lmdb.params.CreateParams(
        public_data_dir=public_data_dir,
        start_from_file_num=0,
        db_dir=db_dir,
        max_db_size_gb=1,
        filter_path=None,
        compression_level=-1,
        show_progress=False,
        commit_frequency=1_000,
    )

    assert params.max_db_size_bytes == 1 * 1000**3

    params.max_db_size_gb = 0.5 / (1000**3)

    with pytest.raises(ValueError):
        params.max_db_size_bytes

    params.max_db_size_gb = 1

    filt_path = tmp_dir / "filt_func.py"

    filt_path.write_text("def filter_func(item): pass")

    params = crossref_lmdb.params.CreateParams(
        public_data_dir=public_data_dir,
        start_from_file_num=0,
        db_dir=db_dir,
        max_db_size_gb=1,
        filter_path=filt_path,
        compression_level=-1,
        show_progress=False,
        commit_frequency=1_000,
    )

    with pytest.raises(ValueError):
        params = crossref_lmdb.params.CreateParams(
            public_data_dir=pathlib.Path("doesntexist"),
            start_from_file_num=0,
            db_dir=db_dir,
            max_db_size_gb=1,
            filter_path=filt_path,
            compression_level=-1,
            show_progress=False,
            commit_frequency=1_000,
        )

    with pytest.raises(ValueError):
        params = crossref_lmdb.params.CreateParams(
            db_dir=pathlib.Path("doesntexist"),
            start_from_file_num=0,
            public_data_dir=public_data_dir,
            max_db_size_gb=1,
            filter_path=filt_path,
            compression_level=-1,
            show_progress=False,
            commit_frequency=1_000,
        )

    with pytest.raises(ValueError):
        params = crossref_lmdb.params.CreateParams(
            db_dir=db_dir,
            start_from_file_num=0,
            public_data_dir=public_data_dir,
            max_db_size_gb=-1,
            filter_path=filt_path,
            compression_level=-1,
            show_progress=False,
            commit_frequency=1_000,
        )

    with pytest.raises(ValueError):
        params = crossref_lmdb.params.CreateParams(
            db_dir=db_dir,
            start_from_file_num=0,
            public_data_dir=public_data_dir,
            max_db_size_gb=1,
            filter_path=filt_path,
            compression_level=11,
            show_progress=False,
            commit_frequency=1_000,
        )

    with pytest.raises(ValueError):
        params = crossref_lmdb.params.CreateParams(
            db_dir=db_dir,
            start_from_file_num=0,
            public_data_dir=public_data_dir,
            max_db_size_gb=1,
            filter_path=pathlib.Path("doesntexist"),
            compression_level=-1,
            show_progress=False,
            commit_frequency=1_000,
        )

    bad_path = filt_path.with_suffix(".txt")
    bad_path.touch()

    with pytest.raises(ValueError):
        params = crossref_lmdb.params.CreateParams(
            db_dir=db_dir,
            start_from_file_num=0,
            public_data_dir=public_data_dir,
            max_db_size_gb=1,
            filter_path=bad_path,
            compression_level=-1,
            show_progress=False,
            commit_frequency=1_000,
        )


def test_update_params(db_dir, tmp_path_factory):

    tmp_dir = tmp_path_factory.mktemp("uparams")

    params = crossref_lmdb.params.UpdateParams(
        email_address="test@test.com",
        from_date=None,
        filter_arg=None,
        db_dir=db_dir,
        max_db_size_gb=1,
        filter_path=None,
        compression_level=-1,
        show_progress=False,
        commit_frequency=1_000,
    )

    with pytest.raises(ValueError):
        params = crossref_lmdb.params.UpdateParams(
            email_address="notavalidemail",
            from_date=None,
            filter_arg=None,
            db_dir=db_dir,
            max_db_size_gb=1,
            filter_path=None,
            compression_level=-1,
            show_progress=False,
            commit_frequency=1_000,
        )

    params = crossref_lmdb.params.UpdateParams(
        email_address="test@test.com",
        from_date="2020-10-18",
        filter_arg=None,
        db_dir=db_dir,
        max_db_size_gb=1,
        filter_path=None,
        compression_level=-1,
        show_progress=False,
        commit_frequency=1_000,
    )

    with pytest.raises(ValueError):
        params = crossref_lmdb.params.UpdateParams(
            email_address="test@test.com",
            from_date="2020-18-10",
            filter_arg=None,
            db_dir=db_dir,
            max_db_size_gb=1,
            filter_path=None,
            compression_level=-1,
            show_progress=False,
            commit_frequency=1_000,
        )
