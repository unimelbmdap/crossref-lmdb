import pathlib

import crossref_lmdb


def test_reader_example(db, db_dir):

    with crossref_lmdb.DBReader(db_dir=db_dir) as reader:

        # print(f"Number of items in database: {len(reader)}")
        assert len(reader) == 4

        # a reference to the most recently-indexed item is stored
        # in the database
        # print(f"Most recently-indexed item: {reader.most_recent_indexed}")
        assert reader.most_recent_indexed == "2023-11-14T16:11:39"

        # dict-like access to metadata for a given DOI
        # doi_metadata = reader["10.7717/peerj.1038"]
        doi_metadata = reader["10.1021/jo020170p"]

        # dict-like iteration over (key, value) pairs
        for doi, metadata in reader.items():
            pass

        # dict-like iteration over keys (DOIs)
        for doi in reader:
            pass

        # dict-like iteration over values (metadata)
        for metadata in reader.values():
            pass
