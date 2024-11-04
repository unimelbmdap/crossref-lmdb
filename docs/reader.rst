Reading from the database
=========================

The database can be queried from within Python using the :py:class:`crossref_lmdb.DBReader` class.
Instances of this class provide a context manager and a dict-like interface for interacting with DOIs and their metadata.

Example
-------

.. code:: python

    import pathlib

    import crossref_lmdb

    # assuming the database is located in the `db` subdirectory
    # at the current location
    db_dir = pathlib.Path("db/")

    with crossref_lmdb.DBReader(db_dir=db_dir) as reader:

        # fast access to the number of items
        print(f"Number of items in database: {len(reader)}")

        # a reference to the most recently-indexed item is stored
        # in the database
        print(f"Most recently-indexed item: {reader.most_recent_indexed}")

        # dict-like access to metadata for a given DOI
        doi_metadata = reader["10.7717/peerj.1038"]

        # dict-like iteration over (key, value) pairs
        for (doi, metadata) in reader.items():
            break

        # dict-like iteration over keys (DOIs)
        for doi in reader:
            break

        # dict-like iteration over values (metadata)
        for metadata in reader.values():
            break
