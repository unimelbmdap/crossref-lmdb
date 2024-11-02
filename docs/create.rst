Creating a database
===================

A database is created from a CrossRef public data export via the ``crossref-lmdb create`` command.

For example, the following command will read from a public data export in the ``public_data_export`` directory and create a database in the ``db`` subdirectory:

.. code:: bash

    crossref-lmdb create --public-data-dir public_data_export/ --db-dir db/

.. warning::

    Creating a database takes a very long time!
    When using the full 2024 public data export, it could take around X hours.


Filtering items
---------------

Items can be prevented from entering the database by providing a ``--filter-path`` argument to ``crossref-lmdb create``.
This argument needs to be a Python file that contains a function called ``filter_func``.
This function must accept one argument, a `simdjson <https://pysimdjson.tkte.ch/native.html#simdjson.Object>`_ dict-like representation of the item metadata, and must return ``True`` if the item is to be included in the database and ``False`` otherwise.

Note that the function must be self-contained, in that any ``import`` statements must appear within the body of the function itself.

For example, if we only wanted to include journal articles in the database, we could create a file called ``journal_article_filter.py`` containing the code:

.. code:: python

    def filter_func(item):
        return item["type"] == "journal-article"


Resuming database creation
--------------------------

If the database creation gets interrupted, it can be resumed by using the ``--start-from-file-num`` command-line option.
The argument to ``--start-from-file-num`` is the file number to resume from, which is the value that is reported by the progress bar.


Maximum database size
---------------------

The ``crossref-lmdb create`` command has an option called ``--max-db-size-gb``, which is required by LMDB to constrain the maximum allowable database size.
This is not pre-filled, so it is safe to use a large value (see `the LMDB documentation <https://lmdb.readthedocs.io/en/release/#environment-class>`_ about the ``map_size`` argument for more details).
