Creating a database
===================

A database is created from a CrossRef public data export via the ``crossref-lmdb create`` command - see :doc:`/cmd` for the command options and defaults.

For example, the following command will read from a public data export in the ``public_data_export`` directory (i.e., the directory containing the series of ``0.json.gz``, ``1.json.gz``, etc. files) and create a database in the ``db`` subdirectory:

.. code:: bash

    crossref-lmdb create --public-data-dir public_data_export/ --db-dir db/

.. warning::

    Creating a database takes a very long time!
    When using the full 2024 public data export, it could take several days.


Filtering items
---------------

The intended usage of this database is where you do not need *all* of the metadata within the public data export; for example, you may only want metadata for DOIs that relate to journal articles or span a particular range of published years.
Items can be prevented from entering the database by providing a ``--filter-path`` argument to ``crossref-lmdb create``.
This argument needs to be a Python file that contains a function called ``filter_func``.
This function must accept one argument, a dict-like representation of the item metadata, and must return ``True`` if the item is to be included in the database and ``False`` otherwise.

.. note::
    The function must be self-contained, in that any ``import`` statements must appear within the body of the function itself.

For example, if we only wanted to include journal articles in the database, we could create a file called ``journal_article_filter.py`` containing the code:

.. code:: python

    def filter_func(item):
        return "type" in item and item["type"] == "journal-article"

If we only wanted to include publications from say 2021 to 2023, we could instead specify the function as:

.. code:: python

    def filter_func(item):

        import datetime
        import crossref_lmdb.date

        pub_date = crossref_lmdb.date.get_published_date(item=item)

        if pub_date is None:
            return False

        start_date = datetime.date(year=2021, month=1, day=1)
        end_date = datetime.date(year=2024, month=1, day=1)

        date_ok = start_date <= pub_date < end_date

        return date_ok

.. note::
    We are using a helper function from ``crossref_lmdb.date`` to extract a Python date object from the item metadata.


Resuming database creation
--------------------------

If the database creation gets interrupted, it can be resumed by using the ``--start-from-file-num`` command-line option.
The argument to ``--start-from-file-num`` is the file number to resume from, which is the value that is reported by the progress bar.


Maximum database size
---------------------

The ``crossref-lmdb create`` command has an option called ``--max-db-size-gb``, which is required by LMDB to constrain the maximum allowable database size.
On Linux and Mac platforms, this is not pre-filled and so it is safe to use a large value (see `the LMDB documentation <https://lmdb.readthedocs.io/en/release/#environment-class>`_ about the ``map_size`` argument for more details).
However, on Windows it does seem to be pre-filled and so it needs to be set to a value that is appropriate for your anticipated database size (the default is also lowered to 2 GB).
