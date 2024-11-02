.. include:: substitutions.txt

Documentation
=============

.. toctree::
    :hidden:

    create
    update
    reader
    cmd


``crossref-lmdb`` is a command-line application and Python library for converting DOI metadata from a CrossRef public data export into a Lightning key:value (DOI:metadata) database.

The `public data export from CrossRef <https://www.crossref.org/blog/2024-public-data-file-now-available-featuring-new-experimental-formats/>`_ is a very useful way to access large amounts of DOI metadata because it avoids the need to acquire data over the web API.
However, the metadata is represented in the public data export as a large number of compressed JSON files - which makes it difficult and time-consuming to access the metadata for a given DOI.
This project imports the metadata into a `Lighting Memory-Mapped Database (LMDB) <https://en.wikipedia.org/wiki/Lightning_Memory-Mapped_Database>`_, in which the DOIs are the database keys and the associated metadata are the database values.

.. note::
    This project is not affiliated with, supported by, or endorsed by CrossRef.

Features
--------

* Creating a Lightning database from the CrossRef public data export, with optional compression of stored metadata values and filtering of DOI items based on custom Python code.
* Updating the database with items from the CrossRef web API that have been added or modified since a given date.
* Reading from the database in Python via a dict-like data structure.

Limitations
-----------

* The Lightning database format is not very efficient with disk space for this data (see `the LMDB documentation <https://lmdb.readthedocs.io/en/release/#storage-efficiency-limits>`_ for more details); the full 2024 public data export (X GB of compressed JSON) is about X GB.
* The creation of the database is quite slow, with database creation from the full 2024 public data export taking around X hours.
* Updating the database is even slower - perhaps prohibitively slow. Updating from the 2024 public data export to November X 2024 requires updating X items, taking around a month.


Installation
------------

The package can be installed using ``pip``:

.. code-block:: bash

    pip install git+https://github.com/unimelbmdap/crossref-lmdb


Documentation guide
-------------------

:doc:`/create`
    Creating a database from the CrossRef public data export.

:doc:`/update`
    Updating a database using the CrossRef web API.

:doc:`/reader`
    Reading from the database in Python.

:doc:`/cmd`
    A reference for the ``crossref-lmdb`` command-line application and its options.
