.. include:: substitutions.txt

Documentation
=============

.. toctree::
    :hidden:

    create
    update
    reader
    cmd


``crossref-lmdb`` is a command-line application and Python library for accessing DOI metadata from a CrossRef public data export via a Lightning key:value (DOI:metadata) database.

The `public data export from CrossRef <https://www.crossref.org/blog/2024-public-data-file-now-available-featuring-new-experimental-formats/>`_ is a very useful way to access large amounts of DOI metadata because it avoids the need to acquire data over the web API.
However, the metadata is represented in the public data export as a large number of compressed JSON files - which makes it difficult and time-consuming to access the metadata for a given DOI.
This project imports the metadata into a `Lighting Memory-Mapped Database (LMDB) <https://en.wikipedia.org/wiki/Lightning_Memory-Mapped_Database>`_, in which the DOIs are the database keys and the associated metadata are the database values.

.. warning::

    This database is mostly only useful for projects requiring a relatively small portion of the total metadata - creating and updating the database is likely to be prohibitively slow otherwise.

Features
--------

* Create a Lightning database from the CrossRef public data export, with optional filtering of DOI items based on custom Python code.
* Update the database with items from the CrossRef web API that have been added or modified since a given date.
* Read from the database in Python via a dict-like data structure.

Limitations
-----------

* The Lightning database format is not very efficient with disk space for this data (see `the LMDB documentation <https://lmdb.readthedocs.io/en/release/#storage-efficiency-limits>`_ for more details).
* The creation of the database is very slow, with database creation from the full 2024 public data export taking multiple days.
* Updating the database is even slower.

.. note::
    This project is not affiliated with, supported by, or endorsed by CrossRef.


Installation
------------

The package can be installed using ``pip``:

.. code-block:: bash

    pip install crossref-lmdb

Using the package requires the CrossRef public data export files (2024 release) to have been downloaded.
See `the instructions from CrossRef <https://www.crossref.org/blog/2024-public-data-file-now-available-featuring-new-experimental-formats/>`_ for obtaining these files.

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


Contact
-------

Issues can be raised via the `Github repository <https://github.com/unimelbmdap/crossref-lmdb/issues>`_.


Authors
-------

Please feel free to email if you find this package to be useful or have any suggestions or feedback.

* Damien Mannion:
    * **Email:** `damien.mannion@unimelb.edu.au <mailto:damien.mannion@unimelb.edu.au>`_
    * **Organisation:** `Melbourne Data Analytics Platform <https://unimelb.edu.au/mdap>`_, `The University of Melbourne <https://www.unimelb.edu.au>`_
    * **Title:** Senior Research Data Specialist
    * **Website:** `https://www.djmannion.net <https://www.djmannion.net>`_

