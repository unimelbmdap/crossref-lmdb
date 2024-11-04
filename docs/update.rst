Updating a database
===================

The public data export from CrossRef will represent the state of the CrossRef system at a particular date (around May 2, 2024, for the 2024 public data release).
To update the database with recently changed or added records, you can use the ``crossref-lmdb update`` command.
This command queries the CrossRef web API based on the ``from-index-date`` metadata field; because the web API is being queried, it requires an email address to be provided so as to access the requested 'polite' API pool.

For example, the following command will update a database (previously created via ``crossref-lmdb create``) that is present in the ``db`` subdirectory and where ``${EMAIL}`` is your email address.
This will automatically update from the most recent index date in the database.

.. code:: bash

    crossref-lmdb update --db-dir db/ --email-address ${EMAIL}


.. warning::

    Updating a database is very slow, and will almost always require item filtering (described below) to be feasible.


Filtering items
---------------

As with `creating a database <create.html#filtering-items>`_, the update command can be used with a ``filter-path`` argument to specify a Python function to filter items from affecting the database.

However, a much better strategy with updating is to instead provide a ``filter-arg`` parameter.
This ``filter-arg`` setting is used when querying the web API and so limits the number of items that are required to be retrieved from the CrossRef server.
See the `CrossRef API documentation <https://api.crossref.org/swagger-ui/index.html#/Works/get_works>`_ for details on the available filters and how they are specified.

For example, the filter for only journal articles that was used when creating the database can instead be specified as: ``--filter-arg 'type:journal-article'``.
The filter that additionally only includes DOIs published between 2021 and 2023 can be specified as ``--filter-arg 'type:journal-article,from-pub-date:2021,until-pub-date:2023'``.


Setting the date to update from
-------------------------------

By default, ``crossref-lmdb update`` will update from the date of the most recently indexed item in the database.
If you would like to update from a different date, you can use the ``--from-date`` parameter.
This value is in one of ``YYYY``, ``YYYY-MM``, or ``YYYY-MM-DD`` formats.
