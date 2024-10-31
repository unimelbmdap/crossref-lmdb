# crossref-lmdb

A command-line application and Python library for converting the CrossRef public data export into a Lightning DOI:metadata key:value database.

> [!NOTE]
> This project is not affiliated with, supported by, or endorsed by CrossRef.



## Installation

The package can be installed using `pip`:

```bash
pip install crossref-lmdb
```

Using the package requires the CrossRef public data export files (2024 release) to have been downloaded.
See [the instructions from CrossRef](https://www.crossref.org/blog/2024-public-data-file-now-available-featuring-new-experimental-formats/) for obtaining these files.

## Creating a database


## Updating a database


## Reading from the database in Python


The database is available as a Python [`Mapping`](https://docs.python.org/3/glossary.html#term-mapping) object via the `crossref_lmdb.DBReader` class.


```python
import pathlib

import crossref_lmdb

# assuming the database is located in the `db` subdirectory
# at the current location
db_dir = pathlib.Path("db/")

with crossref_lmdb.DBReader(db_dir=db_dir) as reader:

    print(f"Number of items in database: {len(reader)}")

    print(f"Most recently-indexed item: {reader.most_recent_indexed}")

    doi_metadata = reader["10.7717/peerj.1038"]

    for (doi, metadata) in reader.items():
        break

    for doi in reader:
        break

    for metadata in reader.values():
        break
```
