import crossref_lmdb.log

from crossref_lmdb.db import DBReader

crossref_lmdb.log.setup_logging()

__all__ = ("DBReader",)
__version__ = "0.1.2"
_project_url = "https://github.com/unimelbmdap/crossref-lmdb"
