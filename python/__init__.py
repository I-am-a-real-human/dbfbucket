"""
dbfbucket - DBF file reader/writer with Arrow support
"""

from . import dbfbucket as _dbfbucket

# Re-export for cleaner API
DBF = _dbfbucket.DBF
Column = _dbfbucket.Column

__version__ = "0.1.0"
__all__ = ["DBF", "Column"]
