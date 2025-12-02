"""
Utilities Module.

Provides utility functions for mainframe data migration:
    - encoding: EBCDIC to ASCII conversion utilities
    - logging: Logging configuration and helpers
    - file_utils: File and directory operations

Example:
    >>> from mf_spark.utils import EBCDICConverter
    >>> converter = EBCDICConverter("cp037")
    >>> ascii_text = converter.decode(ebcdic_bytes)
"""

from mf_spark.utils.encoding import EBCDICConverter, CCSID_CODECS
from mf_spark.utils.file_utils import clean_output, ensure_directory, get_file_checksum

__all__ = [
    "EBCDICConverter",
    "CCSID_CODECS",
    "clean_output",
    "ensure_directory",
    "get_file_checksum",
]
