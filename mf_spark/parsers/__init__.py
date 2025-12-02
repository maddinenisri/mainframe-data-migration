"""
Parsers Module.

Provides parsers for various mainframe definition files:
    - CopybookParser: Parse COBOL copybooks for VSAM record layouts
    - DDLParser: Parse DB2 DDL (CREATE TABLE) statements
    - DCLParser: Parse DB2 DCL (DCLGEN) output

Example:
    >>> from mf_spark.parsers import CopybookParser
    >>> parser = CopybookParser()
    >>> fields = parser.parse_file("CUSTREC.cpy")
    >>> for field in fields:
    ...     print(f"{field.name}: {field.pic_clause}")
"""

from mf_spark.parsers.copybook_parser import CopybookParser, CopybookField
from mf_spark.parsers.ddl_parser import DDLParser, DDLColumn
from mf_spark.parsers.dcl_parser import DCLParser

__all__ = [
    "CopybookParser",
    "CopybookField",
    "DDLParser",
    "DDLColumn",
    "DCLParser",
]
