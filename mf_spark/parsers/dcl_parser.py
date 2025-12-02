"""
DB2 DCL (DCLGEN) Parser.

Parses DB2 DCLGEN output files which contain both SQL declarations
and COBOL host variable definitions.

Example:
    >>> parser = DCLParser()
    >>> result = parser.parse_file("DCLTRCAT.dcl")
    >>> print(f"Table: {result.table_name}")
    >>> for col in result.sql_columns:
    ...     print(f"{col.name}: {col.data_type}")

DCLGEN files contain:
    - EXEC SQL DECLARE statements with table structure
    - COBOL 01 level record with host variables
    - Mapping between SQL columns and COBOL fields
"""

import re
from dataclasses import dataclass, field
from typing import Optional
from pathlib import Path

from mf_spark.parsers.ddl_parser import DDLColumn
from mf_spark.converters.db2_types import DB2TypeConverter
from mf_spark.converters.vsam_types import VSAMTypeConverter


@dataclass
class CobolHostVariable:
    """
    Represents a COBOL host variable from DCLGEN output.

    Attributes:
        name: COBOL variable name (e.g., "DCL-CUST-ID")
        level: COBOL level number
        pic_clause: PIC clause for the variable
        sql_column: Corresponding SQL column name
    """
    name: str
    level: int
    pic_clause: Optional[str] = None
    sql_column: Optional[str] = None

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "name": self.name,
            "level": self.level,
            "pic_clause": self.pic_clause,
            "sql_column": self.sql_column,
        }


@dataclass
class DCLGenResult:
    """
    Result of parsing a DCLGEN file.

    Attributes:
        table_name: Full table name (schema.table)
        schema: Schema name
        table: Table name only
        sql_columns: List of SQL column definitions
        host_variables: List of COBOL host variable definitions
        column_count: Number of columns in the table
    """
    table_name: str
    schema: str = ""
    table: str = ""
    sql_columns: list[DDLColumn] = field(default_factory=list)
    host_variables: list[CobolHostVariable] = field(default_factory=list)
    column_count: int = 0

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "table_name": self.table_name,
            "schema": self.schema,
            "table": self.table,
            "sql_columns": [c.to_dict() for c in self.sql_columns],
            "host_variables": [h.to_dict() for h in self.host_variables],
            "column_count": self.column_count,
        }


class DCLParser:
    """
    Parser for DB2 DCLGEN output files.

    DCLGEN (Declarations Generator) produces files containing:
    1. EXEC SQL DECLARE TABLE statement with SQL types
    2. COBOL 01-level record with host variable definitions

    This parser extracts both SQL and COBOL definitions and
    maps them together for migration purposes.

    Example:
        >>> parser = DCLParser()
        >>> result = parser.parse_file("DCLTRCAT.dcl")
        >>> for col, var in zip(result.sql_columns, result.host_variables):
        ...     print(f"{col.name} ({col.data_type}) -> {var.name}")
    """

    # Pattern for EXEC SQL DECLARE TABLE
    DECLARE_PATTERN = re.compile(
        r"EXEC\s+SQL\s+DECLARE\s+"
        r"(\w+(?:\.\w+)?)\s+TABLE\s*"  # Table name
        r"\((.*?)\)\s*END-EXEC",       # Column definitions
        re.IGNORECASE | re.DOTALL
    )

    # Pattern for SQL column in DECLARE
    SQL_COLUMN_PATTERN = re.compile(
        r"(\w+)\s+"                    # Column name
        r"([A-Z]+(?:\s*\([^)]+\))?)"   # Data type
        r"(\s+NOT\s+NULL)?",           # NOT NULL
        re.IGNORECASE
    )

    # Pattern for COBOL 01 level record
    COBOL_RECORD_PATTERN = re.compile(
        r"01\s+([\w-]+)\.\s*"          # Record name
        r"((?:\s*\d{2}\s+[\w-]+.*?\.)+)",  # Field definitions
        re.IGNORECASE | re.DOTALL
    )

    # Pattern for COBOL field
    COBOL_FIELD_PATTERN = re.compile(
        r"(\d{2})\s+([\w-]+)"          # Level and name
        r"(?:\s+PIC\s+([^\s.]+))?"     # Optional PIC clause
        r"(?:\s+(COMP(?:-[1-5])?))?"   # Optional COMP
        r"(?:\s+USAGE\s+(\w+))?",      # Optional USAGE
        re.IGNORECASE
    )

    # Pattern for column count comment
    COLUMN_COUNT_PATTERN = re.compile(
        r"NUMBER\s+OF\s+COLUMNS\s+.*?IS\s+(\d+)",
        re.IGNORECASE
    )

    def __init__(self):
        """Initialize the DCL parser."""
        self.db2_converter = DB2TypeConverter()
        self.vsam_converter = VSAMTypeConverter()

    def parse_file(self, filepath: str) -> DCLGenResult:
        """
        Parse a DCLGEN file and extract definitions.

        Args:
            filepath: Path to the DCL file

        Returns:
            DCLGenResult with parsed definitions

        Raises:
            FileNotFoundError: If the DCL file doesn't exist
        """
        path = Path(filepath)
        if not path.exists():
            raise FileNotFoundError(f"DCL file not found: {filepath}")

        content = path.read_text(encoding="utf-8", errors="replace")
        return self.parse_content(content)

    def parse_content(self, content: str) -> DCLGenResult:
        """
        Parse DCLGEN content and extract definitions.

        Args:
            content: Raw DCL content as string

        Returns:
            DCLGenResult with parsed definitions
        """
        result = DCLGenResult(table_name="")

        # Parse EXEC SQL DECLARE
        declare_match = self.DECLARE_PATTERN.search(content)
        if declare_match:
            table_name = declare_match.group(1)
            result.table_name = table_name

            # Split schema and table
            if "." in table_name:
                result.schema, result.table = table_name.split(".", 1)
            else:
                result.table = table_name

            # Parse columns
            columns_content = declare_match.group(2)
            result.sql_columns = self._parse_sql_columns(columns_content)

        # Parse COBOL record
        result.host_variables = self._parse_cobol_record(content)

        # Get column count
        count_match = self.COLUMN_COUNT_PATTERN.search(content)
        if count_match:
            result.column_count = int(count_match.group(1))
        else:
            result.column_count = len(result.sql_columns)

        # Map SQL columns to host variables
        self._map_columns_to_variables(result)

        return result

    def _parse_sql_columns(self, content: str) -> list[DDLColumn]:
        """
        Parse SQL column definitions from DECLARE statement.

        Args:
            content: Column definitions string

        Returns:
            List of DDLColumn objects
        """
        columns = []

        # Split by comma, handling nested parentheses
        parts = self._split_by_comma(content)

        for part in parts:
            part = part.strip()
            if not part:
                continue

            match = self.SQL_COLUMN_PATTERN.match(part)
            if match:
                name = match.group(1).upper()
                data_type = match.group(2).strip().upper()
                not_null = match.group(3) is not None

                columns.append(DDLColumn(
                    name=name,
                    data_type=data_type,
                    nullable=not not_null,
                ))

        return columns

    def _parse_cobol_record(self, content: str) -> list[CobolHostVariable]:
        """
        Parse COBOL host variable definitions.

        Args:
            content: Full DCL content

        Returns:
            List of CobolHostVariable objects
        """
        variables = []

        # Find all COBOL field definitions
        # Look for patterns like "10 DCL-FIELD-NAME PIC X(10)."
        lines = content.split("\n")
        for line in lines:
            # Clean line
            line = line.strip()
            if not line:
                continue

            match = self.COBOL_FIELD_PATTERN.search(line)
            if match:
                level = int(match.group(1))
                name = match.group(2).upper()

                # Skip level 01 (record name)
                if level == 1:
                    continue

                # Skip level 49 (VARCHAR length field)
                if level == 49:
                    continue

                pic_clause = None
                if match.group(3):
                    pic_clause = f"PIC {match.group(3)}"
                    if match.group(4):  # COMP modifier
                        pic_clause += f" {match.group(4)}"
                    elif match.group(5):  # USAGE
                        pic_clause += f" {match.group(5)}"

                variables.append(CobolHostVariable(
                    name=name,
                    level=level,
                    pic_clause=pic_clause,
                ))

        return variables

    def _map_columns_to_variables(self, result: DCLGenResult) -> None:
        """
        Map SQL columns to COBOL host variables by name pattern.

        DCLGEN typically uses naming convention:
        SQL column: COLUMN_NAME
        COBOL var:  DCL-COLUMN-NAME
        """
        for var in result.host_variables:
            # Convert COBOL name to potential SQL name
            # Remove DCL- prefix and convert hyphens to underscores
            sql_name = var.name
            if sql_name.startswith("DCL-"):
                sql_name = sql_name[4:]
            sql_name = sql_name.replace("-", "_")

            # Find matching SQL column
            for col in result.sql_columns:
                if col.name == sql_name:
                    var.sql_column = col.name
                    break

    def _split_by_comma(self, content: str) -> list[str]:
        """Split content by comma, respecting parentheses."""
        parts = []
        current = ""
        paren_depth = 0

        for char in content:
            if char == "(":
                paren_depth += 1
                current += char
            elif char == ")":
                paren_depth -= 1
                current += char
            elif char == "," and paren_depth == 0:
                parts.append(current.strip())
                current = ""
            else:
                current += char

        if current.strip():
            parts.append(current.strip())

        return parts

    def to_spark_schema(self, result: DCLGenResult):
        """
        Convert DCL result to a Spark StructType schema.

        Args:
            result: DCLGenResult object

        Returns:
            PySpark StructType schema
        """
        from pyspark.sql.types import StructType, StructField

        fields = []
        for col in result.sql_columns:
            spark_type = self.db2_converter.convert(col.data_type)
            fields.append(StructField(col.name, spark_type, nullable=col.nullable))

        return StructType(fields)

    def print_mapping(self, result: DCLGenResult) -> str:
        """
        Generate a formatted SQL-to-COBOL mapping for documentation.

        Args:
            result: DCLGenResult object

        Returns:
            Formatted string showing the mapping
        """
        lines = []
        lines.append("=" * 80)
        lines.append(f"DCLGEN MAPPING: {result.table_name}")
        lines.append("=" * 80)
        lines.append(f"{'SQL Column':<25} {'SQL Type':<20} {'COBOL Variable':<25} {'PIC'}")
        lines.append("-" * 80)

        for col in result.sql_columns:
            # Find matching COBOL variable
            cobol_name = ""
            pic = ""
            for var in result.host_variables:
                if var.sql_column == col.name:
                    cobol_name = var.name
                    pic = var.pic_clause or ""
                    break

            lines.append(f"{col.name:<25} {col.data_type:<20} {cobol_name:<25} {pic}")

        lines.append("-" * 80)
        lines.append(f"Total Columns: {result.column_count}")
        lines.append("=" * 80)

        return "\n".join(lines)
