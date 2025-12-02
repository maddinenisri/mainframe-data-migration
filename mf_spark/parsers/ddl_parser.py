"""
DB2 DDL Parser.

Parses DB2 DDL (Data Definition Language) statements to extract
table and column definitions.

Example:
    >>> parser = DDLParser()
    >>> columns = parser.parse_file("TRNTYCAT.ddl")
    >>> for col in columns:
    ...     print(f"{col.name}: {col.data_type}")

Supported DDL Statements:
    - CREATE TABLE
    - PRIMARY KEY constraints
    - FOREIGN KEY constraints
    - NOT NULL constraints
"""

import re
from dataclasses import dataclass, field
from typing import Optional
from pathlib import Path

from mf_spark.converters.db2_types import DB2TypeConverter


@dataclass
class DDLColumn:
    """
    Represents a column definition from a DB2 DDL statement.

    Attributes:
        name: Column name
        data_type: DB2 data type (e.g., "DECIMAL(15,2)")
        nullable: Whether NULL values are allowed
        default_value: Default value expression
        is_primary_key: Whether this column is part of primary key
        foreign_key_ref: Foreign key reference (schema.table.column)
    """
    name: str
    data_type: str
    nullable: bool = True
    default_value: Optional[str] = None
    is_primary_key: bool = False
    foreign_key_ref: Optional[str] = None

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "name": self.name,
            "data_type": self.data_type,
            "nullable": self.nullable,
            "default_value": self.default_value,
            "is_primary_key": self.is_primary_key,
            "foreign_key_ref": self.foreign_key_ref,
        }


@dataclass
class DDLTable:
    """
    Represents a table definition from a DB2 DDL statement.

    Attributes:
        schema: Schema name
        name: Table name
        columns: List of column definitions
        primary_key: List of primary key column names
        foreign_keys: Dict of column -> foreign reference
    """
    schema: str
    name: str
    columns: list[DDLColumn] = field(default_factory=list)
    primary_key: list[str] = field(default_factory=list)
    foreign_keys: dict[str, str] = field(default_factory=dict)

    @property
    def full_name(self) -> str:
        """Get fully qualified table name."""
        return f"{self.schema}.{self.name}"

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "schema": self.schema,
            "name": self.name,
            "full_name": self.full_name,
            "columns": [c.to_dict() for c in self.columns],
            "primary_key": self.primary_key,
            "foreign_keys": self.foreign_keys,
        }


class DDLParser:
    """
    Parser for DB2 DDL statements.

    This parser extracts table and column definitions from DB2
    CREATE TABLE statements.

    Example:
        >>> parser = DDLParser()
        >>> table = parser.parse_file("TRNTYCAT.ddl")
        >>> print(f"Table: {table.full_name}")
        >>> for col in table.columns:
        ...     print(f"  {col.name}: {col.data_type}")
    """

    # Regex patterns for parsing DDL
    CREATE_TABLE_PATTERN = re.compile(
        r"CREATE\s+TABLE\s+"
        r"(?:(\w+)\.)?(\w+)\s*"  # Schema.Table
        r"\((.*)\)",             # Column definitions
        re.IGNORECASE | re.DOTALL
    )

    COLUMN_PATTERN = re.compile(
        r"(\w+)\s+"              # Column name
        r"([A-Z]+(?:\s*\([^)]+\))?(?:\s+FOR\s+BIT\s+DATA)?)"  # Data type
        r"(\s+NOT\s+NULL)?"      # NOT NULL
        r"(\s+WITH\s+DEFAULT\s+[^,]+)?",  # DEFAULT
        re.IGNORECASE
    )

    PRIMARY_KEY_PATTERN = re.compile(
        r"PRIMARY\s+KEY\s*\(([^)]+)\)",
        re.IGNORECASE
    )

    FOREIGN_KEY_PATTERN = re.compile(
        r"FOREIGN\s+KEY\s+(\w+)\s*\(([^)]+)\)\s*"
        r"REFERENCES\s+(\w+(?:\.\w+)?)\s*\(([^)]+)\)",
        re.IGNORECASE
    )

    def __init__(self):
        """Initialize the DDL parser."""
        self.type_converter = DB2TypeConverter()

    def parse_file(self, filepath: str) -> DDLTable:
        """
        Parse a DDL file and extract table definition.

        Args:
            filepath: Path to the DDL file

        Returns:
            DDLTable object with parsed definitions

        Raises:
            FileNotFoundError: If the DDL file doesn't exist
            ValueError: If no CREATE TABLE statement found
        """
        path = Path(filepath)
        if not path.exists():
            raise FileNotFoundError(f"DDL file not found: {filepath}")

        content = path.read_text(encoding="utf-8", errors="replace")
        return self.parse_content(content)

    def parse_content(self, content: str) -> DDLTable:
        """
        Parse DDL content and extract table definition.

        Args:
            content: Raw DDL content as string

        Returns:
            DDLTable object with parsed definitions
        """
        # Clean content
        content = self._clean_content(content)

        # Find CREATE TABLE statement
        match = self.CREATE_TABLE_PATTERN.search(content)
        if not match:
            raise ValueError("No CREATE TABLE statement found")

        schema = match.group(1) or "DBO"
        table_name = match.group(2)
        columns_content = match.group(3)

        # Parse columns and constraints
        columns = []
        primary_key = []
        foreign_keys = {}

        # Split by comma, handling nested parentheses
        parts = self._split_columns(columns_content)

        for part in parts:
            part = part.strip()
            if not part:
                continue

            # Check for PRIMARY KEY constraint
            pk_match = self.PRIMARY_KEY_PATTERN.match(part)
            if pk_match:
                pk_cols = pk_match.group(1)
                primary_key = [c.strip() for c in pk_cols.split(",")]
                continue

            # Check for FOREIGN KEY constraint
            fk_match = self.FOREIGN_KEY_PATTERN.match(part)
            if fk_match:
                fk_col = fk_match.group(2).strip()
                ref_table = fk_match.group(3)
                ref_col = fk_match.group(4).strip()
                foreign_keys[fk_col] = f"{ref_table}.{ref_col}"
                continue

            # Skip other constraints
            if part.upper().startswith(("CONSTRAINT", "UNIQUE", "CHECK", "INDEX")):
                continue

            # Parse column definition
            col = self._parse_column(part)
            if col:
                columns.append(col)

        # Update primary key and foreign key flags on columns
        for col in columns:
            if col.name in primary_key:
                col.is_primary_key = True
            if col.name in foreign_keys:
                col.foreign_key_ref = foreign_keys[col.name]

        return DDLTable(
            schema=schema,
            name=table_name,
            columns=columns,
            primary_key=primary_key,
            foreign_keys=foreign_keys,
        )

    def _clean_content(self, content: str) -> str:
        """
        Clean and normalize DDL content.

        Removes comments and normalizes whitespace.
        """
        # Remove SQL comments
        content = re.sub(r"--.*$", "", content, flags=re.MULTILINE)
        content = re.sub(r"/\*.*?\*/", "", content, flags=re.DOTALL)

        # Normalize whitespace
        content = " ".join(content.split())

        return content

    def _split_columns(self, content: str) -> list[str]:
        """
        Split column definitions by comma, respecting parentheses.

        Args:
            content: Column definitions string

        Returns:
            List of individual column/constraint definitions
        """
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

    def _parse_column(self, column_def: str) -> Optional[DDLColumn]:
        """
        Parse a single column definition.

        Args:
            column_def: Column definition string

        Returns:
            DDLColumn or None if parsing fails
        """
        match = self.COLUMN_PATTERN.match(column_def)
        if not match:
            return None

        name = match.group(1).upper()
        data_type = match.group(2).strip().upper()
        not_null = match.group(3) is not None
        default_clause = match.group(4)

        default_value = None
        if default_clause:
            default_value = default_clause.strip()
            default_value = re.sub(r"^\s*WITH\s+DEFAULT\s*", "", default_value, flags=re.IGNORECASE)

        return DDLColumn(
            name=name,
            data_type=data_type,
            nullable=not not_null,
            default_value=default_value,
        )

    def to_spark_schema(self, table: DDLTable):
        """
        Convert table definition to a Spark StructType schema.

        Args:
            table: DDLTable object

        Returns:
            PySpark StructType schema
        """
        from pyspark.sql.types import StructType, StructField

        fields = []
        for col in table.columns:
            spark_type = self.type_converter.convert(col.data_type)
            fields.append(StructField(col.name, spark_type, nullable=col.nullable))

        return StructType(fields)

    def print_table_definition(self, table: DDLTable) -> str:
        """
        Generate a formatted table definition for documentation.

        Args:
            table: DDLTable object

        Returns:
            Formatted string showing table definition
        """
        lines = []
        lines.append("=" * 80)
        lines.append(f"TABLE: {table.full_name}")
        lines.append("=" * 80)
        lines.append(f"{'Column':<25} {'Type':<25} {'Null':<6} {'Key'}")
        lines.append("-" * 80)

        for col in table.columns:
            null_str = "YES" if col.nullable else "NO"
            key_str = ""
            if col.is_primary_key:
                key_str = "PK"
            if col.foreign_key_ref:
                key_str += f" FK->{col.foreign_key_ref}"
            lines.append(f"{col.name:<25} {col.data_type:<25} {null_str:<6} {key_str}")

        lines.append("-" * 80)
        if table.primary_key:
            lines.append(f"Primary Key: {', '.join(table.primary_key)}")
        lines.append("=" * 80)

        return "\n".join(lines)
