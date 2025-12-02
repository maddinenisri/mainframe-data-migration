"""
DB2 z/OS to Spark Type Converter.

Converts DB2 SQL data types to equivalent PySpark data types.
Supports all DB2 z/OS data types including:
    - Numeric (SMALLINT, INTEGER, BIGINT, DECIMAL, FLOAT)
    - Character (CHAR, VARCHAR, CLOB)
    - Binary (BINARY, VARBINARY, BLOB)
    - Date/Time (DATE, TIME, TIMESTAMP)
    - Special (XML, ROWID, BOOLEAN)

Reference:
    See db2_to_spark_mappings.md for complete type mapping documentation.

Example:
    >>> converter = DB2TypeConverter()
    >>> converter.convert("DECIMAL(15,2)")
    DecimalType(15, 2)
    >>> converter.convert("VARCHAR(100)")
    StringType()
"""

import re
from dataclasses import dataclass
from typing import Optional
from enum import Enum
from pyspark.sql.types import (
    DataType,
    StringType,
    IntegerType,
    LongType,
    ShortType,
    FloatType,
    DoubleType,
    DecimalType,
    DateType,
    TimestampType,
    BinaryType,
    BooleanType,
)


class DB2TypeCategory(Enum):
    """Categories of DB2 data types."""
    NUMERIC_INTEGER = "integer"
    NUMERIC_DECIMAL = "decimal"
    NUMERIC_FLOAT = "float"
    CHARACTER = "character"
    GRAPHIC = "graphic"
    BINARY = "binary"
    DATETIME = "datetime"
    SPECIAL = "special"


@dataclass
class ParsedDB2Type:
    """
    Parsed representation of a DB2 data type.

    Attributes:
        base_type: Base type name (e.g., DECIMAL, VARCHAR)
        category: Type category for grouping
        precision: Precision for numeric types
        scale: Scale for decimal types
        length: Length for character/binary types
        is_nullable: Whether NULL is allowed
        ccsid: Character set ID for character types
    """
    base_type: str
    category: DB2TypeCategory
    precision: Optional[int] = None
    scale: Optional[int] = None
    length: Optional[int] = None
    is_nullable: bool = True
    ccsid: Optional[int] = None
    is_for_bit_data: bool = False


class DB2TypeConverter:
    """
    Converts DB2 z/OS data types to PySpark data types.

    This converter handles all DB2 for z/OS data types and maps them
    to appropriate Spark types for data migration.

    Supported Types:
        Numeric:
            - SMALLINT          -> ShortType
            - INTEGER/INT       -> IntegerType
            - BIGINT            -> LongType
            - DECIMAL(p,s)      -> DecimalType(p, s)
            - NUMERIC(p,s)      -> DecimalType(p, s)
            - REAL              -> FloatType
            - FLOAT/DOUBLE      -> DoubleType
            - DECFLOAT(16/34)   -> DecimalType

        Character:
            - CHAR(n)           -> StringType
            - VARCHAR(n)        -> StringType
            - CLOB              -> StringType
            - GRAPHIC(n)        -> StringType
            - VARGRAPHIC(n)     -> StringType

        Binary:
            - BINARY(n)         -> BinaryType
            - VARBINARY(n)      -> BinaryType
            - BLOB              -> BinaryType
            - CHAR FOR BIT DATA -> BinaryType

        Date/Time:
            - DATE              -> DateType
            - TIME              -> StringType
            - TIMESTAMP         -> TimestampType

        Special:
            - BOOLEAN           -> BooleanType
            - XML               -> StringType
            - ROWID             -> StringType

    Example:
        >>> converter = DB2TypeConverter()
        >>> converter.convert("DECIMAL(15,2)")
        DecimalType(15, 2)
        >>> converter.convert("VARCHAR(100)")
        StringType()
    """

    # Regex patterns for parsing DB2 types
    DECIMAL_PATTERN = re.compile(
        r"(DECIMAL|DEC|NUMERIC)\s*\(\s*(\d+)\s*(?:,\s*(\d+))?\s*\)",
        re.IGNORECASE
    )

    DECFLOAT_PATTERN = re.compile(
        r"DECFLOAT\s*\(\s*(\d+)\s*\)",
        re.IGNORECASE
    )

    CHAR_PATTERN = re.compile(
        r"(CHAR|CHARACTER|VARCHAR|LONG\s+VARCHAR)\s*\(\s*(\d+)\s*\)",
        re.IGNORECASE
    )

    GRAPHIC_PATTERN = re.compile(
        r"(GRAPHIC|VARGRAPHIC|LONG\s+VARGRAPHIC)\s*\(\s*(\d+)\s*\)",
        re.IGNORECASE
    )

    BINARY_PATTERN = re.compile(
        r"(BINARY|VARBINARY)\s*\(\s*(\d+)\s*\)",
        re.IGNORECASE
    )

    LOB_PATTERN = re.compile(
        r"(BLOB|CLOB|DBCLOB)\s*(?:\(\s*(\d+)\s*([KMG])?\s*\))?",
        re.IGNORECASE
    )

    TIMESTAMP_PATTERN = re.compile(
        r"TIMESTAMP\s*(?:\(\s*(\d+)\s*\))?\s*(WITH\s+TIME\s+ZONE)?",
        re.IGNORECASE
    )

    # Simple type mappings (no parameters)
    SIMPLE_TYPES = {
        "SMALLINT": ShortType(),
        "INTEGER": IntegerType(),
        "INT": IntegerType(),
        "BIGINT": LongType(),
        "REAL": FloatType(),
        "FLOAT": DoubleType(),
        "DOUBLE": DoubleType(),
        "DOUBLE PRECISION": DoubleType(),
        "DATE": DateType(),
        "TIME": StringType(),      # No direct Spark TIME type
        "BOOLEAN": BooleanType(),
        "XML": StringType(),
        "ROWID": StringType(),
    }

    # CCSID to Python codec mapping
    CCSID_CODECS = {
        37: "cp037",    # US/Canada English
        500: "cp500",   # International Latin-1
        1047: "cp1047", # Open Systems Latin-1
        1140: "cp1140", # US/Canada with Euro
        1200: "utf-16", # Unicode UTF-16
        1208: "utf-8",  # Unicode UTF-8
        930: "cp930",   # Japanese mixed
        935: "cp935",   # Simplified Chinese
        937: "cp937",   # Traditional Chinese
    }

    def __init__(self, default_ccsid: int = 37):
        """
        Initialize the DB2 type converter.

        Args:
            default_ccsid: Default character set ID (37 for US EBCDIC)
        """
        self.default_ccsid = default_ccsid

    def parse_type(self, db2_type: str) -> ParsedDB2Type:
        """
        Parse a DB2 data type into its components.

        Args:
            db2_type: DB2 type definition (e.g., "DECIMAL(15,2)")

        Returns:
            ParsedDB2Type with extracted components
        """
        db2_type = db2_type.strip().upper()
        original_type = db2_type

        # Check for FOR BIT DATA modifier
        is_for_bit_data = "FOR BIT DATA" in db2_type
        if is_for_bit_data:
            db2_type = db2_type.replace("FOR BIT DATA", "").strip()

        # Check for NOT NULL
        is_nullable = "NOT NULL" not in db2_type
        db2_type = db2_type.replace("NOT NULL", "").strip()

        # Try DECIMAL/NUMERIC
        match = self.DECIMAL_PATTERN.match(db2_type)
        if match:
            precision = int(match.group(2))
            scale = int(match.group(3)) if match.group(3) else 0
            return ParsedDB2Type(
                base_type="DECIMAL",
                category=DB2TypeCategory.NUMERIC_DECIMAL,
                precision=precision,
                scale=scale,
                is_nullable=is_nullable,
            )

        # Try DECFLOAT
        match = self.DECFLOAT_PATTERN.match(db2_type)
        if match:
            precision = int(match.group(1))
            return ParsedDB2Type(
                base_type="DECFLOAT",
                category=DB2TypeCategory.NUMERIC_DECIMAL,
                precision=precision,
                scale=0,
                is_nullable=is_nullable,
            )

        # Try CHAR/VARCHAR
        match = self.CHAR_PATTERN.match(db2_type)
        if match:
            base = match.group(1).upper().replace(" ", "_")
            length = int(match.group(2))
            return ParsedDB2Type(
                base_type=base,
                category=DB2TypeCategory.BINARY if is_for_bit_data else DB2TypeCategory.CHARACTER,
                length=length,
                is_nullable=is_nullable,
                is_for_bit_data=is_for_bit_data,
            )

        # Try GRAPHIC/VARGRAPHIC
        match = self.GRAPHIC_PATTERN.match(db2_type)
        if match:
            base = match.group(1).upper().replace(" ", "_")
            length = int(match.group(2))
            return ParsedDB2Type(
                base_type=base,
                category=DB2TypeCategory.GRAPHIC,
                length=length,
                is_nullable=is_nullable,
            )

        # Try BINARY/VARBINARY
        match = self.BINARY_PATTERN.match(db2_type)
        if match:
            base = match.group(1).upper()
            length = int(match.group(2))
            return ParsedDB2Type(
                base_type=base,
                category=DB2TypeCategory.BINARY,
                length=length,
                is_nullable=is_nullable,
            )

        # Try LOB types
        match = self.LOB_PATTERN.match(db2_type)
        if match:
            base = match.group(1).upper()
            size = int(match.group(2)) if match.group(2) else 0
            unit = match.group(3) or ""
            # Convert to bytes
            multipliers = {"K": 1024, "M": 1024**2, "G": 1024**3}
            length = size * multipliers.get(unit.upper(), 1)
            category = DB2TypeCategory.BINARY if base == "BLOB" else DB2TypeCategory.CHARACTER
            return ParsedDB2Type(
                base_type=base,
                category=category,
                length=length,
                is_nullable=is_nullable,
            )

        # Try TIMESTAMP
        match = self.TIMESTAMP_PATTERN.match(db2_type)
        if match:
            precision = int(match.group(1)) if match.group(1) else 6
            has_tz = match.group(2) is not None
            return ParsedDB2Type(
                base_type="TIMESTAMP_WITH_TZ" if has_tz else "TIMESTAMP",
                category=DB2TypeCategory.DATETIME,
                precision=precision,
                is_nullable=is_nullable,
            )

        # Try simple types
        for simple_type in self.SIMPLE_TYPES:
            if db2_type.startswith(simple_type):
                return ParsedDB2Type(
                    base_type=simple_type,
                    category=self._get_category(simple_type),
                    is_nullable=is_nullable,
                )

        # Default to character
        return ParsedDB2Type(
            base_type="UNKNOWN",
            category=DB2TypeCategory.CHARACTER,
            is_nullable=is_nullable,
        )

    def _get_category(self, base_type: str) -> DB2TypeCategory:
        """Get category for a simple type."""
        if base_type in ("SMALLINT", "INTEGER", "INT", "BIGINT"):
            return DB2TypeCategory.NUMERIC_INTEGER
        elif base_type in ("REAL", "FLOAT", "DOUBLE", "DOUBLE PRECISION"):
            return DB2TypeCategory.NUMERIC_FLOAT
        elif base_type in ("DATE", "TIME"):
            return DB2TypeCategory.DATETIME
        elif base_type in ("BOOLEAN", "XML", "ROWID"):
            return DB2TypeCategory.SPECIAL
        return DB2TypeCategory.CHARACTER

    def convert(self, db2_type: str) -> DataType:
        """
        Convert a DB2 data type to a Spark DataType.

        Args:
            db2_type: DB2 type definition (e.g., "DECIMAL(15,2)")

        Returns:
            Appropriate PySpark DataType

        Example:
            >>> converter.convert("DECIMAL(15,2)")
            DecimalType(15, 2)
            >>> converter.convert("VARCHAR(100)")
            StringType()
        """
        parsed = self.parse_type(db2_type)
        return self._map_to_spark_type(parsed)

    def _map_to_spark_type(self, parsed: ParsedDB2Type) -> DataType:
        """
        Map a parsed DB2 type to a Spark DataType.

        Args:
            parsed: Parsed DB2 type object

        Returns:
            Appropriate PySpark DataType
        """
        # Check simple types first
        if parsed.base_type in self.SIMPLE_TYPES:
            return self.SIMPLE_TYPES[parsed.base_type]

        # Handle by category
        if parsed.category == DB2TypeCategory.NUMERIC_DECIMAL:
            precision = parsed.precision or 18
            scale = parsed.scale or 0
            return DecimalType(precision, scale)

        if parsed.category == DB2TypeCategory.NUMERIC_FLOAT:
            return DoubleType()

        if parsed.category == DB2TypeCategory.BINARY:
            return BinaryType()

        if parsed.category == DB2TypeCategory.DATETIME:
            if parsed.base_type == "DATE":
                return DateType()
            elif parsed.base_type in ("TIMESTAMP", "TIMESTAMP_WITH_TZ"):
                return TimestampType()
            return StringType()

        if parsed.category == DB2TypeCategory.SPECIAL:
            if parsed.base_type == "BOOLEAN":
                return BooleanType()
            return StringType()

        # Default for character and graphic types
        if parsed.is_for_bit_data:
            return BinaryType()

        return StringType()

    def get_codec(self, ccsid: Optional[int] = None) -> str:
        """
        Get the Python codec for a CCSID.

        Args:
            ccsid: DB2 character set ID

        Returns:
            Python codec name (e.g., 'cp037')
        """
        ccsid = ccsid or self.default_ccsid
        return self.CCSID_CODECS.get(ccsid, "cp037")


# Convenience mapping dictionary for quick lookups
DB2_TYPE_MAP = {
    # Integer types
    "SMALLINT": ShortType(),
    "INTEGER": IntegerType(),
    "INT": IntegerType(),
    "BIGINT": LongType(),

    # Floating point
    "REAL": FloatType(),
    "FLOAT": DoubleType(),
    "DOUBLE": DoubleType(),
    "DOUBLE PRECISION": DoubleType(),

    # Character
    "CHAR": StringType(),
    "VARCHAR": StringType(),
    "CLOB": StringType(),

    # Binary
    "BINARY": BinaryType(),
    "VARBINARY": BinaryType(),
    "BLOB": BinaryType(),

    # Date/Time
    "DATE": DateType(),
    "TIME": StringType(),
    "TIMESTAMP": TimestampType(),

    # Special
    "BOOLEAN": BooleanType(),
    "XML": StringType(),
    "ROWID": StringType(),
}
