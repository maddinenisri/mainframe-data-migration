"""
VSAM/COBOL to Spark Type Converter.

Converts COBOL PIC (Picture) clauses to equivalent PySpark data types.
Supports all common COBOL data types including:
    - Alphanumeric (PIC X, PIC A)
    - Zoned Decimal (PIC 9, PIC S9)
    - Packed Decimal (COMP-3)
    - Binary (COMP, COMP-4, COMP-5)
    - Floating Point (COMP-1, COMP-2)

Reference:
    See vsam_to_spark_mappings.md for complete type mapping documentation.

Example:
    >>> converter = VSAMTypeConverter()
    >>> converter.convert("PIC X(25)")
    StringType()
    >>> converter.convert("PIC S9(7)V99 COMP-3")
    DecimalType(9, 2)
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
)


class CobolUsage(Enum):
    """COBOL USAGE clause types."""
    DISPLAY = "DISPLAY"         # Default, zoned decimal
    COMP = "COMP"               # Binary
    COMP_1 = "COMP-1"           # Single-precision float
    COMP_2 = "COMP-2"           # Double-precision float
    COMP_3 = "COMP-3"           # Packed decimal
    COMP_4 = "COMP-4"           # Binary (same as COMP)
    COMP_5 = "COMP-5"           # Native binary


class CobolPicType(Enum):
    """COBOL PIC clause base types."""
    ALPHANUMERIC = "X"          # PIC X
    ALPHABETIC = "A"            # PIC A
    NUMERIC = "9"               # PIC 9
    SIGNED_NUMERIC = "S9"       # PIC S9


@dataclass
class ParsedPicClause:
    """
    Parsed representation of a COBOL PIC clause.

    Attributes:
        pic_type: Base PIC type (X, A, 9, S9)
        integer_digits: Number of digits before decimal point
        decimal_digits: Number of digits after decimal point (V)
        usage: USAGE clause (COMP, COMP-3, etc.)
        total_length: Total storage length in bytes
        is_signed: Whether the field is signed
    """
    pic_type: CobolPicType
    integer_digits: int = 0
    decimal_digits: int = 0
    usage: CobolUsage = CobolUsage.DISPLAY
    total_length: int = 0
    is_signed: bool = False

    @property
    def total_digits(self) -> int:
        """Total number of digits (integer + decimal)."""
        return self.integer_digits + self.decimal_digits

    @property
    def is_alphanumeric(self) -> bool:
        """Check if this is an alphanumeric field."""
        return self.pic_type in (CobolPicType.ALPHANUMERIC, CobolPicType.ALPHABETIC)

    @property
    def is_packed(self) -> bool:
        """Check if this is a packed decimal field (COMP-3)."""
        return self.usage == CobolUsage.COMP_3

    @property
    def is_binary(self) -> bool:
        """Check if this is a binary field (COMP/COMP-4/COMP-5)."""
        return self.usage in (CobolUsage.COMP, CobolUsage.COMP_4, CobolUsage.COMP_5)


class VSAMTypeConverter:
    """
    Converts COBOL PIC clauses to PySpark data types.

    This converter handles all common COBOL data type definitions and
    maps them to appropriate Spark types for data migration.

    Supported Patterns:
        - PIC X(n)              -> StringType
        - PIC A(n)              -> StringType
        - PIC 9(n)              -> LongType/IntegerType/ShortType
        - PIC S9(n)             -> LongType/IntegerType/ShortType
        - PIC 9(n)V9(m)         -> DecimalType(n+m, m)
        - PIC S9(n)V9(m) COMP-3 -> DecimalType(n+m, m)
        - COMP-1                -> FloatType
        - COMP-2                -> DoubleType

    Example:
        >>> converter = VSAMTypeConverter()
        >>> converter.convert("PIC X(50)")
        StringType()
        >>> converter.convert("PIC S9(7)V99 COMP-3")
        DecimalType(9, 2)
    """

    # Regex patterns for parsing PIC clauses
    PIC_PATTERN = re.compile(
        r"PIC\s+"
        r"(S)?"                          # Optional sign
        r"([XA9])"                        # Base type
        r"\((\d+)\)"                      # Length
        r"(?:V(?:9\((\d+)\)|(\d+)))?"    # Optional decimal (V9(n) or Vnn)
        r"(?:\s+(COMP(?:-[1-5])?))?"     # Optional COMP clause
        r"(?:\s+OCCURS\s+(\d+))?"        # Optional OCCURS
        , re.IGNORECASE
    )

    # Alternate pattern for inline digits (PIC 999 instead of PIC 9(3))
    PIC_INLINE_PATTERN = re.compile(
        r"PIC\s+"
        r"(S)?"                          # Optional sign
        r"([XA9]+)"                       # Repeated type chars
        r"(?:V([9]+))?"                  # Optional decimal
        r"(?:\s+(COMP(?:-[1-5])?))?"     # Optional COMP clause
        , re.IGNORECASE
    )

    # COMP size thresholds (digits -> byte size -> Spark type)
    COMP_SIZES = {
        (1, 4): ShortType(),     # 1-4 digits -> 2 bytes
        (5, 9): IntegerType(),   # 5-9 digits -> 4 bytes
        (10, 18): LongType(),    # 10-18 digits -> 8 bytes
    }

    def __init__(self, default_encoding: str = "cp037"):
        """
        Initialize the VSAM type converter.

        Args:
            default_encoding: Default EBCDIC code page (cp037 for US)
        """
        self.default_encoding = default_encoding

    def parse_pic_clause(self, pic_clause: str) -> ParsedPicClause:
        """
        Parse a COBOL PIC clause into its components.

        Args:
            pic_clause: Raw PIC clause string (e.g., "PIC S9(7)V99 COMP-3")

        Returns:
            ParsedPicClause with extracted components

        Raises:
            ValueError: If the PIC clause cannot be parsed
        """
        pic_clause = pic_clause.strip().upper()

        # Try standard pattern first (PIC X(n))
        match = self.PIC_PATTERN.match(pic_clause)
        if match:
            sign, base_type, length, decimal_paren, decimal_inline, comp, occurs = match.groups()
            integer_digits = int(length)
            decimal_digits = int(decimal_paren or decimal_inline or 0)

        else:
            # Try inline pattern (PIC XXX or PIC 999)
            match = self.PIC_INLINE_PATTERN.match(pic_clause)
            if match:
                sign, type_chars, decimal_chars, comp = match.groups()
                base_type = type_chars[0]
                integer_digits = len(type_chars)
                decimal_digits = len(decimal_chars) if decimal_chars else 0
            else:
                raise ValueError(f"Unable to parse PIC clause: {pic_clause}")

        # Determine PIC type
        if base_type == "X":
            pic_type = CobolPicType.ALPHANUMERIC
        elif base_type == "A":
            pic_type = CobolPicType.ALPHABETIC
        elif sign:
            pic_type = CobolPicType.SIGNED_NUMERIC
        else:
            pic_type = CobolPicType.NUMERIC

        # Determine USAGE
        usage = CobolUsage.DISPLAY
        if comp:
            comp = comp.upper().replace(" ", "")
            usage_map = {
                "COMP": CobolUsage.COMP,
                "COMP-1": CobolUsage.COMP_1,
                "COMP-2": CobolUsage.COMP_2,
                "COMP-3": CobolUsage.COMP_3,
                "COMP-4": CobolUsage.COMP_4,
                "COMP-5": CobolUsage.COMP_5,
            }
            usage = usage_map.get(comp, CobolUsage.DISPLAY)

        # Calculate storage length
        total_digits = integer_digits + decimal_digits
        if usage == CobolUsage.DISPLAY:
            # Zoned decimal: 1 byte per digit
            total_length = total_digits
        elif usage == CobolUsage.COMP_3:
            # Packed decimal: (n+1)/2 bytes
            total_length = (total_digits + 1) // 2 + 1
        elif usage in (CobolUsage.COMP, CobolUsage.COMP_4, CobolUsage.COMP_5):
            # Binary: 2/4/8 bytes based on digit count
            if total_digits <= 4:
                total_length = 2
            elif total_digits <= 9:
                total_length = 4
            else:
                total_length = 8
        elif usage == CobolUsage.COMP_1:
            total_length = 4
        elif usage == CobolUsage.COMP_2:
            total_length = 8
        else:
            total_length = integer_digits

        return ParsedPicClause(
            pic_type=pic_type,
            integer_digits=integer_digits,
            decimal_digits=decimal_digits,
            usage=usage,
            total_length=total_length,
            is_signed=(sign is not None or pic_type == CobolPicType.SIGNED_NUMERIC),
        )

    def convert(self, pic_clause: str) -> DataType:
        """
        Convert a COBOL PIC clause to a Spark DataType.

        Args:
            pic_clause: COBOL PIC clause (e.g., "PIC S9(7)V99 COMP-3")

        Returns:
            Appropriate PySpark DataType

        Example:
            >>> converter.convert("PIC X(50)")
            StringType()
            >>> converter.convert("PIC S9(9)V99 COMP-3")
            DecimalType(11, 2)
        """
        try:
            parsed = self.parse_pic_clause(pic_clause)
        except ValueError:
            # Fallback to StringType for unparseable clauses
            return StringType()

        return self._map_to_spark_type(parsed)

    def _map_to_spark_type(self, parsed: ParsedPicClause) -> DataType:
        """
        Map a parsed PIC clause to a Spark DataType.

        Args:
            parsed: Parsed PIC clause object

        Returns:
            Appropriate PySpark DataType
        """
        # Alphanumeric types -> StringType
        if parsed.is_alphanumeric:
            return StringType()

        # Floating point types
        if parsed.usage == CobolUsage.COMP_1:
            return FloatType()
        if parsed.usage == CobolUsage.COMP_2:
            return DoubleType()

        # Packed decimal (COMP-3) or decimals -> DecimalType
        if parsed.is_packed or parsed.decimal_digits > 0:
            precision = parsed.total_digits
            scale = parsed.decimal_digits
            return DecimalType(precision, scale)

        # Binary types (COMP, COMP-4, COMP-5)
        if parsed.is_binary:
            return self._get_binary_type(parsed.total_digits)

        # Zoned decimal (DISPLAY) without decimals
        return self._get_integer_type(parsed.total_digits)

    def _get_binary_type(self, digits: int) -> DataType:
        """
        Get appropriate integer type for binary (COMP) fields.

        Args:
            digits: Number of digits in the field

        Returns:
            ShortType, IntegerType, or LongType
        """
        if digits <= 4:
            return ShortType()
        elif digits <= 9:
            return IntegerType()
        else:
            return LongType()

    def _get_integer_type(self, digits: int) -> DataType:
        """
        Get appropriate integer type for zoned decimal fields.

        Args:
            digits: Number of digits in the field

        Returns:
            ShortType, IntegerType, or LongType
        """
        if digits <= 4:
            return ShortType()
        elif digits <= 9:
            return IntegerType()
        else:
            return LongType()

    def get_storage_bytes(self, pic_clause: str) -> int:
        """
        Calculate the storage size in bytes for a PIC clause.

        Args:
            pic_clause: COBOL PIC clause

        Returns:
            Number of bytes for storage
        """
        parsed = self.parse_pic_clause(pic_clause)
        return parsed.total_length


# Convenience mapping dictionary for quick lookups
VSAM_TYPE_MAP = {
    # Alphanumeric
    "PIC X": StringType(),
    "PIC A": StringType(),

    # Zoned Decimal (Display)
    "PIC 9": LongType(),
    "PIC S9": LongType(),

    # Floating Point
    "COMP-1": FloatType(),
    "COMP-2": DoubleType(),

    # Packed Decimal and Binary require dynamic calculation
    # based on precision/scale
}
