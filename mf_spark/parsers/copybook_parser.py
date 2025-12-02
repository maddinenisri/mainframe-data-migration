"""
COBOL Copybook Parser.

Parses COBOL copybook files to extract field definitions for VSAM records.
Supports hierarchical record structures with level numbers.

Example:
    >>> parser = CopybookParser()
    >>> fields = parser.parse_file("CUSTREC.cpy")
    >>> for field in fields:
    ...     print(f"{field.name}: {field.pic_clause} ({field.length} bytes)")

Supported Features:
    - Level numbers (01-49, 66, 77, 88)
    - PIC clauses (X, A, 9, S9, V)
    - COMP, COMP-1, COMP-2, COMP-3, COMP-4, COMP-5
    - OCCURS clauses (arrays)
    - REDEFINES clauses
    - FILLER fields
"""

import re
from dataclasses import dataclass, field
from typing import Optional
from pathlib import Path

from mf_spark.converters.vsam_types import VSAMTypeConverter


@dataclass
class CopybookField:
    """
    Represents a field definition from a COBOL copybook.

    Attributes:
        name: Field name (e.g., "CUST-ID")
        level: COBOL level number (01-49, 66, 77, 88)
        pic_clause: Original PIC clause (e.g., "PIC 9(9)")
        length: Field length in bytes
        offset: Byte offset from start of record
        is_filler: Whether this is a FILLER field
        is_group: Whether this is a group item (no PIC)
        occurs: Number of occurrences for arrays
        redefines: Name of field this redefines
        parent: Parent field name for nested structures
        children: Child field names for group items
    """
    name: str
    level: int
    pic_clause: Optional[str] = None
    length: int = 0
    offset: int = 0
    is_filler: bool = False
    is_group: bool = False
    occurs: int = 1
    redefines: Optional[str] = None
    parent: Optional[str] = None
    children: list[str] = field(default_factory=list)

    @property
    def is_elementary(self) -> bool:
        """Check if this is an elementary item (has PIC clause)."""
        return self.pic_clause is not None and not self.is_group

    @property
    def total_length(self) -> int:
        """Total length including OCCURS."""
        return self.length * self.occurs

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "name": self.name,
            "level": self.level,
            "pic_clause": self.pic_clause,
            "length": self.length,
            "offset": self.offset,
            "is_filler": self.is_filler,
            "is_group": self.is_group,
            "occurs": self.occurs,
            "redefines": self.redefines,
            "parent": self.parent,
        }


class CopybookParser:
    """
    Parser for COBOL copybook files.

    This parser extracts field definitions from COBOL copybooks,
    calculating offsets and lengths for each field.

    Example:
        >>> parser = CopybookParser()
        >>> fields = parser.parse_file("CUSTREC.cpy")
        >>> schema = parser.to_spark_schema(fields)

    Attributes:
        type_converter: Converter for PIC clauses to Spark types
        ignore_fillers: Whether to exclude FILLER fields from output
    """

    # Regex patterns for parsing copybook lines
    LEVEL_PATTERN = re.compile(
        r"^\s*(\d{2})\s+"                    # Level number
        r"([\w-]+)"                           # Field name
        r"(.*)$",                             # Rest of line
        re.IGNORECASE
    )

    PIC_PATTERN = re.compile(
        r"PIC(?:TURE)?\s+"
        r"(S)?([XA9]+(?:\(\d+\))?)"           # Base type with optional length
        r"(?:V([9]+(?:\(\d+\))?))?",          # Optional decimal
        re.IGNORECASE
    )

    OCCURS_PATTERN = re.compile(
        r"OCCURS\s+(\d+)",
        re.IGNORECASE
    )

    REDEFINES_PATTERN = re.compile(
        r"REDEFINES\s+([\w-]+)",
        re.IGNORECASE
    )

    COMP_PATTERN = re.compile(
        r"(COMP(?:-[1-5])?)",
        re.IGNORECASE
    )

    def __init__(
        self,
        ignore_fillers: bool = False,
        default_encoding: str = "cp037",
    ):
        """
        Initialize the copybook parser.

        Args:
            ignore_fillers: Whether to exclude FILLER fields
            default_encoding: EBCDIC encoding for string fields
        """
        self.ignore_fillers = ignore_fillers
        self.default_encoding = default_encoding
        self.type_converter = VSAMTypeConverter(default_encoding=default_encoding)

    def parse_file(self, filepath: str) -> list[CopybookField]:
        """
        Parse a copybook file and extract field definitions.

        Args:
            filepath: Path to the copybook file

        Returns:
            List of CopybookField objects

        Raises:
            FileNotFoundError: If the copybook file doesn't exist
        """
        path = Path(filepath)
        if not path.exists():
            raise FileNotFoundError(f"Copybook not found: {filepath}")

        content = path.read_text(encoding="utf-8", errors="replace")
        return self.parse_content(content)

    def parse_content(self, content: str) -> list[CopybookField]:
        """
        Parse copybook content and extract field definitions.

        Args:
            content: Raw copybook content as string

        Returns:
            List of CopybookField objects
        """
        fields = []
        current_offset = 0
        parent_stack = []  # Stack of (level, name, offset) for hierarchy

        # Clean and normalize content
        lines = self._clean_content(content)

        for line in lines:
            field = self._parse_line(line)
            if field is None:
                continue

            # Handle hierarchy using level numbers
            while parent_stack and parent_stack[-1][0] >= field.level:
                parent_stack.pop()

            if parent_stack:
                field.parent = parent_stack[-1][1]
                # Don't reset offset for group items
                if field.is_group:
                    field.offset = current_offset
            else:
                field.offset = current_offset

            # Calculate offset for elementary items
            if field.is_elementary:
                field.offset = current_offset
                field.length = self._calculate_length(field.pic_clause)
                current_offset += field.total_length

            # Handle REDEFINES (overlay on previous field)
            if field.redefines:
                # Find the redefined field and use its offset
                for f in reversed(fields):
                    if f.name == field.redefines:
                        field.offset = f.offset
                        break
                # Don't advance offset for REDEFINES

            # Add to parent's children if applicable
            if parent_stack:
                for f in reversed(fields):
                    if f.name == parent_stack[-1][1]:
                        f.children.append(field.name)
                        break

            # Push group items onto parent stack
            if field.is_group:
                parent_stack.append((field.level, field.name, field.offset))

            # Apply filter and add to results
            if not (self.ignore_fillers and field.is_filler):
                fields.append(field)

        return fields

    def _clean_content(self, content: str) -> list[str]:
        """
        Clean and normalize copybook content.

        Removes comments, handles line continuations, and normalizes whitespace.
        """
        lines = []
        current_line = ""

        for line in content.split("\n"):
            # Skip empty lines
            if not line.strip():
                continue

            # Handle sequence numbers (columns 1-6) and indicator (column 7)
            if len(line) >= 7:
                indicator = line[6] if len(line) > 6 else " "
                # Skip comment lines (indicator = *)
                if indicator == "*":
                    continue
                # Handle continuation lines (indicator = -)
                if indicator == "-":
                    current_line = current_line.rstrip() + line[7:].lstrip()
                    continue
                # Take content from columns 8-72
                line_content = line[7:72] if len(line) > 7 else ""
            else:
                line_content = line

            # If we have a pending continuation, complete it
            if current_line:
                lines.append(current_line)
                current_line = ""

            # Remove inline comments (text after *)
            if "*" in line_content:
                line_content = line_content.split("*")[0]

            line_content = line_content.strip()
            if line_content:
                # Handle multi-line statements (ends with period)
                if line_content.endswith("."):
                    lines.append(line_content)
                else:
                    current_line = line_content

        if current_line:
            lines.append(current_line)

        return lines

    def _parse_line(self, line: str) -> Optional[CopybookField]:
        """
        Parse a single copybook line into a field definition.

        Args:
            line: Cleaned copybook line

        Returns:
            CopybookField or None if line is not a field definition
        """
        # Remove trailing period
        line = line.rstrip(".")

        # Match level number and field name
        match = self.LEVEL_PATTERN.match(line)
        if not match:
            return None

        level = int(match.group(1))
        name = match.group(2).upper()
        rest = match.group(3)

        # Skip level 66 (RENAMES) and level 88 (condition names)
        if level in (66, 88):
            return None

        # Check for FILLER
        is_filler = name == "FILLER"

        # Check for REDEFINES
        redefines = None
        redefines_match = self.REDEFINES_PATTERN.search(rest)
        if redefines_match:
            redefines = redefines_match.group(1).upper()

        # Check for OCCURS
        occurs = 1
        occurs_match = self.OCCURS_PATTERN.search(rest)
        if occurs_match:
            occurs = int(occurs_match.group(1))

        # Check for PIC clause
        pic_clause = None
        pic_match = self.PIC_PATTERN.search(rest)
        if pic_match:
            sign = pic_match.group(1) or ""
            integer_part = pic_match.group(2)
            decimal_part = pic_match.group(3) or ""

            # Reconstruct PIC clause
            pic_clause = f"PIC {sign}{integer_part}"
            if decimal_part:
                pic_clause += f"V{decimal_part}"

            # Check for COMP modifier
            comp_match = self.COMP_PATTERN.search(rest)
            if comp_match:
                pic_clause += f" {comp_match.group(1).upper()}"

        # Determine if this is a group item (no PIC clause)
        is_group = pic_clause is None and not is_filler

        return CopybookField(
            name=name,
            level=level,
            pic_clause=pic_clause,
            is_filler=is_filler,
            is_group=is_group,
            occurs=occurs,
            redefines=redefines,
        )

    def _calculate_length(self, pic_clause: Optional[str]) -> int:
        """
        Calculate the storage length for a PIC clause.

        Args:
            pic_clause: COBOL PIC clause

        Returns:
            Length in bytes
        """
        if not pic_clause:
            return 0

        return self.type_converter.get_storage_bytes(pic_clause)

    def get_elementary_fields(self, fields: list[CopybookField]) -> list[CopybookField]:
        """
        Get only elementary (non-group) fields.

        Args:
            fields: List of all fields

        Returns:
            List of elementary fields only
        """
        return [f for f in fields if f.is_elementary and not f.is_filler]

    def get_record_length(self, fields: list[CopybookField]) -> int:
        """
        Calculate total record length from field definitions.

        Args:
            fields: List of fields

        Returns:
            Total record length in bytes
        """
        max_offset = 0
        for f in fields:
            if f.is_elementary:
                end_offset = f.offset + f.total_length
                max_offset = max(max_offset, end_offset)
        return max_offset

    def to_spark_schema(self, fields: list[CopybookField]):
        """
        Convert field definitions to a Spark StructType schema.

        Args:
            fields: List of CopybookField objects

        Returns:
            PySpark StructType schema
        """
        from pyspark.sql.types import StructType, StructField

        schema_fields = []
        for f in self.get_elementary_fields(fields):
            spark_type = self.type_converter.convert(f.pic_clause)
            schema_fields.append(StructField(f.name, spark_type, nullable=True))

        return StructType(schema_fields)

    def print_layout(self, fields: list[CopybookField]) -> str:
        """
        Generate a formatted record layout for documentation.

        Args:
            fields: List of CopybookField objects

        Returns:
            Formatted string showing record layout
        """
        lines = []
        lines.append("=" * 80)
        lines.append("RECORD LAYOUT")
        lines.append("=" * 80)
        lines.append(f"{'Offset':<8} {'Length':<8} {'Level':<6} {'Name':<30} {'PIC Clause'}")
        lines.append("-" * 80)

        for f in fields:
            if f.is_filler and self.ignore_fillers:
                continue
            indent = "  " * (f.level // 5)
            name = indent + f.name
            pic = f.pic_clause or "(GROUP)"
            lines.append(f"{f.offset:<8} {f.length:<8} {f.level:<6} {name:<30} {pic}")

        lines.append("-" * 80)
        lines.append(f"Total Record Length: {self.get_record_length(fields)} bytes")
        lines.append("=" * 80)

        return "\n".join(lines)
