"""
Unified Type Mapper.

Provides a single interface for type conversion across different
mainframe source systems (VSAM, DB2).

Example:
    >>> mapper = TypeMapper()
    >>> mapper.from_vsam("PIC S9(7)V99 COMP-3")
    DecimalType(9, 2)
    >>> mapper.from_db2("DECIMAL(15,2)")
    DecimalType(15, 2)
"""

from typing import Optional
from pyspark.sql.types import DataType, StructType, StructField, StringType

from mf_spark.converters.vsam_types import VSAMTypeConverter
from mf_spark.converters.db2_types import DB2TypeConverter
from mf_spark.core.base import SourceType


class TypeMapper:
    """
    Unified type mapper for mainframe data sources.

    This class provides a single interface for converting data types
    from different mainframe sources to Spark types.

    Attributes:
        vsam_converter: Converter for VSAM/COBOL types
        db2_converter: Converter for DB2 types

    Example:
        >>> mapper = TypeMapper()
        >>> mapper.convert("PIC X(50)", SourceType.VSAM)
        StringType()
        >>> mapper.convert("VARCHAR(100)", SourceType.DB2)
        StringType()
    """

    def __init__(
        self,
        ebcdic_encoding: str = "cp037",
        db2_ccsid: int = 37,
    ):
        """
        Initialize the type mapper.

        Args:
            ebcdic_encoding: Default EBCDIC encoding for VSAM
            db2_ccsid: Default CCSID for DB2
        """
        self.vsam_converter = VSAMTypeConverter(default_encoding=ebcdic_encoding)
        self.db2_converter = DB2TypeConverter(default_ccsid=db2_ccsid)

    def convert(self, type_def: str, source_type: SourceType) -> DataType:
        """
        Convert a type definition to Spark DataType.

        Args:
            type_def: Type definition string
            source_type: Source system type (VSAM, DB2)

        Returns:
            Appropriate PySpark DataType

        Raises:
            ValueError: If source type is not supported
        """
        if source_type == SourceType.VSAM:
            return self.from_vsam(type_def)
        elif source_type == SourceType.DB2:
            return self.from_db2(type_def)
        else:
            raise ValueError(f"Unsupported source type: {source_type}")

    def from_vsam(self, pic_clause: str) -> DataType:
        """
        Convert a COBOL PIC clause to Spark DataType.

        Args:
            pic_clause: COBOL PIC clause (e.g., "PIC S9(7)V99 COMP-3")

        Returns:
            PySpark DataType
        """
        return self.vsam_converter.convert(pic_clause)

    def from_db2(self, db2_type: str) -> DataType:
        """
        Convert a DB2 type to Spark DataType.

        Args:
            db2_type: DB2 type definition (e.g., "DECIMAL(15,2)")

        Returns:
            PySpark DataType
        """
        return self.db2_converter.convert(db2_type)

    def build_schema_from_vsam(
        self,
        field_definitions: list[tuple[str, str]],
    ) -> StructType:
        """
        Build a Spark StructType from VSAM field definitions.

        Args:
            field_definitions: List of (field_name, pic_clause) tuples

        Returns:
            PySpark StructType schema

        Example:
            >>> fields = [
            ...     ("CUST_ID", "PIC 9(9)"),
            ...     ("CUST_NAME", "PIC X(50)"),
            ...     ("BALANCE", "PIC S9(9)V99 COMP-3"),
            ... ]
            >>> schema = mapper.build_schema_from_vsam(fields)
        """
        fields = []
        for name, pic_clause in field_definitions:
            spark_type = self.from_vsam(pic_clause)
            fields.append(StructField(name, spark_type, nullable=True))
        return StructType(fields)

    def build_schema_from_db2(
        self,
        column_definitions: list[tuple[str, str, bool]],
    ) -> StructType:
        """
        Build a Spark StructType from DB2 column definitions.

        Args:
            column_definitions: List of (column_name, db2_type, nullable) tuples

        Returns:
            PySpark StructType schema

        Example:
            >>> columns = [
            ...     ("CUST_ID", "INTEGER", False),
            ...     ("CUST_NAME", "VARCHAR(100)", True),
            ...     ("BALANCE", "DECIMAL(15,2)", True),
            ... ]
            >>> schema = mapper.build_schema_from_db2(columns)
        """
        fields = []
        for name, db2_type, nullable in column_definitions:
            spark_type = self.from_db2(db2_type)
            fields.append(StructField(name, spark_type, nullable=nullable))
        return StructType(fields)

    def get_compatible_types(
        self,
        vsam_type: str,
        db2_type: str,
    ) -> tuple[DataType, DataType, bool]:
        """
        Check if VSAM and DB2 types are compatible.

        This is useful when migrating both VSAM and DB2 data to a
        unified target schema.

        Args:
            vsam_type: COBOL PIC clause
            db2_type: DB2 type definition

        Returns:
            Tuple of (vsam_spark_type, db2_spark_type, is_compatible)
        """
        vsam_spark = self.from_vsam(vsam_type)
        db2_spark = self.from_db2(db2_type)

        # Types are compatible if they're the same or both numeric/string
        is_compatible = (
            vsam_spark == db2_spark or
            (self._is_numeric(vsam_spark) and self._is_numeric(db2_spark)) or
            (self._is_string(vsam_spark) and self._is_string(db2_spark))
        )

        return vsam_spark, db2_spark, is_compatible

    def _is_numeric(self, data_type: DataType) -> bool:
        """Check if a data type is numeric."""
        from pyspark.sql.types import (
            ShortType, IntegerType, LongType, FloatType, DoubleType, DecimalType
        )
        return isinstance(data_type, (ShortType, IntegerType, LongType,
                                       FloatType, DoubleType, DecimalType))

    def _is_string(self, data_type: DataType) -> bool:
        """Check if a data type is string-like."""
        return isinstance(data_type, StringType)


# Type comparison table for documentation
TYPE_COMPARISON = """
VSAM/DB2 Type Equivalents
=========================

| Data Category    | DB2 z/OS       | VSAM/COBOL                | Spark         |
|------------------|----------------|---------------------------|---------------|
| Small Integer    | SMALLINT       | PIC S9(4) COMP            | ShortType     |
| Integer          | INTEGER        | PIC S9(9) COMP            | IntegerType   |
| Big Integer      | BIGINT         | PIC S9(18) COMP           | LongType      |
| Packed Decimal   | DECIMAL(p,s)   | PIC S9(p-s)V9(s) COMP-3   | DecimalType   |
| Float            | REAL           | COMP-1                    | FloatType     |
| Double           | DOUBLE         | COMP-2                    | DoubleType    |
| Fixed String     | CHAR(n)        | PIC X(n)                  | StringType    |
| Variable String  | VARCHAR(n)     | N/A                       | StringType    |
| Date             | DATE           | PIC 9(8)                  | DateType      |
| Timestamp        | TIMESTAMP      | PIC X(26)                 | TimestampType |
| Binary           | BLOB           | PIC X(n) binary           | BinaryType    |
"""
