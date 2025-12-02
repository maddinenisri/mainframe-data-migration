"""
Schema Validator.

Provides schema validation and compatibility checking for data migrations.

Example:
    >>> validator = SchemaValidator()
    >>> result = validator.compare_schemas(source_schema, target_schema)
    >>> if result.is_compatible:
    ...     print("Schemas are compatible")
"""

from dataclasses import dataclass, field
from typing import Optional, Any
from pyspark.sql.types import (
    StructType, StructField, DataType,
    StringType, IntegerType, LongType, ShortType,
    FloatType, DoubleType, DecimalType,
    DateType, TimestampType, BinaryType, BooleanType,
)


@dataclass
class SchemaComparisonResult:
    """
    Result of a schema comparison operation.

    Attributes:
        is_compatible: Whether schemas are compatible for migration
        source_fields: List of source field names
        target_fields: List of target field names
        matching_fields: Fields present in both schemas
        missing_in_target: Fields in source but not in target
        missing_in_source: Fields in target but not in source
        type_mismatches: Fields with incompatible types
        compatible_changes: Fields with compatible type changes
    """
    is_compatible: bool = True
    source_fields: list[str] = field(default_factory=list)
    target_fields: list[str] = field(default_factory=list)
    matching_fields: list[str] = field(default_factory=list)
    missing_in_target: list[str] = field(default_factory=list)
    missing_in_source: list[str] = field(default_factory=list)
    type_mismatches: dict[str, tuple[str, str]] = field(default_factory=dict)
    compatible_changes: dict[str, tuple[str, str]] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "is_compatible": self.is_compatible,
            "source_fields": self.source_fields,
            "target_fields": self.target_fields,
            "matching_fields": self.matching_fields,
            "missing_in_target": self.missing_in_target,
            "missing_in_source": self.missing_in_source,
            "type_mismatches": {
                k: {"source": v[0], "target": v[1]}
                for k, v in self.type_mismatches.items()
            },
            "compatible_changes": {
                k: {"source": v[0], "target": v[1]}
                for k, v in self.compatible_changes.items()
            },
        }

    def summary(self) -> str:
        """Generate a human-readable summary."""
        lines = []
        lines.append("=" * 60)
        lines.append("SCHEMA COMPARISON SUMMARY")
        lines.append("=" * 60)
        lines.append(f"Status: {'COMPATIBLE' if self.is_compatible else 'INCOMPATIBLE'}")
        lines.append(f"Source Fields: {len(self.source_fields)}")
        lines.append(f"Target Fields: {len(self.target_fields)}")
        lines.append(f"Matching: {len(self.matching_fields)}")

        if self.missing_in_target:
            lines.append(f"\nMissing in Target: {len(self.missing_in_target)}")
            for f in self.missing_in_target:
                lines.append(f"  - {f}")

        if self.missing_in_source:
            lines.append(f"\nMissing in Source: {len(self.missing_in_source)}")
            for f in self.missing_in_source:
                lines.append(f"  - {f}")

        if self.type_mismatches:
            lines.append(f"\nType Mismatches: {len(self.type_mismatches)}")
            for f, (src, tgt) in self.type_mismatches.items():
                lines.append(f"  - {f}: {src} -> {tgt}")

        if self.compatible_changes:
            lines.append(f"\nCompatible Type Changes: {len(self.compatible_changes)}")
            for f, (src, tgt) in self.compatible_changes.items():
                lines.append(f"  - {f}: {src} -> {tgt}")

        lines.append("=" * 60)
        return "\n".join(lines)


class SchemaValidator:
    """
    Validator for schema compatibility.

    Compares source and target schemas to determine compatibility
    for data migration. Handles type promotions and compatible changes.

    Example:
        >>> validator = SchemaValidator()
        >>> result = validator.compare_schemas(source, target)
        >>> print(result.summary())
    """

    # Type compatibility matrix
    # source_type -> list of compatible target types
    COMPATIBLE_TYPES = {
        ShortType: [ShortType, IntegerType, LongType, DecimalType, FloatType, DoubleType, StringType],
        IntegerType: [IntegerType, LongType, DecimalType, FloatType, DoubleType, StringType],
        LongType: [LongType, DecimalType, DoubleType, StringType],
        FloatType: [FloatType, DoubleType, StringType],
        DoubleType: [DoubleType, StringType],
        DecimalType: [DecimalType, DoubleType, StringType],
        StringType: [StringType],
        DateType: [DateType, TimestampType, StringType],
        TimestampType: [TimestampType, StringType],
        BinaryType: [BinaryType, StringType],
        BooleanType: [BooleanType, StringType, IntegerType],
    }

    def __init__(
        self,
        allow_missing_in_target: bool = False,
        allow_type_promotion: bool = True,
        strict_mode: bool = False,
    ):
        """
        Initialize the schema validator.

        Args:
            allow_missing_in_target: Allow source fields missing in target
            allow_type_promotion: Allow widening type conversions
            strict_mode: Require exact type matches
        """
        self.allow_missing_in_target = allow_missing_in_target
        self.allow_type_promotion = allow_type_promotion
        self.strict_mode = strict_mode

    def compare_schemas(
        self,
        source: StructType,
        target: StructType,
    ) -> SchemaComparisonResult:
        """
        Compare source and target schemas.

        Args:
            source: Source StructType schema
            target: Target StructType schema

        Returns:
            SchemaComparisonResult with comparison details
        """
        result = SchemaComparisonResult()

        # Get field names
        result.source_fields = [f.name for f in source.fields]
        result.target_fields = [f.name for f in target.fields]

        source_dict = {f.name: f for f in source.fields}
        target_dict = {f.name: f for f in target.fields}

        # Find matching, missing, and extra fields
        source_set = set(result.source_fields)
        target_set = set(result.target_fields)

        result.matching_fields = list(source_set & target_set)
        result.missing_in_target = list(source_set - target_set)
        result.missing_in_source = list(target_set - source_set)

        # Check compatibility of matching fields
        for field_name in result.matching_fields:
            source_field = source_dict[field_name]
            target_field = target_dict[field_name]

            is_compatible, is_exact = self._check_type_compatibility(
                source_field.dataType,
                target_field.dataType,
            )

            if not is_compatible:
                result.type_mismatches[field_name] = (
                    str(source_field.dataType),
                    str(target_field.dataType),
                )
            elif not is_exact:
                result.compatible_changes[field_name] = (
                    str(source_field.dataType),
                    str(target_field.dataType),
                )

        # Determine overall compatibility
        result.is_compatible = len(result.type_mismatches) == 0
        if not self.allow_missing_in_target and result.missing_in_target:
            result.is_compatible = False
        if self.strict_mode and result.compatible_changes:
            result.is_compatible = False

        return result

    def _check_type_compatibility(
        self,
        source_type: DataType,
        target_type: DataType,
    ) -> tuple[bool, bool]:
        """
        Check if source type is compatible with target type.

        Args:
            source_type: Source DataType
            target_type: Target DataType

        Returns:
            Tuple of (is_compatible, is_exact_match)
        """
        # Exact match
        if source_type == target_type:
            return True, True

        # Handle DecimalType separately (needs precision/scale check)
        if isinstance(source_type, DecimalType) and isinstance(target_type, DecimalType):
            # Target must have >= precision and >= scale
            source_precision = source_type.precision
            source_scale = source_type.scale
            target_precision = target_type.precision
            target_scale = target_type.scale

            is_compatible = (
                target_precision >= source_precision and
                target_scale >= source_scale
            )
            is_exact = (
                target_precision == source_precision and
                target_scale == source_scale
            )
            return is_compatible, is_exact

        # Check type promotion compatibility
        source_base = type(source_type)
        target_base = type(target_type)

        compatible_targets = self.COMPATIBLE_TYPES.get(source_base, [])

        if target_base in compatible_targets:
            if self.allow_type_promotion:
                return True, False
            else:
                return source_base == target_base, source_base == target_base

        return False, False

    def validate_schema(
        self,
        schema: StructType,
        required_fields: Optional[list[str]] = None,
        field_types: Optional[dict[str, type]] = None,
    ) -> tuple[bool, list[str]]:
        """
        Validate a schema against requirements.

        Args:
            schema: StructType schema to validate
            required_fields: List of required field names
            field_types: Dict of field name -> expected DataType class

        Returns:
            Tuple of (is_valid, error_messages)
        """
        errors = []
        field_dict = {f.name: f for f in schema.fields}

        # Check required fields
        if required_fields:
            for field_name in required_fields:
                if field_name not in field_dict:
                    errors.append(f"Required field missing: {field_name}")

        # Check field types
        if field_types:
            for field_name, expected_type in field_types.items():
                if field_name in field_dict:
                    actual_type = type(field_dict[field_name].dataType)
                    if actual_type != expected_type:
                        errors.append(
                            f"Type mismatch for {field_name}: "
                            f"expected {expected_type.__name__}, "
                            f"got {actual_type.__name__}"
                        )

        return len(errors) == 0, errors

    def infer_target_schema(
        self,
        source: StructType,
        type_mappings: Optional[dict[str, DataType]] = None,
        column_mappings: Optional[dict[str, str]] = None,
    ) -> StructType:
        """
        Infer target schema from source with optional transformations.

        Args:
            source: Source StructType schema
            type_mappings: Dict of field name -> new DataType
            column_mappings: Dict of source name -> target name

        Returns:
            New StructType for target schema
        """
        type_mappings = type_mappings or {}
        column_mappings = column_mappings or {}

        fields = []
        for source_field in source.fields:
            # Apply column name mapping
            target_name = column_mappings.get(source_field.name, source_field.name)

            # Apply type mapping
            target_type = type_mappings.get(source_field.name, source_field.dataType)

            fields.append(StructField(
                target_name,
                target_type,
                source_field.nullable,
            ))

        return StructType(fields)
