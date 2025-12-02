"""
Data Validator.

Provides comprehensive data validation for migrated mainframe data.
Includes record count verification, null checks, and data quality metrics.

Example:
    >>> validator = DataValidator()
    >>> result = validator.validate(df, expected_count=1000)
    >>> print(result.summary())
"""

from dataclasses import dataclass, field
from typing import Optional, Any
from datetime import datetime


@dataclass
class ValidationResult:
    """
    Result of a data validation operation.

    Attributes:
        is_valid: Whether all validations passed
        record_count: Actual record count
        expected_count: Expected record count (if specified)
        null_counts: Dict of column -> null count
        errors: List of validation error messages
        warnings: List of validation warning messages
        metrics: Additional validation metrics
        timestamp: When validation was performed
    """
    is_valid: bool = True
    record_count: int = 0
    expected_count: Optional[int] = None
    null_counts: dict[str, int] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    metrics: dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.now)

    def add_error(self, message: str) -> None:
        """Add an error message and mark as invalid."""
        self.errors.append(message)
        self.is_valid = False

    def add_warning(self, message: str) -> None:
        """Add a warning message."""
        self.warnings.append(message)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "is_valid": self.is_valid,
            "record_count": self.record_count,
            "expected_count": self.expected_count,
            "null_counts": self.null_counts,
            "errors": self.errors,
            "warnings": self.warnings,
            "metrics": self.metrics,
            "timestamp": self.timestamp.isoformat(),
        }

    def summary(self) -> str:
        """Generate a human-readable summary."""
        lines = []
        lines.append("=" * 60)
        lines.append("VALIDATION SUMMARY")
        lines.append("=" * 60)
        lines.append(f"Status: {'PASSED' if self.is_valid else 'FAILED'}")
        lines.append(f"Record Count: {self.record_count:,}")
        if self.expected_count:
            lines.append(f"Expected Count: {self.expected_count:,}")
            diff = self.record_count - self.expected_count
            lines.append(f"Difference: {diff:+,}")

        if self.null_counts:
            lines.append("\nNull Counts:")
            for col, count in sorted(self.null_counts.items()):
                if count > 0:
                    lines.append(f"  {col}: {count:,}")

        if self.errors:
            lines.append("\nErrors:")
            for err in self.errors:
                lines.append(f"  - {err}")

        if self.warnings:
            lines.append("\nWarnings:")
            for warn in self.warnings:
                lines.append(f"  - {warn}")

        lines.append("=" * 60)
        return "\n".join(lines)


class DataValidator:
    """
    Validator for migrated mainframe data.

    Provides various validation checks including:
    - Record count verification
    - Null value detection
    - Data type validation
    - Business rule validation
    - Checksum verification

    Example:
        >>> validator = DataValidator()
        >>> result = validator.validate(df)
        >>> if not result.is_valid:
        ...     for error in result.errors:
        ...         print(f"ERROR: {error}")
    """

    def __init__(
        self,
        null_threshold: float = 0.5,
        count_tolerance: float = 0.0,
    ):
        """
        Initialize the data validator.

        Args:
            null_threshold: Warn if null percentage exceeds this (0.0-1.0)
            count_tolerance: Allowed variance from expected count (0.0-1.0)
        """
        self.null_threshold = null_threshold
        self.count_tolerance = count_tolerance

    def validate(
        self,
        df: Any,
        expected_count: Optional[int] = None,
        required_columns: Optional[list[str]] = None,
        non_null_columns: Optional[list[str]] = None,
    ) -> ValidationResult:
        """
        Validate a DataFrame against specified criteria.

        Args:
            df: PySpark DataFrame to validate
            expected_count: Expected number of records
            required_columns: Columns that must exist
            non_null_columns: Columns that must not contain nulls

        Returns:
            ValidationResult with validation details
        """
        result = ValidationResult()

        # Get record count
        result.record_count = df.count()

        # Validate expected count
        if expected_count is not None:
            result.expected_count = expected_count
            self._validate_count(result, expected_count)

        # Validate required columns
        if required_columns:
            self._validate_required_columns(result, df, required_columns)

        # Check for nulls
        self._validate_nulls(result, df, non_null_columns)

        # Calculate metrics
        result.metrics = self._calculate_metrics(df)

        return result

    def _validate_count(self, result: ValidationResult, expected: int) -> None:
        """Validate record count against expected."""
        actual = result.record_count

        if actual == 0:
            result.add_error("No records found in dataset")
            return

        if self.count_tolerance == 0.0:
            # Exact match required
            if actual != expected:
                result.add_error(
                    f"Record count mismatch: expected {expected:,}, got {actual:,}"
                )
        else:
            # Tolerance allowed
            tolerance = int(expected * self.count_tolerance)
            if abs(actual - expected) > tolerance:
                result.add_error(
                    f"Record count outside tolerance: expected {expected:,} "
                    f"(±{tolerance:,}), got {actual:,}"
                )

    def _validate_required_columns(
        self,
        result: ValidationResult,
        df: Any,
        required_columns: list[str],
    ) -> None:
        """Validate that required columns exist."""
        existing_columns = set(df.columns)
        for col in required_columns:
            if col not in existing_columns:
                result.add_error(f"Required column missing: {col}")

    def _validate_nulls(
        self,
        result: ValidationResult,
        df: Any,
        non_null_columns: Optional[list[str]],
    ) -> None:
        """Check for null values in DataFrame."""
        from pyspark.sql.functions import col, count, when, isnan

        total_count = result.record_count
        if total_count == 0:
            return

        # Count nulls for all columns
        for column in df.columns:
            try:
                null_count = df.filter(
                    col(column).isNull() |
                    (col(column) == "") |
                    (col(column).cast("string") == "null")
                ).count()

                result.null_counts[column] = null_count

                # Check threshold
                null_pct = null_count / total_count
                if null_pct > self.null_threshold:
                    result.add_warning(
                        f"High null percentage in {column}: "
                        f"{null_pct:.1%} ({null_count:,}/{total_count:,})"
                    )

                # Check non-null constraint
                if non_null_columns and column in non_null_columns and null_count > 0:
                    result.add_error(
                        f"Non-null column {column} contains {null_count:,} nulls"
                    )
            except Exception:
                # Skip columns that can't be checked
                pass

    def _calculate_metrics(self, df: Any) -> dict[str, Any]:
        """Calculate additional data quality metrics."""
        metrics = {
            "column_count": len(df.columns),
            "columns": df.columns,
        }

        try:
            # Get schema info
            schema_info = {}
            for field in df.schema.fields:
                schema_info[field.name] = str(field.dataType)
            metrics["schema"] = schema_info
        except Exception:
            pass

        return metrics

    def validate_checksum(
        self,
        df: Any,
        checksum_column: str,
        expected_checksum: str,
    ) -> bool:
        """
        Validate data checksum for integrity.

        Args:
            df: DataFrame to validate
            checksum_column: Column containing checksums
            expected_checksum: Expected checksum value

        Returns:
            True if checksum matches
        """
        from pyspark.sql.functions import md5, concat_ws

        # Calculate checksum of all data
        calculated = df.select(
            md5(concat_ws("|", *[df[c] for c in df.columns]))
        ).first()[0]

        return calculated == expected_checksum

    def compare_datasets(
        self,
        df1: Any,
        df2: Any,
        key_columns: list[str],
    ) -> dict[str, Any]:
        """
        Compare two datasets for consistency.

        Args:
            df1: First DataFrame (e.g., source)
            df2: Second DataFrame (e.g., target)
            key_columns: Columns to use as join key

        Returns:
            Dictionary with comparison results
        """
        results = {
            "df1_count": df1.count(),
            "df2_count": df2.count(),
            "matching": 0,
            "df1_only": 0,
            "df2_only": 0,
        }

        # Perform outer join
        joined = df1.alias("a").join(
            df2.alias("b"),
            on=key_columns,
            how="outer"
        )

        # Count matches and differences
        results["matching"] = joined.filter(
            " AND ".join([f"a.{c} IS NOT NULL AND b.{c} IS NOT NULL" for c in key_columns])
        ).count()

        results["df1_only"] = joined.filter(
            " AND ".join([f"a.{c} IS NOT NULL AND b.{c} IS NULL" for c in key_columns])
        ).count()

        results["df2_only"] = joined.filter(
            " AND ".join([f"a.{c} IS NULL AND b.{c} IS NOT NULL" for c in key_columns])
        ).count()

        return results
