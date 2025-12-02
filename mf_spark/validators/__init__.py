"""
Validators Module.

Provides data validation and quality check functions for mainframe migrations:
    - DataValidator: Validate data against schema and business rules
    - SchemaValidator: Validate schema compatibility
    - RecordValidator: Record-level validation with CRC checks

Example:
    >>> from mf_spark.validators import DataValidator
    >>> validator = DataValidator()
    >>> result = validator.validate(df, expected_count=1000)
    >>> if result.is_valid:
    ...     print("Validation passed!")
"""

from mf_spark.validators.data_validator import DataValidator, ValidationResult
from mf_spark.validators.schema_validator import SchemaValidator

__all__ = [
    "DataValidator",
    "ValidationResult",
    "SchemaValidator",
]
