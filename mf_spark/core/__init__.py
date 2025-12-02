"""
Core module for Spark session management and base migration classes.

This module provides:
    - SparkSessionManager: Centralized Spark session configuration
    - BaseMigrator: Abstract base class for all migrators
    - DatasetResult: Standard result container for migrations
"""

from mf_spark.core.session import SparkSessionManager
from mf_spark.core.migrator import MainframeMigrator
from mf_spark.core.base import BaseMigrator, DatasetResult

__all__ = [
    "SparkSessionManager",
    "MainframeMigrator",
    "BaseMigrator",
    "DatasetResult",
]
