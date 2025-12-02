"""
Mainframe Spark Migration Framework.

A modular, reusable PySpark framework for migrating mainframe data (VSAM/DB2)
to modern cloud-native formats (JSON, Parquet) with full type conversion support.

Architecture:
    mf_spark/
    ├── core/           # Core Spark session and base classes
    ├── converters/     # Data type converters (VSAM, DB2 to Spark)
    ├── parsers/        # File parsers (Copybook, DDL, DCL)
    ├── validators/     # Data validation and quality checks
    ├── utils/          # Utility functions (encoding, logging)
    └── config/         # Configuration management

Usage:
    from mf_spark import MainframeMigrator

    migrator = MainframeMigrator()
    migrator.convert_vsam("input/data.ps", "input/copybook.cpy", "output/")
    migrator.convert_db2("SCHEMA.TABLE", "output/")
"""

__version__ = "1.0.0"
__author__ = "Mainframe Migration Team"

from mf_spark.core.session import SparkSessionManager
from mf_spark.core.migrator import MainframeMigrator
from mf_spark.config.settings import MigrationConfig

__all__ = [
    "SparkSessionManager",
    "MainframeMigrator",
    "MigrationConfig",
    "__version__",
]
