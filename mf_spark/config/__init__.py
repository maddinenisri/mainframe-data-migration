"""
Configuration Module.

Provides configuration management for mainframe migrations.

Example:
    >>> from mf_spark.config import MigrationConfig
    >>> config = MigrationConfig.from_file("migration.yaml")
    >>> print(config.input_dir)
"""

from mf_spark.config.settings import MigrationConfig, DatasetDefinition

__all__ = [
    "MigrationConfig",
    "DatasetDefinition",
]
