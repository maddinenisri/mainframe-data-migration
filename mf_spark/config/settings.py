"""
Configuration Settings.

Provides configuration classes for mainframe migration jobs.

Example:
    >>> config = MigrationConfig(
    ...     input_dir="input",
    ...     output_dir="output",
    ...     output_format="json",
    ... )
"""

from dataclasses import dataclass, field
from typing import Optional, Any
from pathlib import Path
import json
import os


@dataclass
class DatasetDefinition:
    """
    Definition of a dataset to migrate.

    Attributes:
        name: Logical name for the dataset
        data_file: Path to the data file
        copybook: Path to the copybook (for VSAM)
        source_table: DB2 table name (for DB2)
        record_format: Record format (F, V, VB)
        record_length: Expected record length
        description: Human-readable description
        primary_key: List of primary key columns
        enabled: Whether to include in migration
    """
    name: str
    data_file: Optional[str] = None
    copybook: Optional[str] = None
    source_table: Optional[str] = None
    record_format: str = "F"
    record_length: Optional[int] = None
    description: str = ""
    primary_key: list[str] = field(default_factory=list)
    enabled: bool = True

    @classmethod
    def from_dict(cls, data: dict) -> "DatasetDefinition":
        """Create from dictionary."""
        return cls(
            name=data["name"],
            data_file=data.get("data_file"),
            copybook=data.get("copybook"),
            source_table=data.get("source_table"),
            record_format=data.get("record_format", "F"),
            record_length=data.get("record_length"),
            description=data.get("description", ""),
            primary_key=data.get("primary_key", []),
            enabled=data.get("enabled", True),
        )

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "name": self.name,
            "data_file": self.data_file,
            "copybook": self.copybook,
            "source_table": self.source_table,
            "record_format": self.record_format,
            "record_length": self.record_length,
            "description": self.description,
            "primary_key": self.primary_key,
            "enabled": self.enabled,
        }


@dataclass
class MigrationConfig:
    """
    Configuration for a mainframe migration job.

    This class holds all configuration settings for running a migration,
    including paths, formats, and processing options.

    Attributes:
        input_dir: Directory containing input files
        output_dir: Directory for output files
        output_format: Output format (json, parquet, csv)
        cobrix_version: Version of Cobrix to use
        ebcdic_encoding: EBCDIC code page
        spark_app_name: Name for Spark application
        spark_master: Spark master URL
        datasets: List of dataset definitions
        clean_output: Whether to clean output before running
        validate_output: Whether to validate after conversion
        generate_manifest: Whether to generate manifest file

    Example:
        >>> config = MigrationConfig(
        ...     input_dir="input",
        ...     output_dir="output",
        ...     datasets=[
        ...         DatasetDefinition(
        ...             name="customers",
        ...             data_file="CUSTDATA.PS",
        ...             copybook="CUSTREC.cpy",
        ...         ),
        ...     ],
        ... )
    """
    input_dir: str = "input"
    output_dir: str = "output"
    output_format: str = "json"
    cobrix_version: str = "2.6.9"
    ebcdic_encoding: str = "cp037"
    spark_app_name: str = "MainframeMigration"
    spark_master: str = "local[*]"
    datasets: list[DatasetDefinition] = field(default_factory=list)
    clean_output: bool = True
    validate_output: bool = True
    generate_manifest: bool = True

    # DB2 connection settings
    db2_url: Optional[str] = None
    db2_user: Optional[str] = None
    db2_password: Optional[str] = None
    db2_driver: str = "com.ibm.db2.jcc.DB2Driver"
    db2_mock_mode: bool = True

    # Advanced options
    coalesce_output: bool = True
    partition_by: Optional[list[str]] = None
    extra_spark_configs: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_file(cls, filepath: str) -> "MigrationConfig":
        """
        Load configuration from a JSON file.

        Args:
            filepath: Path to the configuration file

        Returns:
            MigrationConfig instance
        """
        with open(filepath, "r") as f:
            data = json.load(f)
        return cls.from_dict(data)

    @classmethod
    def from_dict(cls, data: dict) -> "MigrationConfig":
        """
        Create configuration from a dictionary.

        Args:
            data: Configuration dictionary

        Returns:
            MigrationConfig instance
        """
        datasets = []
        if "datasets" in data:
            for ds_data in data["datasets"]:
                datasets.append(DatasetDefinition.from_dict(ds_data))

        return cls(
            input_dir=data.get("input_dir", "input"),
            output_dir=data.get("output_dir", "output"),
            output_format=data.get("output_format", "json"),
            cobrix_version=data.get("cobrix_version", "2.6.9"),
            ebcdic_encoding=data.get("ebcdic_encoding", "cp037"),
            spark_app_name=data.get("spark_app_name", "MainframeMigration"),
            spark_master=data.get("spark_master", "local[*]"),
            datasets=datasets,
            clean_output=data.get("clean_output", True),
            validate_output=data.get("validate_output", True),
            generate_manifest=data.get("generate_manifest", True),
            db2_url=data.get("db2_url"),
            db2_user=data.get("db2_user"),
            db2_password=data.get("db2_password"),
            db2_driver=data.get("db2_driver", "com.ibm.db2.jcc.DB2Driver"),
            db2_mock_mode=data.get("db2_mock_mode", True),
            coalesce_output=data.get("coalesce_output", True),
            partition_by=data.get("partition_by"),
            extra_spark_configs=data.get("extra_spark_configs", {}),
        )

    def to_dict(self) -> dict:
        """Convert configuration to dictionary."""
        return {
            "input_dir": self.input_dir,
            "output_dir": self.output_dir,
            "output_format": self.output_format,
            "cobrix_version": self.cobrix_version,
            "ebcdic_encoding": self.ebcdic_encoding,
            "spark_app_name": self.spark_app_name,
            "spark_master": self.spark_master,
            "datasets": [ds.to_dict() for ds in self.datasets],
            "clean_output": self.clean_output,
            "validate_output": self.validate_output,
            "generate_manifest": self.generate_manifest,
            "db2_url": self.db2_url,
            "db2_user": self.db2_user,
            "db2_driver": self.db2_driver,
            "db2_mock_mode": self.db2_mock_mode,
            "coalesce_output": self.coalesce_output,
            "partition_by": self.partition_by,
            "extra_spark_configs": self.extra_spark_configs,
        }

    def save(self, filepath: str) -> None:
        """
        Save configuration to a JSON file.

        Args:
            filepath: Path to save the configuration
        """
        with open(filepath, "w") as f:
            json.dump(self.to_dict(), f, indent=2)

    def get_data_path(self, filename: str) -> str:
        """Get full path to a data file."""
        return os.path.join(self.input_dir, filename)

    def get_output_path(self, dataset_name: str) -> str:
        """Get full path to output directory for a dataset."""
        return os.path.join(self.output_dir, dataset_name)

    def get_enabled_datasets(self) -> list[DatasetDefinition]:
        """Get list of enabled datasets."""
        return [ds for ds in self.datasets if ds.enabled]

    def validate(self) -> tuple[bool, list[str]]:
        """
        Validate the configuration.

        Returns:
            Tuple of (is_valid, error_messages)
        """
        errors = []

        # Check input directory
        if not os.path.isdir(self.input_dir):
            errors.append(f"Input directory does not exist: {self.input_dir}")

        # Check output format
        valid_formats = ["json", "parquet", "csv", "delta"]
        if self.output_format not in valid_formats:
            errors.append(f"Invalid output format: {self.output_format}")

        # Check dataset definitions
        for ds in self.datasets:
            if ds.data_file and ds.copybook:
                # VSAM dataset
                data_path = self.get_data_path(ds.data_file)
                copybook_path = self.get_data_path(ds.copybook)
                if not os.path.exists(data_path):
                    errors.append(f"Data file not found: {data_path}")
                if not os.path.exists(copybook_path):
                    errors.append(f"Copybook not found: {copybook_path}")
            elif ds.source_table:
                # DB2 dataset
                if not self.db2_mock_mode and not self.db2_url:
                    errors.append("DB2 URL required for non-mock mode")

        return len(errors) == 0, errors


# Default dataset configurations for CardDemo
CARDDEMO_DATASETS = [
    DatasetDefinition(
        name="customers",
        data_file="AWS.M2.CARDDEMO.CUSTDATA.PS",
        copybook="CUSTREC.cpy",
        description="Customer master data with demographics and contact info",
        record_length=500,
    ),
    DatasetDefinition(
        name="accounts",
        data_file="AWS.M2.CARDDEMO.ACCTDATA.PS",
        copybook="CVACT01Y.cpy",
        description="Account records with balances and credit limits",
        record_length=300,
    ),
    DatasetDefinition(
        name="cards",
        data_file="AWS.M2.CARDDEMO.CARDDATA.PS",
        copybook="CVACT02Y.cpy",
        description="Credit card information with CVV and expiration",
        record_length=150,
    ),
    DatasetDefinition(
        name="card_xref",
        data_file="AWS.M2.CARDDEMO.CARDXREF.PS",
        copybook="CVACT03Y.cpy",
        description="Card to customer/account cross-reference",
        record_length=50,
    ),
    DatasetDefinition(
        name="daily_transactions",
        data_file="AWS.M2.CARDDEMO.DALYTRAN.PS",
        copybook="CVTRA06Y.cpy",
        description="Daily transaction records with merchant details",
        record_length=350,
    ),
    DatasetDefinition(
        name="transaction_types",
        data_file="AWS.M2.CARDDEMO.TRANTYPE.PS",
        copybook="CVTRA03Y.cpy",
        description="Transaction type reference data",
        record_length=60,
    ),
    DatasetDefinition(
        name="transaction_categories",
        data_file="AWS.M2.CARDDEMO.TRANCATG.PS",
        copybook="CVTRA04Y.cpy",
        description="Transaction category reference data",
        record_length=60,
    ),
    DatasetDefinition(
        name="users",
        data_file="AWS.M2.CARDDEMO.USRSEC.PS",
        copybook="CSUSR01Y.cpy",
        description="User security and authentication data",
        record_length=80,
    ),
    DatasetDefinition(
        name="discount_groups",
        data_file="AWS.M2.CARDDEMO.DISCGRP.PS",
        copybook="DISCGRP.cpy",
        description="Discount group configuration by transaction type",
        record_length=50,
    ),
    DatasetDefinition(
        name="category_balances",
        data_file="AWS.M2.CARDDEMO.TCATBALF.PS",
        copybook="TCATBALF.cpy",
        description="Transaction category balance summaries",
        record_length=50,
    ),
    DatasetDefinition(
        name="export_data",
        data_file="AWS.M2.CARDDEMO.EXPORT.DATA.PS",
        copybook="CVEXPORT.cpy",
        description="Multi-record export file for branch migration",
        record_length=500,
    ),
]


def get_carddemo_config(
    input_dir: str = "input",
    output_dir: str = "output",
) -> MigrationConfig:
    """
    Get a pre-configured MigrationConfig for CardDemo.

    Args:
        input_dir: Input directory path
        output_dir: Output directory path

    Returns:
        MigrationConfig for CardDemo datasets
    """
    return MigrationConfig(
        input_dir=input_dir,
        output_dir=output_dir,
        datasets=CARDDEMO_DATASETS.copy(),
        spark_app_name="CardDemo_Migration",
    )
