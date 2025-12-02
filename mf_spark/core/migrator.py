"""
Main Migrator Orchestrator.

Provides the main entry point for mainframe data migrations.
Orchestrates VSAM and DB2 conversions with unified output.

Example:
    >>> from mf_spark import MainframeMigrator
    >>> migrator = MainframeMigrator()
    >>> results = migrator.run()
    >>> print(migrator.summary())
"""

import os
import json
from datetime import datetime
from typing import Optional, Any
from dataclasses import dataclass, field

from mf_spark.core.session import SparkSessionManager
from mf_spark.core.base import DatasetResult, MigrationStatus, OutputFormat
from mf_spark.config.settings import MigrationConfig, DatasetDefinition, get_carddemo_config
from mf_spark.validators.data_validator import DataValidator, ValidationResult
from mf_spark.utils.file_utils import clean_output, ensure_directory, save_manifest


@dataclass
class MigrationSummary:
    """
    Summary of a migration run.

    Attributes:
        start_time: When migration started
        end_time: When migration completed
        total_datasets: Total number of datasets
        successful: Number of successful conversions
        failed: Number of failed conversions
        skipped: Number of skipped datasets
        total_records: Total records processed
        results: List of individual dataset results
    """
    start_time: datetime = field(default_factory=datetime.now)
    end_time: Optional[datetime] = None
    total_datasets: int = 0
    successful: int = 0
    failed: int = 0
    skipped: int = 0
    total_records: int = 0
    results: list[DatasetResult] = field(default_factory=list)

    @property
    def duration_seconds(self) -> Optional[float]:
        """Calculate total duration in seconds."""
        if self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None

    @property
    def success_rate(self) -> float:
        """Calculate success rate as percentage."""
        if self.total_datasets == 0:
            return 0.0
        return (self.successful / self.total_datasets) * 100

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "duration_seconds": self.duration_seconds,
            "total_datasets": self.total_datasets,
            "successful": self.successful,
            "failed": self.failed,
            "skipped": self.skipped,
            "total_records": self.total_records,
            "success_rate": f"{self.success_rate:.1f}%",
            "results": [r.to_dict() for r in self.results],
        }


class MainframeMigrator:
    """
    Main orchestrator for mainframe data migrations.

    This class provides a unified interface for migrating both VSAM
    and DB2 data to modern formats (JSON, Parquet).

    Features:
        - VSAM/EBCDIC to JSON conversion using Cobrix
        - DB2 table export via JDBC or mock mode
        - Data validation and quality checks
        - Manifest generation for audit trails
        - Configurable output formats

    Example:
        >>> migrator = MainframeMigrator()
        >>> results = migrator.run()
        >>> if migrator.summary.failed == 0:
        ...     print("All migrations successful!")

    Attributes:
        config: Migration configuration
        session_manager: Spark session manager
        validator: Data validator
        summary: Migration summary
    """

    def __init__(
        self,
        config: Optional[MigrationConfig] = None,
        use_carddemo_defaults: bool = True,
    ):
        """
        Initialize the migrator.

        Args:
            config: Migration configuration (optional)
            use_carddemo_defaults: Use CardDemo default datasets if no config
        """
        if config is None:
            if use_carddemo_defaults:
                config = get_carddemo_config()
            else:
                config = MigrationConfig()

        self.config = config
        self.session_manager: Optional[SparkSessionManager] = None
        self.validator = DataValidator()
        self.summary = MigrationSummary()
        self._spark = None

    def run(
        self,
        datasets: Optional[list[str]] = None,
        output_format: OutputFormat = OutputFormat.JSON,
    ) -> list[DatasetResult]:
        """
        Run the migration for all configured datasets.

        Args:
            datasets: Optional list of dataset names to process (None = all)
            output_format: Output format to use

        Returns:
            List of DatasetResult objects
        """
        self.summary = MigrationSummary()
        self.summary.start_time = datetime.now()

        # Get datasets to process
        datasets_to_process = self.config.get_enabled_datasets()
        if datasets:
            datasets_to_process = [
                ds for ds in datasets_to_process
                if ds.name in datasets
            ]

        self.summary.total_datasets = len(datasets_to_process)

        print("=" * 80)
        print("MAINFRAME DATA MIGRATION")
        print("=" * 80)
        print(f"Start Time: {self.summary.start_time}")
        print(f"Input Directory: {self.config.input_dir}")
        print(f"Output Directory: {self.config.output_dir}")
        print(f"Output Format: {output_format.value}")
        print(f"Datasets: {len(datasets_to_process)}")
        print("=" * 80)

        # Ensure output directory exists
        ensure_directory(self.config.output_dir)

        # Initialize Spark session
        print("\nInitializing Spark session...")
        self._init_spark()

        try:
            # Process each dataset
            for i, dataset in enumerate(datasets_to_process, 1):
                print(f"\n[{i}/{len(datasets_to_process)}] Processing {dataset.name}...")
                result = self._process_dataset(dataset, output_format)
                self.summary.results.append(result)

                if result.status == MigrationStatus.SUCCESS:
                    self.summary.successful += 1
                    self.summary.total_records += result.record_count
                    print(f"    ✓ Success: {result.record_count:,} records")
                elif result.status == MigrationStatus.SKIPPED:
                    self.summary.skipped += 1
                    print(f"    ⊘ Skipped: {result.error}")
                else:
                    self.summary.failed += 1
                    print(f"    ✗ Failed: {result.error}")

            # Generate manifest
            if self.config.generate_manifest:
                self._save_manifest()

        finally:
            # Stop Spark session
            self._stop_spark()

        self.summary.end_time = datetime.now()

        # Print summary
        self._print_summary()

        return self.summary.results

    def run_vsam(
        self,
        data_file: str,
        copybook: str,
        output_name: str,
        output_format: OutputFormat = OutputFormat.JSON,
    ) -> DatasetResult:
        """
        Run migration for a single VSAM file.

        Args:
            data_file: Path to the EBCDIC data file
            copybook: Path to the copybook file
            output_name: Name for the output directory
            output_format: Output format

        Returns:
            DatasetResult with conversion details
        """
        dataset = DatasetDefinition(
            name=output_name,
            data_file=os.path.basename(data_file),
            copybook=os.path.basename(copybook),
        )

        # Override input dir if full paths provided
        if os.path.isabs(data_file):
            self.config.input_dir = os.path.dirname(data_file)

        return self.run(datasets=[output_name])[0]

    def _init_spark(self) -> None:
        """Initialize the Spark session."""
        self.session_manager = SparkSessionManager(
            app_name=self.config.spark_app_name,
            master=self.config.spark_master,
            cobrix_version=self.config.cobrix_version,
            enable_cobrix=True,
            extra_configs=self.config.extra_spark_configs,
        )
        self._spark = self.session_manager.get_or_create()

    def _stop_spark(self) -> None:
        """Stop the Spark session."""
        if self.session_manager:
            self.session_manager.stop()
            self._spark = None

    def _process_dataset(
        self,
        dataset: DatasetDefinition,
        output_format: OutputFormat,
    ) -> DatasetResult:
        """
        Process a single dataset.

        Args:
            dataset: Dataset definition
            output_format: Output format

        Returns:
            DatasetResult with processing details
        """
        result = DatasetResult(name=dataset.name)
        result.start_time = datetime.now()

        try:
            # Determine source type and process accordingly
            if dataset.data_file and dataset.copybook:
                result = self._process_vsam(dataset, output_format)
            elif dataset.source_table:
                result = self._process_db2(dataset, output_format)
            else:
                result.status = MigrationStatus.SKIPPED
                result.error = "No data source specified"

        except Exception as e:
            result.status = MigrationStatus.FAILED
            result.error = str(e)

        result.end_time = datetime.now()
        return result

    def _process_vsam(
        self,
        dataset: DatasetDefinition,
        output_format: OutputFormat,
    ) -> DatasetResult:
        """
        Process a VSAM/EBCDIC dataset.

        Args:
            dataset: Dataset definition with data_file and copybook
            output_format: Output format

        Returns:
            DatasetResult
        """
        result = DatasetResult(name=dataset.name)
        result.start_time = datetime.now()

        # Build paths
        data_path = self.config.get_data_path(dataset.data_file)
        copybook_path = self.config.get_data_path(dataset.copybook)
        output_path = self.config.get_output_path(dataset.name)

        # Validate files exist
        if not os.path.exists(data_path):
            result.status = MigrationStatus.SKIPPED
            result.error = f"Data file not found: {data_path}"
            return result

        if not os.path.exists(copybook_path):
            result.status = MigrationStatus.SKIPPED
            result.error = f"Copybook not found: {copybook_path}"
            return result

        result.source_info = {
            "data_file": dataset.data_file,
            "copybook": dataset.copybook,
            "type": "vsam",
        }

        try:
            # Clean previous output
            if self.config.clean_output:
                clean_output(output_path)

            # Read using Cobrix
            df = (
                self._spark.read
                .format("cobol")
                .option("copybook", copybook_path)
                .option("record_format", dataset.record_format)
                .option("schema_retention_policy", "collapse_root")
                .option("generate_record_id", "true")
                .load(data_path)
            )

            # Get record count
            result.record_count = df.count()

            # Write output
            self._write_output(df, output_path, output_format)

            result.status = MigrationStatus.SUCCESS
            result.output_path = output_path

            # Validate if configured
            if self.config.validate_output:
                validation = self.validator.validate(
                    df,
                    expected_count=result.record_count,
                )
                if not validation.is_valid:
                    result.source_info["validation_errors"] = validation.errors

        except Exception as e:
            result.status = MigrationStatus.FAILED
            result.error = str(e)

        result.end_time = datetime.now()
        return result

    def _process_db2(
        self,
        dataset: DatasetDefinition,
        output_format: OutputFormat,
    ) -> DatasetResult:
        """
        Process a DB2 dataset.

        Args:
            dataset: Dataset definition with source_table
            output_format: Output format

        Returns:
            DatasetResult
        """
        result = DatasetResult(name=dataset.name)
        result.start_time = datetime.now()

        output_path = self.config.get_output_path(dataset.name)

        result.source_info = {
            "source_table": dataset.source_table,
            "type": "db2",
            "mode": "mock" if self.config.db2_mock_mode else "jdbc",
        }

        try:
            # Clean previous output
            if self.config.clean_output:
                clean_output(output_path)

            if self.config.db2_mock_mode:
                # Use mock data for testing
                result.status = MigrationStatus.SKIPPED
                result.error = "DB2 mock mode - no actual data"
            else:
                # Read from DB2 via JDBC
                df = (
                    self._spark.read
                    .format("jdbc")
                    .option("url", self.config.db2_url)
                    .option("dbtable", dataset.source_table)
                    .option("user", self.config.db2_user)
                    .option("password", self.config.db2_password)
                    .option("driver", self.config.db2_driver)
                    .load()
                )

                result.record_count = df.count()
                self._write_output(df, output_path, output_format)
                result.status = MigrationStatus.SUCCESS
                result.output_path = output_path

        except Exception as e:
            result.status = MigrationStatus.FAILED
            result.error = str(e)

        result.end_time = datetime.now()
        return result

    def _write_output(
        self,
        df: Any,
        output_path: str,
        output_format: OutputFormat,
    ) -> None:
        """
        Write DataFrame to output.

        Args:
            df: DataFrame to write
            output_path: Output path
            output_format: Format to write
        """
        writer = df

        if self.config.coalesce_output:
            writer = writer.coalesce(1)

        if self.config.partition_by:
            writer = writer.partitionBy(*self.config.partition_by)

        if output_format == OutputFormat.JSON:
            writer.write.mode("overwrite").json(output_path)
        elif output_format == OutputFormat.PARQUET:
            writer.write.mode("overwrite").parquet(output_path)
        elif output_format == OutputFormat.CSV:
            writer.write.mode("overwrite").option("header", True).csv(output_path)
        elif output_format == OutputFormat.DELTA:
            writer.write.mode("overwrite").format("delta").save(output_path)

    def _save_manifest(self) -> None:
        """Save migration manifest."""
        manifest_path = os.path.join(
            self.config.output_dir,
            "conversion_manifest.json"
        )

        manifest = {
            "migration_summary": self.summary.to_dict(),
            "config": {
                "cobrix_version": self.config.cobrix_version,
                "spark_version": self._spark.version if self._spark else "unknown",
                "output_format": self.config.output_format,
            },
        }

        save_manifest(manifest_path, manifest)
        print(f"\nManifest saved to: {manifest_path}")

    def _print_summary(self) -> None:
        """Print migration summary."""
        print("\n" + "=" * 80)
        print("MIGRATION SUMMARY")
        print("=" * 80)

        duration = self.summary.duration_seconds or 0
        print(f"\nDuration: {duration:.1f} seconds")
        print(f"Total Datasets: {self.summary.total_datasets}")
        print(f"  Successful: {self.summary.successful}")
        print(f"  Failed: {self.summary.failed}")
        print(f"  Skipped: {self.summary.skipped}")
        print(f"\nTotal Records: {self.summary.total_records:,}")
        print(f"Success Rate: {self.summary.success_rate:.1f}%")

        print("\n" + "-" * 80)
        print(f"{'Dataset':<25} {'Status':<10} {'Records':>12} {'Duration':>10}")
        print("-" * 80)

        for r in self.summary.results:
            duration_str = ""
            if r.duration_seconds:
                duration_str = f"{r.duration_seconds:.1f}s"
            print(f"{r.name:<25} {r.status.value:<10} {r.record_count:>12,} {duration_str:>10}")

        print("-" * 80)

    def get_summary(self) -> MigrationSummary:
        """Get the migration summary."""
        return self.summary
