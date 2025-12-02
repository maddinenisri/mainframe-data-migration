"""
Base classes for migration operations.

Provides abstract base classes and data structures used across
all migration operations (VSAM, DB2, etc.).
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional, Any
from datetime import datetime
from enum import Enum


class MigrationStatus(Enum):
    """Status of a migration operation."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


class SourceType(Enum):
    """Type of mainframe data source."""
    VSAM = "vsam"
    DB2 = "db2"
    IMS = "ims"
    SEQUENTIAL = "sequential"


class OutputFormat(Enum):
    """Supported output formats."""
    JSON = "json"
    PARQUET = "parquet"
    CSV = "csv"
    DELTA = "delta"


@dataclass
class DatasetConfig:
    """
    Configuration for a dataset to be migrated.

    This class holds all the metadata needed to identify and process
    a mainframe dataset, whether from VSAM files or DB2 tables.

    Attributes:
        name: Logical name for the dataset (used for output directory)
        description: Human-readable description of the dataset
        source_type: Type of source (VSAM, DB2, etc.)
        record_length: Expected record length for validation
        primary_key: Primary key column(s) for the dataset
        foreign_keys: Foreign key relationships (column -> target table)

    Example:
        >>> config = DatasetConfig(
        ...     name="customers",
        ...     description="Customer master data",
        ...     source_type=SourceType.VSAM,
        ...     record_length=500,
        ...     primary_key=["CUST_ID"]
        ... )
    """
    name: str
    description: str = ""
    source_type: SourceType = SourceType.VSAM
    record_length: Optional[int] = None
    primary_key: list[str] = field(default_factory=list)
    foreign_keys: dict[str, str] = field(default_factory=dict)

    # VSAM-specific fields
    data_file: Optional[str] = None
    copybook: Optional[str] = None
    record_format: str = "F"  # F=Fixed, V=Variable, VB=Variable Blocked

    # DB2-specific fields
    source_table: Optional[str] = None
    source_schema: Optional[str] = None


@dataclass
class DatasetResult:
    """
    Result of a dataset migration operation.

    This class captures all the details of a migration attempt,
    including success/failure status, record counts, and timing.

    Attributes:
        name: Dataset name
        status: Migration status (success, failed, etc.)
        record_count: Number of records processed
        error: Error message if failed
        output_path: Path to output files
        start_time: When the migration started
        end_time: When the migration completed
        source_info: Additional source metadata

    Example:
        >>> result = DatasetResult(
        ...     name="customers",
        ...     status=MigrationStatus.SUCCESS,
        ...     record_count=1000,
        ...     output_path="output/customers"
        ... )
    """
    name: str
    status: MigrationStatus = MigrationStatus.PENDING
    record_count: int = 0
    error: Optional[str] = None
    output_path: Optional[str] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    source_info: dict[str, Any] = field(default_factory=dict)

    @property
    def duration_seconds(self) -> Optional[float]:
        """Calculate the duration of the migration in seconds."""
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None

    @property
    def is_success(self) -> bool:
        """Check if the migration was successful."""
        return self.status == MigrationStatus.SUCCESS

    def to_dict(self) -> dict[str, Any]:
        """Convert result to dictionary for JSON serialization."""
        return {
            "name": self.name,
            "status": self.status.value,
            "record_count": self.record_count,
            "error": self.error,
            "output_path": self.output_path,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "duration_seconds": self.duration_seconds,
            "source_info": self.source_info,
        }


class BaseMigrator(ABC):
    """
    Abstract base class for all data migrators.

    This class defines the interface that all migrators must implement,
    ensuring consistent behavior across different source types.

    Subclasses must implement:
        - validate_source(): Validate source files/connections
        - read_source(): Read data from source
        - transform(): Apply transformations
        - write_output(): Write to target format

    Example:
        >>> class VSAMMigrator(BaseMigrator):
        ...     def validate_source(self) -> bool:
        ...         return os.path.exists(self.data_file)
        ...     def read_source(self):
        ...         return spark.read.format("cobol").load(self.data_file)
    """

    def __init__(self, config: DatasetConfig):
        """
        Initialize the migrator with dataset configuration.

        Args:
            config: Dataset configuration object
        """
        self.config = config
        self._result = DatasetResult(name=config.name)

    @abstractmethod
    def validate_source(self) -> bool:
        """
        Validate that the source data is accessible.

        Returns:
            True if source is valid and accessible
        """
        pass

    @abstractmethod
    def read_source(self) -> Any:
        """
        Read data from the source.

        Returns:
            DataFrame containing the source data
        """
        pass

    @abstractmethod
    def transform(self, df: Any) -> Any:
        """
        Apply transformations to the data.

        Args:
            df: Input DataFrame

        Returns:
            Transformed DataFrame
        """
        pass

    @abstractmethod
    def write_output(self, df: Any, output_path: str, output_format: OutputFormat) -> None:
        """
        Write data to the output destination.

        Args:
            df: DataFrame to write
            output_path: Path to write output
            output_format: Format to write (JSON, Parquet, etc.)
        """
        pass

    def migrate(self, output_path: str, output_format: OutputFormat = OutputFormat.JSON) -> DatasetResult:
        """
        Execute the full migration pipeline.

        This method orchestrates the full migration process:
        1. Validate source
        2. Read source data
        3. Transform data
        4. Write output

        Args:
            output_path: Base path for output files
            output_format: Output format (default: JSON)

        Returns:
            DatasetResult with migration details
        """
        self._result.start_time = datetime.now()

        try:
            # Step 1: Validate
            if not self.validate_source():
                self._result.status = MigrationStatus.SKIPPED
                self._result.error = "Source validation failed"
                return self._result

            self._result.status = MigrationStatus.IN_PROGRESS

            # Step 2: Read
            df = self.read_source()

            # Step 3: Transform
            df = self.transform(df)

            # Step 4: Get count
            self._result.record_count = df.count()

            # Step 5: Write
            self.write_output(df, output_path, output_format)

            self._result.status = MigrationStatus.SUCCESS
            self._result.output_path = output_path

        except Exception as e:
            self._result.status = MigrationStatus.FAILED
            self._result.error = str(e)

        finally:
            self._result.end_time = datetime.now()

        return self._result

    @property
    def result(self) -> DatasetResult:
        """Get the migration result."""
        return self._result
