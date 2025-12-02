"""
Spark Session Manager.

Provides centralized Spark session configuration with support for:
    - AWS Glue 5.0 compatibility (Spark 3.5 / Java 17+)
    - Cobrix for COBOL/EBCDIC parsing
    - DB2 JDBC connectivity
    - Configurable resource allocation

Example:
    manager = SparkSessionManager()
    spark = manager.get_or_create()
    # ... use spark session
    manager.stop()
"""

from pyspark.sql import SparkSession
from typing import Optional
import os


class SparkSessionManager:
    """
    Manages Spark session lifecycle with mainframe migration optimizations.

    This class provides a singleton-like pattern for Spark session management,
    ensuring consistent configuration across the migration pipeline.

    Attributes:
        app_name: Name of the Spark application
        master: Spark master URL (default: local[*])
        cobrix_version: Version of Cobrix package to use

    Example:
        >>> manager = SparkSessionManager(app_name="MyMigration")
        >>> spark = manager.get_or_create()
        >>> df = spark.read.format("cobol").load("data.ps")
    """

    # Java 17+ module access flags required for Spark compatibility
    JAVA_17_OPTIONS = " ".join([
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED",
    ])

    # Default Cobrix package for COBOL parsing
    DEFAULT_COBRIX_PACKAGE = "za.co.absa.cobrix:spark-cobol_2.12:2.6.9"

    def __init__(
        self,
        app_name: str = "MainframeMigration",
        master: str = "local[*]",
        cobrix_version: str = "2.6.9",
        enable_cobrix: bool = True,
        enable_jdbc: bool = False,
        jdbc_jar_path: Optional[str] = None,
        extra_packages: Optional[list[str]] = None,
        extra_configs: Optional[dict[str, str]] = None,
    ):
        """
        Initialize the Spark session manager.

        Args:
            app_name: Application name for Spark UI
            master: Spark master URL (local[*], yarn, etc.)
            cobrix_version: Version of Cobrix to use for COBOL parsing
            enable_cobrix: Whether to include Cobrix package
            enable_jdbc: Whether to configure for JDBC connections
            jdbc_jar_path: Path to JDBC driver JAR (e.g., db2jcc4.jar)
            extra_packages: Additional Maven packages to include
            extra_configs: Additional Spark configurations
        """
        self.app_name = app_name
        self.master = master
        self.cobrix_version = cobrix_version
        self.enable_cobrix = enable_cobrix
        self.enable_jdbc = enable_jdbc
        self.jdbc_jar_path = jdbc_jar_path
        self.extra_packages = extra_packages or []
        self.extra_configs = extra_configs or {}

        self._session: Optional[SparkSession] = None

    def _build_packages_string(self) -> str:
        """
        Build the Maven packages string for Spark configuration.

        Returns:
            Comma-separated list of Maven coordinates
        """
        packages = []

        if self.enable_cobrix:
            packages.append(f"za.co.absa.cobrix:spark-cobol_2.12:{self.cobrix_version}")

        packages.extend(self.extra_packages)

        return ",".join(packages)

    def get_or_create(self) -> SparkSession:
        """
        Get existing Spark session or create a new one.

        This method implements lazy initialization - the session is only
        created when first requested.

        Returns:
            Active SparkSession instance

        Example:
            >>> spark = manager.get_or_create()
            >>> spark.version
            '3.5.0'
        """
        if self._session is not None:
            return self._session

        builder = (
            SparkSession.builder
            .appName(self.app_name)
            .master(self.master)
            .config("spark.driver.extraJavaOptions", self.JAVA_17_OPTIONS)
            .config("spark.executor.extraJavaOptions", self.JAVA_17_OPTIONS)
            .config("spark.sql.session.timeZone", "UTC")
        )

        # Add Maven packages if any
        packages = self._build_packages_string()
        if packages:
            builder = builder.config("spark.jars.packages", packages)

        # Add JDBC driver if specified
        if self.enable_jdbc and self.jdbc_jar_path:
            if os.path.exists(self.jdbc_jar_path):
                builder = builder.config("spark.jars", self.jdbc_jar_path)

        # Apply extra configurations
        for key, value in self.extra_configs.items():
            builder = builder.config(key, value)

        self._session = builder.getOrCreate()
        return self._session

    def stop(self) -> None:
        """
        Stop the Spark session and release resources.

        This should be called when migration is complete to properly
        cleanup Spark resources.
        """
        if self._session is not None:
            self._session.stop()
            self._session = None

    @property
    def is_active(self) -> bool:
        """Check if the Spark session is currently active."""
        return self._session is not None

    def get_spark_version(self) -> str:
        """
        Get the Spark version.

        Returns:
            Spark version string (e.g., '3.5.0')
        """
        spark = self.get_or_create()
        return spark.version

    def get_cobrix_version(self) -> str:
        """
        Get the configured Cobrix version.

        Returns:
            Cobrix version string (e.g., '2.6.9')
        """
        return self.cobrix_version


# Convenience function for quick session creation
def create_spark_session(
    app_name: str = "MainframeMigration",
    enable_cobrix: bool = True,
) -> SparkSession:
    """
    Convenience function to create a Spark session with default settings.

    Args:
        app_name: Application name for Spark UI
        enable_cobrix: Whether to include Cobrix for COBOL parsing

    Returns:
        Configured SparkSession

    Example:
        >>> spark = create_spark_session("MyApp")
        >>> df = spark.read.format("cobol").load("data.ps")
    """
    manager = SparkSessionManager(app_name=app_name, enable_cobrix=enable_cobrix)
    return manager.get_or_create()
