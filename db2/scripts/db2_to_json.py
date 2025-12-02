"""
DB2 to JSON Export Script.

Exports DB2 tables to JSON format using Spark + JDBC, following the same
pattern as the EBCDIC file migration.

This script can operate in two modes:
1. Connected Mode: Direct JDBC connection to DB2 (requires DB2 access)
2. Mock Mode: Uses sample data for local testing without DB2

Runtime Environment:
- Spark 3.5.0
- Java 17+ (requires --add-opens JVM flags)
- DB2 JDBC Driver (db2jcc4.jar) for connected mode
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from dataclasses import dataclass
from typing import Optional
import os
import shutil
import json
import sys

# --- CONFIGURATION ---
OUTPUT_DIR = "db2/output"

# Java 17+ requires "--add-opens" for Spark to work correctly
JAVA_17_OPTIONS = " ".join([
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.util=ALL-UNNAMED",
])

# DB2 Connection (set via environment variables for security)
DB2_CONFIG = {
    "url": os.environ.get("DB2_URL", "jdbc:db2://localhost:50000/CARDDEMO"),
    "user": os.environ.get("DB2_USER", "db2admin"),
    "password": os.environ.get("DB2_PASSWORD", ""),
    "driver": "com.ibm.db2.jcc.DB2Driver",
    "fetchsize": "10000",
}


@dataclass
class TableConfig:
    """Configuration for a DB2 table export."""
    name: str                    # Logical name for output
    source_table: str           # Full DB2 table name (schema.table)
    description: str            # Table description
    primary_key: list[str]      # Primary key columns
    foreign_keys: Optional[dict] = None  # FK relationships


# Tables to export (in dependency order - parents first)
TABLES = [
    TableConfig(
        name="transaction_type",
        source_table="CARDDEMO.TRANSACTION_TYPE",
        description="Transaction type reference data",
        primary_key=["TR_TYPE"],
    ),
    TableConfig(
        name="transaction_type_category",
        source_table="CARDDEMO.TRANSACTION_TYPE_CATEGORY",
        description="Transaction category reference data",
        primary_key=["TRC_TYPE_CODE", "TRC_TYPE_CATEGORY"],
        foreign_keys={"TRC_TYPE_CODE": "CARDDEMO.TRANSACTION_TYPE.TR_TYPE"},
    ),
]

# Sample data for mock mode (when DB2 is not available)
MOCK_DATA = {
    "CARDDEMO.TRANSACTION_TYPE": [
        {"TR_TYPE": "01", "TR_DESCRIPTION": "Purchase"},
        {"TR_TYPE": "02", "TR_DESCRIPTION": "Cash Advance"},
        {"TR_TYPE": "03", "TR_DESCRIPTION": "Balance Transfer"},
        {"TR_TYPE": "04", "TR_DESCRIPTION": "Payment"},
        {"TR_TYPE": "05", "TR_DESCRIPTION": "Refund"},
        {"TR_TYPE": "06", "TR_DESCRIPTION": "Fee"},
        {"TR_TYPE": "07", "TR_DESCRIPTION": "Interest"},
    ],
    "CARDDEMO.TRANSACTION_TYPE_CATEGORY": [
        {"TRC_TYPE_CODE": "01", "TRC_TYPE_CATEGORY": "5411", "TRC_CAT_DATA": "Grocery Stores"},
        {"TRC_TYPE_CODE": "01", "TRC_TYPE_CATEGORY": "5541", "TRC_CAT_DATA": "Gas Stations"},
        {"TRC_TYPE_CODE": "01", "TRC_TYPE_CATEGORY": "5812", "TRC_CAT_DATA": "Restaurants"},
        {"TRC_TYPE_CODE": "01", "TRC_TYPE_CATEGORY": "5912", "TRC_CAT_DATA": "Drug Stores"},
        {"TRC_TYPE_CODE": "01", "TRC_TYPE_CATEGORY": "5999", "TRC_CAT_DATA": "Miscellaneous Retail"},
        {"TRC_TYPE_CODE": "02", "TRC_TYPE_CATEGORY": "6010", "TRC_CAT_DATA": "ATM Cash Advance"},
        {"TRC_TYPE_CODE": "02", "TRC_TYPE_CATEGORY": "6011", "TRC_CAT_DATA": "Bank Cash Advance"},
        {"TRC_TYPE_CODE": "03", "TRC_TYPE_CATEGORY": "0001", "TRC_CAT_DATA": "Internal Transfer"},
        {"TRC_TYPE_CODE": "03", "TRC_TYPE_CATEGORY": "0002", "TRC_CAT_DATA": "External Transfer"},
        {"TRC_TYPE_CODE": "04", "TRC_TYPE_CATEGORY": "0001", "TRC_CAT_DATA": "Online Payment"},
        {"TRC_TYPE_CODE": "04", "TRC_TYPE_CATEGORY": "0002", "TRC_CAT_DATA": "Auto Payment"},
        {"TRC_TYPE_CODE": "04", "TRC_TYPE_CATEGORY": "0003", "TRC_CAT_DATA": "Phone Payment"},
        {"TRC_TYPE_CODE": "05", "TRC_TYPE_CATEGORY": "0001", "TRC_CAT_DATA": "Merchant Refund"},
        {"TRC_TYPE_CODE": "05", "TRC_TYPE_CATEGORY": "0002", "TRC_CAT_DATA": "Dispute Credit"},
        {"TRC_TYPE_CODE": "06", "TRC_TYPE_CATEGORY": "0001", "TRC_CAT_DATA": "Annual Fee"},
        {"TRC_TYPE_CODE": "06", "TRC_TYPE_CATEGORY": "0002", "TRC_CAT_DATA": "Late Fee"},
        {"TRC_TYPE_CODE": "06", "TRC_TYPE_CATEGORY": "0003", "TRC_CAT_DATA": "Over Limit Fee"},
        {"TRC_TYPE_CODE": "07", "TRC_TYPE_CATEGORY": "0001", "TRC_CAT_DATA": "Purchase Interest"},
    ],
}


def clean_output(path: str) -> None:
    """Remove previous output directory if it exists."""
    if os.path.exists(path):
        shutil.rmtree(path)


def create_spark_session(use_jdbc: bool = False) -> SparkSession:
    """
    Create a Spark session configured for DB2 migration.

    Args:
        use_jdbc: If True, configure for DB2 JDBC driver
    """
    builder = (
        SparkSession.builder
        .appName("DB2_to_JSON_Export")
        .config("spark.driver.extraJavaOptions", JAVA_17_OPTIONS)
        .config("spark.executor.extraJavaOptions", JAVA_17_OPTIONS)
        .config("spark.sql.session.timeZone", "UTC")
        .master("local[*]")
    )

    if use_jdbc:
        # Add DB2 JDBC driver if available
        db2_jar = "db2/libs/db2jcc4.jar"
        if os.path.exists(db2_jar):
            builder = builder.config("spark.jars", db2_jar)

    return builder.getOrCreate()


def read_from_db2(spark: SparkSession, table: str) -> "DataFrame":
    """
    Read a table from DB2 via JDBC.

    Args:
        spark: Active Spark session
        table: Full table name (schema.table)

    Returns:
        DataFrame containing table data
    """
    return (
        spark.read
        .format("jdbc")
        .option("url", DB2_CONFIG["url"])
        .option("dbtable", table)
        .option("user", DB2_CONFIG["user"])
        .option("password", DB2_CONFIG["password"])
        .option("driver", DB2_CONFIG["driver"])
        .option("fetchsize", DB2_CONFIG["fetchsize"])
        .load()
    )


def read_mock_data(spark: SparkSession, table: str) -> "DataFrame":
    """
    Create a DataFrame from mock data for testing without DB2.

    Args:
        spark: Active Spark session
        table: Full table name (schema.table)

    Returns:
        DataFrame containing mock data
    """
    if table not in MOCK_DATA:
        raise ValueError(f"No mock data defined for table: {table}")

    data = MOCK_DATA[table]
    return spark.createDataFrame(data)


def transform_columns(df: "DataFrame") -> "DataFrame":
    """
    Transform column names from DB2 uppercase to lowercase.

    Args:
        df: Input DataFrame

    Returns:
        DataFrame with lowercase column names
    """
    for col_name in df.columns:
        df = df.withColumnRenamed(col_name, col_name.lower())
    return df


def export_table(
    spark: SparkSession,
    config: TableConfig,
    use_mock: bool = True
) -> dict:
    """
    Export a single DB2 table to JSON.

    Args:
        spark: Active Spark session
        config: Table configuration
        use_mock: If True, use mock data instead of DB2 connection

    Returns:
        Dictionary with export results
    """
    output_path = os.path.join(OUTPUT_DIR, config.name)

    result = {
        "name": config.name,
        "source_table": config.source_table,
        "status": "pending",
        "record_count": 0,
        "error": None,
        "mode": "mock" if use_mock else "jdbc",
    }

    try:
        # Clean previous output
        clean_output(output_path)

        # Read data
        if use_mock:
            df = read_mock_data(spark, config.source_table)
        else:
            df = read_from_db2(spark, config.source_table)

        # Transform column names to lowercase
        df = transform_columns(df)

        # Get record count
        record_count = df.count()
        result["record_count"] = record_count

        # Write to JSON
        df.coalesce(1).write.mode("overwrite").json(output_path)

        result["status"] = "success"
        result["output_path"] = output_path

    except Exception as e:
        result["status"] = "failed"
        result["error"] = str(e)

    return result


def print_summary(results: list[dict]) -> None:
    """Print a summary of all export results."""
    print("\n" + "=" * 80)
    print("EXPORT SUMMARY")
    print("=" * 80)

    success_count = sum(1 for r in results if r["status"] == "success")
    failed_count = sum(1 for r in results if r["status"] == "failed")
    total_records = sum(r["record_count"] for r in results)
    mode = results[0]["mode"] if results else "unknown"

    print(f"\nMode: {'Mock Data' if mode == 'mock' else 'DB2 JDBC'}")
    print(f"Total Tables: {len(results)}")
    print(f"  Successful: {success_count}")
    print(f"  Failed: {failed_count}")
    print(f"\nTotal Records Exported: {total_records:,}")

    print("\n" + "-" * 80)
    print(f"{'Table':<30} {'Status':<10} {'Records':>10} {'Source'}")
    print("-" * 80)

    for r in results:
        print(f"{r['name']:<30} {r['status']:<10} {r['record_count']:>10,} {r['source_table']}")

    print("-" * 80)


def main():
    """Main execution function."""
    # Check for mock mode (default) vs JDBC mode
    use_mock = os.environ.get("DB2_MODE", "mock").lower() == "mock"

    print("=" * 80)
    print("DB2 to JSON Export")
    print("=" * 80)
    print(f"\nMode: {'Mock Data (local testing)' if use_mock else 'DB2 JDBC Connection'}")

    if not use_mock:
        print(f"DB2 URL: {DB2_CONFIG['url']}")
        print(f"DB2 User: {DB2_CONFIG['user']}")

    print(f"\nOutput Directory: {OUTPUT_DIR}")

    # Create output directory
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Initialize Spark
    print("\nInitializing Spark session...")
    spark = create_spark_session(use_jdbc=not use_mock)

    results = []

    try:
        print(f"\nExporting {len(TABLES)} tables...\n")

        for i, config in enumerate(TABLES, 1):
            print(f"[{i}/{len(TABLES)}] Exporting {config.name}...")
            print(f"         Source: {config.source_table}")
            print(f"         Description: {config.description}")

            result = export_table(spark, config, use_mock=use_mock)
            results.append(result)

            if result["status"] == "success":
                print(f"         ✓ Success: {result['record_count']:,} records")
            else:
                print(f"         ✗ Failed: {result['error']}")

            print()

        # Print summary
        print_summary(results)

        # Save results manifest
        manifest_path = os.path.join(OUTPUT_DIR, "export_manifest.json")
        with open(manifest_path, "w") as f:
            json.dump({
                "tables": results,
                "total_records": sum(r["record_count"] for r in results),
                "mode": "mock" if use_mock else "jdbc",
                "spark_version": "3.5.0",
            }, f, indent=2)
        print(f"\nManifest saved to: {manifest_path}")

        # Return appropriate exit code
        if any(r["status"] == "failed" for r in results):
            return 1
        return 0

    except Exception as e:
        print(f"\nFATAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        return 1

    finally:
        spark.stop()
        print("\nSpark session stopped.")


if __name__ == "__main__":
    sys.exit(main())
