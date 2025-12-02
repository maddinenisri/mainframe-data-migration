"""
DB2 to PostgreSQL Migration Script.

Migrates DB2 tables directly to PostgreSQL using Spark + JDBC, following the same
pattern as the EBCDIC file migration.

This script can operate in two modes:
1. Connected Mode: DB2 → PostgreSQL via JDBC (requires both databases)
2. Mock Mode: Sample data → PostgreSQL for local testing

Runtime Environment:
- Spark 3.5.0
- Java 17+ (requires --add-opens JVM flags)
- DB2 JDBC Driver (db2jcc4.jar) for DB2 connection
- PostgreSQL JDBC Driver (postgresql-42.x.jar) for PostgreSQL connection
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, trim
from dataclasses import dataclass
from typing import Optional
import os
import json
import sys

# --- CONFIGURATION ---

# Java 17+ requires "--add-opens" for Spark to work correctly
JAVA_17_OPTIONS = " ".join([
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.util=ALL-UNNAMED",
])

# DB2 Connection Configuration
DB2_CONFIG = {
    "url": os.environ.get("DB2_URL", "jdbc:db2://localhost:50000/CARDDEMO"),
    "user": os.environ.get("DB2_USER", "db2admin"),
    "password": os.environ.get("DB2_PASSWORD", ""),
    "driver": "com.ibm.db2.jcc.DB2Driver",
    "fetchsize": "10000",
}

# PostgreSQL Connection Configuration
PG_CONFIG = {
    "url": os.environ.get("PG_URL", "jdbc:postgresql://localhost:5432/carddemo"),
    "user": os.environ.get("PG_USER", "postgres"),
    "password": os.environ.get("PG_PASSWORD", "postgres"),
    "driver": "org.postgresql.Driver",
    "batchsize": "10000",
}

# JDBC Driver JAR locations
JDBC_JARS = {
    "db2": "db2/libs/db2jcc4.jar",
    "postgresql": "db2/libs/postgresql-42.7.4.jar",
}


@dataclass
class TableConfig:
    """Configuration for a DB2 to PostgreSQL table migration."""
    name: str                    # Logical name
    source_table: str           # Full DB2 table name (schema.table)
    target_table: str           # PostgreSQL table name
    description: str            # Table description
    primary_key: list[str]      # Primary key columns
    has_foreign_keys: bool = False  # If True, disable FK checks during load
    truncate_strings: bool = True   # Trim whitespace from CHAR fields


# Tables to migrate (in dependency order - parents first)
TABLES = [
    TableConfig(
        name="transaction_type",
        source_table="CARDDEMO.TRANSACTION_TYPE",
        target_table="transaction_type",
        description="Transaction type reference data",
        primary_key=["TR_TYPE"],
        has_foreign_keys=False,
    ),
    TableConfig(
        name="transaction_type_category",
        source_table="CARDDEMO.TRANSACTION_TYPE_CATEGORY",
        target_table="transaction_type_category",
        description="Transaction category reference data",
        primary_key=["TRC_TYPE_CODE", "TRC_TYPE_CATEGORY"],
        has_foreign_keys=True,
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


def create_spark_session(jars: list[str] = None) -> SparkSession:
    """
    Create a Spark session configured for JDBC migration.

    Args:
        jars: List of JAR file paths to include
    """
    builder = (
        SparkSession.builder
        .appName("DB2_to_PostgreSQL_Migration")
        .config("spark.driver.extraJavaOptions", JAVA_17_OPTIONS)
        .config("spark.executor.extraJavaOptions", JAVA_17_OPTIONS)
        .config("spark.sql.session.timeZone", "UTC")
        .master("local[*]")
    )

    # Add JDBC driver JARs if available
    if jars:
        existing_jars = [j for j in jars if os.path.exists(j)]
        if existing_jars:
            builder = builder.config("spark.jars", ",".join(existing_jars))

    return builder.getOrCreate()


def read_from_db2(spark: SparkSession, table: str) -> "DataFrame":
    """Read a table from DB2 via JDBC."""
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
    """Create a DataFrame from mock data for testing without DB2."""
    if table not in MOCK_DATA:
        raise ValueError(f"No mock data defined for table: {table}")
    return spark.createDataFrame(MOCK_DATA[table])


def transform_dataframe(df: "DataFrame", config: TableConfig) -> "DataFrame":
    """
    Transform DataFrame for PostgreSQL compatibility.

    - Convert column names to lowercase
    - Trim whitespace from CHAR fields (optional)

    Args:
        df: Input DataFrame
        config: Table configuration

    Returns:
        Transformed DataFrame
    """
    # Convert column names to lowercase
    for col_name in df.columns:
        df = df.withColumnRenamed(col_name, col_name.lower())

    # Trim whitespace from string columns (CHAR fields have trailing spaces)
    if config.truncate_strings:
        for field in df.schema.fields:
            if field.dataType.typeName() == "string":
                df = df.withColumn(field.name.lower(), trim(col(field.name.lower())))

    return df


def write_to_postgres(df: "DataFrame", table: str, mode: str = "overwrite") -> None:
    """
    Write DataFrame to PostgreSQL via JDBC.

    Args:
        df: DataFrame to write
        table: Target table name
        mode: Write mode (overwrite, append, ignore, error)
    """
    (
        df.write
        .format("jdbc")
        .option("url", PG_CONFIG["url"])
        .option("dbtable", table)
        .option("user", PG_CONFIG["user"])
        .option("password", PG_CONFIG["password"])
        .option("driver", PG_CONFIG["driver"])
        .option("batchsize", PG_CONFIG["batchsize"])
        .mode(mode)
        .save()
    )


def write_to_json(df: "DataFrame", output_path: str) -> None:
    """Write DataFrame to JSON as fallback when PostgreSQL is not available."""
    import shutil
    if os.path.exists(output_path):
        shutil.rmtree(output_path)
    df.coalesce(1).write.mode("overwrite").json(output_path)


def migrate_table(
    spark: SparkSession,
    config: TableConfig,
    use_mock: bool = True,
    target: str = "json"  # "json" or "postgres"
) -> dict:
    """
    Migrate a single table from DB2 to target.

    Args:
        spark: Active Spark session
        config: Table configuration
        use_mock: If True, use mock data instead of DB2 connection
        target: "json" for JSON files, "postgres" for PostgreSQL

    Returns:
        Dictionary with migration results
    """
    result = {
        "name": config.name,
        "source_table": config.source_table,
        "target_table": config.target_table,
        "target_type": target,
        "status": "pending",
        "record_count": 0,
        "error": None,
        "mode": "mock" if use_mock else "jdbc",
    }

    try:
        # Read source data
        if use_mock:
            df = read_mock_data(spark, config.source_table)
        else:
            df = read_from_db2(spark, config.source_table)

        # Transform for PostgreSQL compatibility
        df = transform_dataframe(df, config)

        # Get record count
        record_count = df.count()
        result["record_count"] = record_count

        # Write to target
        if target == "postgres":
            write_to_postgres(df, config.target_table)
            result["output_path"] = f"PostgreSQL: {config.target_table}"
        else:
            output_path = f"db2/output/{config.name}"
            write_to_json(df, output_path)
            result["output_path"] = output_path

        result["status"] = "success"

    except Exception as e:
        result["status"] = "failed"
        result["error"] = str(e)

    return result


def print_summary(results: list[dict]) -> None:
    """Print a summary of all migration results."""
    print("\n" + "=" * 80)
    print("MIGRATION SUMMARY")
    print("=" * 80)

    success_count = sum(1 for r in results if r["status"] == "success")
    failed_count = sum(1 for r in results if r["status"] == "failed")
    total_records = sum(r["record_count"] for r in results)
    mode = results[0]["mode"] if results else "unknown"
    target = results[0]["target_type"] if results else "unknown"

    print(f"\nSource Mode: {'Mock Data' if mode == 'mock' else 'DB2 JDBC'}")
    print(f"Target: {'PostgreSQL' if target == 'postgres' else 'JSON Files'}")
    print(f"\nTotal Tables: {len(results)}")
    print(f"  Successful: {success_count}")
    print(f"  Failed: {failed_count}")
    print(f"\nTotal Records Migrated: {total_records:,}")

    print("\n" + "-" * 80)
    print(f"{'Table':<25} {'Status':<10} {'Records':>10} {'Target'}")
    print("-" * 80)

    for r in results:
        target_info = r.get("output_path", r["target_table"])
        if len(target_info) > 30:
            target_info = target_info[:27] + "..."
        print(f"{r['name']:<25} {r['status']:<10} {r['record_count']:>10,} {target_info}")

    print("-" * 80)


def check_postgres_connection(spark: SparkSession) -> bool:
    """Check if PostgreSQL is accessible."""
    try:
        test_df = spark.read \
            .format("jdbc") \
            .option("url", PG_CONFIG["url"]) \
            .option("query", "SELECT 1") \
            .option("user", PG_CONFIG["user"]) \
            .option("password", PG_CONFIG["password"]) \
            .option("driver", PG_CONFIG["driver"]) \
            .load()
        test_df.collect()
        return True
    except Exception:
        return False


def main():
    """Main execution function."""
    # Determine modes from environment
    use_mock = os.environ.get("DB2_MODE", "mock").lower() == "mock"
    target = os.environ.get("TARGET", "json").lower()  # "json" or "postgres"

    print("=" * 80)
    print("DB2 to PostgreSQL Migration")
    print("=" * 80)
    print(f"\nSource Mode: {'Mock Data (local testing)' if use_mock else 'DB2 JDBC Connection'}")
    print(f"Target: {'PostgreSQL' if target == 'postgres' else 'JSON Files'}")

    if not use_mock:
        print(f"\nDB2 URL: {DB2_CONFIG['url']}")

    if target == "postgres":
        print(f"PostgreSQL URL: {PG_CONFIG['url']}")

    # Collect available JARs
    jars = [path for path in JDBC_JARS.values() if os.path.exists(path)]

    # Initialize Spark
    print("\nInitializing Spark session...")
    spark = create_spark_session(jars=jars)

    # Check PostgreSQL connection if targeting postgres
    if target == "postgres":
        print("Checking PostgreSQL connection...")
        if check_postgres_connection(spark):
            print("  ✓ PostgreSQL connection successful")
        else:
            print("  ✗ PostgreSQL connection failed, falling back to JSON output")
            target = "json"

    # Create output directory for JSON
    if target == "json":
        os.makedirs("db2/output", exist_ok=True)

    results = []

    try:
        print(f"\nMigrating {len(TABLES)} tables...\n")

        for i, config in enumerate(TABLES, 1):
            print(f"[{i}/{len(TABLES)}] Migrating {config.name}...")
            print(f"         Source: {config.source_table}")
            print(f"         Target: {config.target_table}")

            result = migrate_table(spark, config, use_mock=use_mock, target=target)
            results.append(result)

            if result["status"] == "success":
                print(f"         ✓ Success: {result['record_count']:,} records")
            else:
                print(f"         ✗ Failed: {result['error']}")

            print()

        # Print summary
        print_summary(results)

        # Save results manifest
        manifest_path = "db2/output/migration_manifest.json"
        os.makedirs(os.path.dirname(manifest_path), exist_ok=True)
        with open(manifest_path, "w") as f:
            json.dump({
                "tables": results,
                "total_records": sum(r["record_count"] for r in results),
                "source_mode": "mock" if use_mock else "db2",
                "target": target,
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
