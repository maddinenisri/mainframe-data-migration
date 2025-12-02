"""
Comprehensive EBCDIC to JSON converter for AWS CardDemo mainframe data.

This script converts all mainframe EBCDIC data files to JSON format using
Apache Spark and Cobrix, mimicking AWS Glue 5.0 runtime environment.

Runtime Environment:
- Spark 3.5.0
- Java 17+ (requires --add-opens JVM flags)
- Cobrix 2.6.9 for COBOL/EBCDIC parsing
"""

from pyspark.sql import SparkSession
from dataclasses import dataclass
from typing import Optional
import os
import shutil
import json
import sys

# --- CONFIGURATION ---
COBRIX_PACKAGE = "za.co.absa.cobrix:spark-cobol_2.12:2.6.9"
INPUT_DIR = "input"
OUTPUT_DIR = "output"

# Java 17+ requires "--add-opens" for Spark to work correctly
JAVA_17_OPTIONS = " ".join([
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.util=ALL-UNNAMED",
])


@dataclass
class DatasetConfig:
    """Configuration for a mainframe dataset conversion."""
    name: str                    # Logical name for the dataset
    data_file: str              # EBCDIC data file name
    copybook: str               # Copybook file name
    record_format: str = "F"    # F=Fixed, V=Variable, VB=Variable Blocked
    description: str = ""       # Dataset description
    record_length: Optional[int] = None  # Expected record length


# Dataset mappings based on CardDemo application analysis
DATASETS = [
    DatasetConfig(
        name="customers",
        data_file="AWS.M2.CARDDEMO.CUSTDATA.PS",
        copybook="CUSTREC.cpy",
        description="Customer master data with demographics and contact info",
        record_length=500,
    ),
    DatasetConfig(
        name="accounts",
        data_file="AWS.M2.CARDDEMO.ACCTDATA.PS",
        copybook="CVACT01Y.cpy",
        description="Account records with balances and credit limits",
        record_length=300,
    ),
    DatasetConfig(
        name="cards",
        data_file="AWS.M2.CARDDEMO.CARDDATA.PS",
        copybook="CVACT02Y.cpy",
        description="Credit card information with CVV and expiration",
        record_length=150,
    ),
    DatasetConfig(
        name="card_xref",
        data_file="AWS.M2.CARDDEMO.CARDXREF.PS",
        copybook="CVACT03Y.cpy",
        description="Card to customer/account cross-reference",
        record_length=50,
    ),
    DatasetConfig(
        name="daily_transactions",
        data_file="AWS.M2.CARDDEMO.DALYTRAN.PS",
        copybook="CVTRA06Y.cpy",
        description="Daily transaction records with merchant details",
        record_length=350,
    ),
    DatasetConfig(
        name="transaction_types",
        data_file="AWS.M2.CARDDEMO.TRANTYPE.PS",
        copybook="CVTRA03Y.cpy",
        description="Transaction type reference data",
        record_length=60,
    ),
    DatasetConfig(
        name="transaction_categories",
        data_file="AWS.M2.CARDDEMO.TRANCATG.PS",
        copybook="CVTRA04Y.cpy",
        description="Transaction category reference data",
        record_length=60,
    ),
    DatasetConfig(
        name="users",
        data_file="AWS.M2.CARDDEMO.USRSEC.PS",
        copybook="CSUSR01Y.cpy",
        description="User security and authentication data",
        record_length=80,
    ),
    DatasetConfig(
        name="discount_groups",
        data_file="AWS.M2.CARDDEMO.DISCGRP.PS",
        copybook="DISCGRP.cpy",
        description="Discount group configuration by transaction type",
        record_length=50,
    ),
    DatasetConfig(
        name="category_balances",
        data_file="AWS.M2.CARDDEMO.TCATBALF.PS",
        copybook="TCATBALF.cpy",
        description="Transaction category balance summaries",
        record_length=50,
    ),
    DatasetConfig(
        name="export_data",
        data_file="AWS.M2.CARDDEMO.EXPORT.DATA.PS",
        copybook="CVEXPORT.cpy",
        description="Multi-record export file for branch migration",
        record_length=500,
    ),
]


def clean_output(path: str) -> None:
    """Remove previous output directory if it exists."""
    if os.path.exists(path):
        shutil.rmtree(path)


def create_spark_session() -> SparkSession:
    """Create a Spark session configured for Glue 5.0 / Java 17 compatibility."""
    return (
        SparkSession.builder
        .appName("CardDemo_MainframeMigration")
        .config("spark.jars.packages", COBRIX_PACKAGE)
        .config("spark.driver.extraJavaOptions", JAVA_17_OPTIONS)
        .config("spark.executor.extraJavaOptions", JAVA_17_OPTIONS)
        .config("spark.sql.session.timeZone", "UTC")
        .master("local[*]")
        .getOrCreate()
    )


def convert_dataset(spark: SparkSession, config: DatasetConfig) -> dict:
    """
    Convert a single mainframe dataset to JSON.

    Args:
        spark: Active Spark session
        config: Dataset configuration

    Returns:
        Dictionary with conversion results
    """
    data_path = os.path.join(INPUT_DIR, config.data_file)
    copybook_path = os.path.join(INPUT_DIR, config.copybook)
    output_path = os.path.join(OUTPUT_DIR, config.name)

    result = {
        "name": config.name,
        "data_file": config.data_file,
        "copybook": config.copybook,
        "status": "pending",
        "record_count": 0,
        "error": None,
    }

    # Check if files exist
    if not os.path.exists(data_path):
        result["status"] = "skipped"
        result["error"] = f"Data file not found: {data_path}"
        return result

    if not os.path.exists(copybook_path):
        result["status"] = "skipped"
        result["error"] = f"Copybook not found: {copybook_path}"
        return result

    try:
        # Clean previous output
        clean_output(output_path)

        # Read the EBCDIC data using Cobrix
        df = (
            spark.read
            .format("cobol")
            .option("copybook", copybook_path)
            .option("record_format", config.record_format)
            .option("schema_retention_policy", "collapse_root")
            .option("generate_record_id", "true")
            .load(data_path)
        )

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
    """Print a summary of all conversion results."""
    print("\n" + "=" * 80)
    print("CONVERSION SUMMARY")
    print("=" * 80)

    success_count = sum(1 for r in results if r["status"] == "success")
    failed_count = sum(1 for r in results if r["status"] == "failed")
    skipped_count = sum(1 for r in results if r["status"] == "skipped")
    total_records = sum(r["record_count"] for r in results)

    print(f"\nTotal Datasets: {len(results)}")
    print(f"  Successful: {success_count}")
    print(f"  Failed: {failed_count}")
    print(f"  Skipped: {skipped_count}")
    print(f"\nTotal Records Converted: {total_records:,}")

    print("\n" + "-" * 80)
    print(f"{'Dataset':<25} {'Status':<10} {'Records':>10} {'Notes'}")
    print("-" * 80)

    for r in results:
        notes = r.get("error", "") or ""
        if len(notes) > 35:
            notes = notes[:32] + "..."
        print(f"{r['name']:<25} {r['status']:<10} {r['record_count']:>10,} {notes}")

    print("-" * 80)


def main():
    """Main execution function."""
    print("=" * 80)
    print("CardDemo Mainframe Data Migration")
    print("EBCDIC to JSON Converter")
    print("=" * 80)
    print(f"\nEnvironment: AWS Glue 5.0 Compatible (Spark 3.5 / Java 17+)")
    print(f"Cobrix Package: {COBRIX_PACKAGE}")
    print(f"\nInput Directory: {INPUT_DIR}")
    print(f"Output Directory: {OUTPUT_DIR}")

    # Create output directory
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Initialize Spark
    print("\nInitializing Spark session...")
    spark = create_spark_session()

    results = []

    try:
        print(f"\nProcessing {len(DATASETS)} datasets...\n")

        for i, config in enumerate(DATASETS, 1):
            print(f"[{i}/{len(DATASETS)}] Converting {config.name}...")
            print(f"         Data: {config.data_file}")
            print(f"         Copybook: {config.copybook}")

            result = convert_dataset(spark, config)
            results.append(result)

            if result["status"] == "success":
                print(f"         ✓ Success: {result['record_count']:,} records")
            elif result["status"] == "skipped":
                print(f"         ⊘ Skipped: {result['error']}")
            else:
                print(f"         ✗ Failed: {result['error']}")

            print()

        # Print summary
        print_summary(results)

        # Save results manifest
        manifest_path = os.path.join(OUTPUT_DIR, "conversion_manifest.json")
        with open(manifest_path, "w") as f:
            json.dump({
                "datasets": results,
                "total_records": sum(r["record_count"] for r in results),
                "cobrix_version": "2.6.9",
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
