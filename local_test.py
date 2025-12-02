"""
Local test script for AWS Glue 5.0 mainframe data migration.

This script mimics the AWS Glue 5.0 runtime environment:
- Spark 3.5.0
- Java 17+ (requires --add-opens JVM flags)
- Cobrix for COBOL/EBCDIC parsing
"""

from pyspark.sql import SparkSession
import os
import shutil

# --- CONFIGURATION ---
COBRIX_PACKAGE = "za.co.absa.cobrix:spark-cobol_2.12:2.6.9"
COPYBOOK_PATH = "input/CUSTREC.cpy"
DATA_PATH = "input/AWS.M2.CARDDEMO.CUSTDATA.PS"
OUTPUT_PATH = "output/customer_json"

# Java 17+ requires "--add-opens" for Spark to work correctly
# These flags allow Spark to access internal Java modules blocked by strict encapsulation
JAVA_17_OPTIONS = " ".join([
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.util=ALL-UNNAMED",
])


def clean_output(path: str) -> None:
    """Remove previous output directory if it exists."""
    if os.path.exists(path):
        shutil.rmtree(path)
        print(f"Cleaned previous output at {path}")


def create_spark_session() -> SparkSession:
    """
    Create a Spark session configured for Glue 5.0 / Java 17 compatibility.

    Uses spark.jars.packages to automatically download Cobrix and all its
    dependencies from Maven Central.
    """
    return (
        SparkSession.builder
        .appName("LocalMainframeTest_Glue5")
        .config("spark.jars.packages", COBRIX_PACKAGE)
        .config("spark.driver.extraJavaOptions", JAVA_17_OPTIONS)
        .config("spark.executor.extraJavaOptions", JAVA_17_OPTIONS)
        .master("local[*]")
        .getOrCreate()
    )


def read_mainframe_data(spark: SparkSession, copybook: str, data: str):
    """
    Read mainframe EBCDIC data using Cobrix.

    Args:
        spark: Active Spark session
        copybook: Path to the COBOL copybook file
        data: Path to the EBCDIC data file

    Returns:
        DataFrame containing parsed mainframe records
    """
    return (
        spark.read
        .format("cobol")
        .option("copybook", copybook)
        .option("record_format", "F")  # Fixed-length records
        .option("schema_retention_policy", "collapse_root")
        .load(data)
    )


def main():
    """Main execution function."""
    # Verify required files exist
    for path in [COPYBOOK_PATH, DATA_PATH]:
        if not os.path.exists(path):
            print(f"ERROR: Required file not found: {path}")
            return 1

    # Clean previous output
    clean_output(OUTPUT_PATH)

    # Initialize Spark
    print("Initializing Spark session (Glue 5.0 compatible)...")
    print(f"Using Cobrix package: {COBRIX_PACKAGE}")
    spark = create_spark_session()

    try:
        # Read mainframe data
        print(f"\nReading data from {DATA_PATH}...")
        print(f"Using copybook: {COPYBOOK_PATH}")

        df = read_mainframe_data(spark, COPYBOOK_PATH, DATA_PATH)

        # Validation
        print("\n" + "=" * 60)
        print("SCHEMA INFERRED FROM COPYBOOK:")
        print("=" * 60)
        df.printSchema()

        record_count = df.count()
        print(f"\nRecord Count: {record_count}")

        print("\n" + "=" * 60)
        print("SAMPLE DATA (first 5 records):")
        print("=" * 60)
        df.show(5, truncate=False)

        # Write to JSON
        print(f"\nWriting to {OUTPUT_PATH}...")
        df.coalesce(1).write.json(OUTPUT_PATH)
        print(f"Successfully converted {record_count} records to JSON!")

        return 0

    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()
        return 1

    finally:
        spark.stop()
        print("\nSpark session stopped.")


if __name__ == "__main__":
    exit(main())
