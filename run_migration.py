#!/usr/bin/env python
"""
Mainframe Migration Runner.

Main entry point for running mainframe data migrations using the
mf_spark modular framework.

This script:
1. Initializes the migration framework
2. Converts all VSAM/EBCDIC files to JSON using Cobrix
3. Validates the output
4. Generates a manifest file

Usage:
    python run_migration.py                    # Run all datasets
    python run_migration.py --dataset customers # Run specific dataset
    python run_migration.py --clean            # Clean output before running

Example:
    uv run python run_migration.py
"""

import sys
import argparse
import shutil
import os

# Add the project root to the path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from mf_spark import MainframeMigrator, MigrationConfig
from mf_spark.core.base import OutputFormat
from mf_spark.config.settings import get_carddemo_config


def clean_output_directory(output_dir: str) -> None:
    """Clean the entire output directory."""
    if os.path.exists(output_dir):
        print(f"Cleaning output directory: {output_dir}")
        shutil.rmtree(output_dir)
        print("Output directory cleaned.")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Mainframe Migration Runner using mf_spark framework"
    )
    parser.add_argument(
        "--input", "-i",
        default="input",
        help="Input directory containing data files and copybooks"
    )
    parser.add_argument(
        "--output", "-o",
        default="output",
        help="Output directory for converted JSON files"
    )
    parser.add_argument(
        "--dataset", "-d",
        help="Specific dataset to process (default: all)"
    )
    parser.add_argument(
        "--format", "-f",
        choices=["json", "parquet", "csv"],
        default="json",
        help="Output format (default: json)"
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Clean output directory before running"
    )
    parser.add_argument(
        "--validate",
        action="store_true",
        default=True,
        help="Validate output after conversion"
    )

    args = parser.parse_args()

    # Clean output if requested
    if args.clean:
        clean_output_directory(args.output)

    # Build configuration using CardDemo defaults
    config = get_carddemo_config(
        input_dir=args.input,
        output_dir=args.output,
    )
    config.output_format = args.format
    config.validate_output = args.validate

    # Validate configuration
    is_valid, errors = config.validate()
    if not is_valid:
        print("\nConfiguration validation failed:")
        for error in errors:
            print(f"  - {error}")
        return 1

    # Create migrator and run
    migrator = MainframeMigrator(config=config)

    # Determine output format
    format_map = {"json": OutputFormat.JSON, "parquet": OutputFormat.PARQUET, "csv": OutputFormat.CSV}
    output_format = format_map.get(args.format.lower(), OutputFormat.JSON)

    # Run migration
    datasets = [args.dataset] if args.dataset else None
    results = migrator.run(datasets=datasets, output_format=output_format)

    # Return exit code based on results
    summary = migrator.get_summary()
    if summary.failed > 0:
        print(f"\n*** Migration completed with {summary.failed} failures ***")
        return 1

    print(f"\n*** Migration completed successfully! ***")
    print(f"Total records converted: {summary.total_records:,}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
