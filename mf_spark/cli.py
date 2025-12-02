#!/usr/bin/env python
"""
Mainframe Spark Migration CLI.

Command-line interface for running mainframe data migrations.

Usage:
    python -m mf_spark.cli run                    # Run all datasets
    python -m mf_spark.cli run --dataset customers # Run specific dataset
    python -m mf_spark.cli validate               # Validate configuration
    python -m mf_spark.cli list                   # List configured datasets

Example:
    python -m mf_spark.cli run --input input --output output --format json
"""

import argparse
import sys
import os

from mf_spark.core.migrator import MainframeMigrator
from mf_spark.core.base import OutputFormat
from mf_spark.config.settings import MigrationConfig, get_carddemo_config
from mf_spark.parsers.copybook_parser import CopybookParser
from mf_spark.parsers.ddl_parser import DDLParser


def cmd_run(args):
    """Run the migration."""
    # Build configuration
    if args.config:
        config = MigrationConfig.from_file(args.config)
    else:
        config = get_carddemo_config(
            input_dir=args.input,
            output_dir=args.output,
        )

    # Override settings from command line
    if args.format:
        config.output_format = args.format

    # Validate configuration
    is_valid, errors = config.validate()
    if not is_valid:
        print("Configuration validation failed:")
        for error in errors:
            print(f"  - {error}")
        return 1

    # Run migration
    migrator = MainframeMigrator(config=config)

    output_format = OutputFormat(config.output_format)
    datasets = [args.dataset] if args.dataset else None

    results = migrator.run(datasets=datasets, output_format=output_format)

    # Return exit code based on results
    if migrator.summary.failed > 0:
        return 1
    return 0


def cmd_validate(args):
    """Validate configuration."""
    if args.config:
        config = MigrationConfig.from_file(args.config)
    else:
        config = get_carddemo_config(
            input_dir=args.input,
            output_dir=args.output,
        )

    is_valid, errors = config.validate()

    if is_valid:
        print("Configuration is valid!")
        print(f"\nDatasets configured: {len(config.datasets)}")
        for ds in config.datasets:
            status = "enabled" if ds.enabled else "disabled"
            print(f"  - {ds.name} ({status})")
        return 0
    else:
        print("Configuration validation failed:")
        for error in errors:
            print(f"  - {error}")
        return 1


def cmd_list(args):
    """List configured datasets."""
    if args.config:
        config = MigrationConfig.from_file(args.config)
    else:
        config = get_carddemo_config(
            input_dir=args.input,
            output_dir=args.output,
        )

    print("=" * 70)
    print("CONFIGURED DATASETS")
    print("=" * 70)
    print(f"{'Name':<25} {'Type':<8} {'File/Table':<30}")
    print("-" * 70)

    for ds in config.datasets:
        if ds.data_file:
            ds_type = "VSAM"
            source = ds.data_file[:30]
        elif ds.source_table:
            ds_type = "DB2"
            source = ds.source_table[:30]
        else:
            ds_type = "?"
            source = "-"

        status = "" if ds.enabled else " (disabled)"
        print(f"{ds.name:<25} {ds_type:<8} {source:<30}{status}")

    print("-" * 70)
    print(f"Total: {len(config.datasets)} datasets")


def cmd_parse_copybook(args):
    """Parse and display a copybook."""
    parser = CopybookParser(ignore_fillers=args.no_fillers)

    try:
        fields = parser.parse_file(args.file)
        print(parser.print_layout(fields))

        if args.schema:
            schema = parser.to_spark_schema(fields)
            print("\nSpark Schema:")
            for field in schema.fields:
                print(f"  {field.name}: {field.dataType}")

    except FileNotFoundError as e:
        print(f"Error: {e}")
        return 1

    return 0


def cmd_parse_ddl(args):
    """Parse and display a DDL file."""
    parser = DDLParser()

    try:
        table = parser.parse_file(args.file)
        print(parser.print_table_definition(table))

        if args.schema:
            schema = parser.to_spark_schema(table)
            print("\nSpark Schema:")
            for field in schema.fields:
                print(f"  {field.name}: {field.dataType}")

    except FileNotFoundError as e:
        print(f"Error: {e}")
        return 1
    except ValueError as e:
        print(f"Parse error: {e}")
        return 1

    return 0


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Mainframe Spark Migration CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Run command
    run_parser = subparsers.add_parser("run", help="Run migration")
    run_parser.add_argument("--input", "-i", default="input", help="Input directory")
    run_parser.add_argument("--output", "-o", default="output", help="Output directory")
    run_parser.add_argument("--config", "-c", help="Configuration file (JSON)")
    run_parser.add_argument("--dataset", "-d", help="Specific dataset to process")
    run_parser.add_argument(
        "--format", "-f",
        choices=["json", "parquet", "csv"],
        default="json",
        help="Output format"
    )
    run_parser.set_defaults(func=cmd_run)

    # Validate command
    validate_parser = subparsers.add_parser("validate", help="Validate configuration")
    validate_parser.add_argument("--input", "-i", default="input", help="Input directory")
    validate_parser.add_argument("--output", "-o", default="output", help="Output directory")
    validate_parser.add_argument("--config", "-c", help="Configuration file (JSON)")
    validate_parser.set_defaults(func=cmd_validate)

    # List command
    list_parser = subparsers.add_parser("list", help="List configured datasets")
    list_parser.add_argument("--input", "-i", default="input", help="Input directory")
    list_parser.add_argument("--output", "-o", default="output", help="Output directory")
    list_parser.add_argument("--config", "-c", help="Configuration file (JSON)")
    list_parser.set_defaults(func=cmd_list)

    # Parse copybook command
    copybook_parser = subparsers.add_parser("parse-copybook", help="Parse a copybook")
    copybook_parser.add_argument("file", help="Copybook file to parse")
    copybook_parser.add_argument("--no-fillers", action="store_true", help="Exclude FILLER fields")
    copybook_parser.add_argument("--schema", action="store_true", help="Show Spark schema")
    copybook_parser.set_defaults(func=cmd_parse_copybook)

    # Parse DDL command
    ddl_parser = subparsers.add_parser("parse-ddl", help="Parse a DDL file")
    ddl_parser.add_argument("file", help="DDL file to parse")
    ddl_parser.add_argument("--schema", action="store_true", help="Show Spark schema")
    ddl_parser.set_defaults(func=cmd_parse_ddl)

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        return 0

    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
