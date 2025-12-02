"""
File Utilities.

Provides file and directory operation utilities for migrations.

Example:
    >>> from mf_spark.utils import clean_output, ensure_directory
    >>> clean_output("output/customers")
    >>> ensure_directory("output/new_dataset")
"""

import os
import shutil
import hashlib
from pathlib import Path
from typing import Optional
import json


def clean_output(path: str) -> bool:
    """
    Remove output directory if it exists.

    Args:
        path: Path to the directory to remove

    Returns:
        True if directory was removed, False if it didn't exist
    """
    if os.path.exists(path):
        shutil.rmtree(path)
        return True
    return False


def ensure_directory(path: str) -> None:
    """
    Ensure a directory exists, creating it if necessary.

    Args:
        path: Path to the directory
    """
    os.makedirs(path, exist_ok=True)


def get_file_checksum(filepath: str, algorithm: str = "md5") -> str:
    """
    Calculate checksum of a file.

    Args:
        filepath: Path to the file
        algorithm: Hash algorithm ('md5', 'sha1', 'sha256')

    Returns:
        Hexadecimal checksum string
    """
    hash_func = getattr(hashlib, algorithm)()

    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            hash_func.update(chunk)

    return hash_func.hexdigest()


def get_directory_checksum(directory: str, algorithm: str = "md5") -> str:
    """
    Calculate combined checksum of all files in a directory.

    Args:
        directory: Path to the directory
        algorithm: Hash algorithm

    Returns:
        Hexadecimal checksum string
    """
    hash_func = getattr(hashlib, algorithm)()

    for root, dirs, files in sorted(os.walk(directory)):
        for filename in sorted(files):
            filepath = os.path.join(root, filename)
            with open(filepath, "rb") as f:
                for chunk in iter(lambda: f.read(8192), b""):
                    hash_func.update(chunk)

    return hash_func.hexdigest()


def count_json_records(directory: str) -> int:
    """
    Count total records in JSON files in a directory.

    Works with Spark-generated JSON output (one JSON per line).

    Args:
        directory: Path to directory containing JSON files

    Returns:
        Total record count
    """
    count = 0
    for filepath in Path(directory).glob("*.json"):
        with open(filepath, "r") as f:
            for line in f:
                if line.strip():
                    count += 1
    return count


def list_data_files(
    directory: str,
    extensions: Optional[list[str]] = None,
) -> list[str]:
    """
    List data files in a directory.

    Args:
        directory: Path to the directory
        extensions: List of extensions to filter (e.g., ['.ps', '.dat'])

    Returns:
        List of file paths
    """
    extensions = extensions or [".ps", ".dat", ".vsam", ".eb"]
    files = []

    for entry in os.scandir(directory):
        if entry.is_file():
            if extensions is None or any(
                entry.name.lower().endswith(ext.lower()) for ext in extensions
            ):
                files.append(entry.path)

    return sorted(files)


def list_copybooks(directory: str) -> list[str]:
    """
    List copybook files in a directory.

    Args:
        directory: Path to the directory

    Returns:
        List of copybook file paths
    """
    extensions = [".cpy", ".cob", ".cbl", ".copy"]
    return list_data_files(directory, extensions)


def list_ddl_files(directory: str) -> list[str]:
    """
    List DDL files in a directory.

    Args:
        directory: Path to the directory

    Returns:
        List of DDL file paths
    """
    extensions = [".ddl", ".sql"]
    return list_data_files(directory, extensions)


def save_manifest(
    filepath: str,
    data: dict,
    indent: int = 2,
) -> None:
    """
    Save a manifest/metadata file in JSON format.

    Args:
        filepath: Path to the manifest file
        data: Dictionary to save
        indent: JSON indentation level
    """
    with open(filepath, "w") as f:
        json.dump(data, f, indent=indent, default=str)


def load_manifest(filepath: str) -> dict:
    """
    Load a manifest/metadata file.

    Args:
        filepath: Path to the manifest file

    Returns:
        Dictionary with manifest data
    """
    with open(filepath, "r") as f:
        return json.load(f)


def get_file_size(filepath: str) -> int:
    """
    Get file size in bytes.

    Args:
        filepath: Path to the file

    Returns:
        File size in bytes
    """
    return os.path.getsize(filepath)


def format_size(size_bytes: int) -> str:
    """
    Format byte size as human-readable string.

    Args:
        size_bytes: Size in bytes

    Returns:
        Formatted string (e.g., "1.5 MB")
    """
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} PB"


def find_matching_copybook(
    data_file: str,
    copybook_dir: str,
) -> Optional[str]:
    """
    Find a matching copybook for a data file based on naming convention.

    Tries various naming patterns to match data files with copybooks.

    Args:
        data_file: Path to the data file
        copybook_dir: Directory containing copybooks

    Returns:
        Path to matching copybook or None
    """
    data_name = Path(data_file).stem

    # Extract potential copybook name from data file name
    # AWS.M2.CARDDEMO.CUSTDATA.PS -> CUSTDATA
    parts = data_name.split(".")
    potential_names = []

    # Add full name and each part
    potential_names.append(data_name)
    potential_names.extend(parts)

    # Look for matching copybook
    for name in potential_names:
        for ext in [".cpy", ".cob", ".cbl", ".copy"]:
            copybook_path = os.path.join(copybook_dir, f"{name}{ext}")
            if os.path.exists(copybook_path):
                return copybook_path

            # Try uppercase
            copybook_path = os.path.join(copybook_dir, f"{name.upper()}{ext}")
            if os.path.exists(copybook_path):
                return copybook_path

    return None
