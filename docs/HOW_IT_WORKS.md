# How Mainframe Data Migration Works

## Step-by-Step Technical Guide

This document explains the complete data flow from mainframe EBCDIC files to modern JSON format.

---

## Table of Contents

1. [Overview](#1-overview)
2. [Understanding Source Data](#2-understanding-source-data)
3. [Technology Stack](#3-technology-stack)
4. [Spark Session Initialization](#4-spark-session-initialization)
5. [EBCDIC Parsing with Cobrix](#5-ebcdic-parsing-with-cobrix)
6. [Data Type Conversions](#6-data-type-conversions)
7. [JSON Output Generation](#7-json-output-generation)
8. [Full Pipeline Execution](#8-full-pipeline-execution)
9. [Java 17+ Compatibility](#9-java-17-compatibility)
10. [Running the Migration](#10-running-the-migration)

---

## 1. Overview

The migration converts mainframe EBCDIC binary files into human-readable JSON format using Apache Spark and the Cobrix library.

```
┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐
│   EBCDIC FILE    │    │     COPYBOOK     │    │   SPARK+COBRIX   │
│                  │    │                  │    │                  │
│  Binary data     │───▶│  Structure def   │───▶│  Parse & decode  │
│  (not readable)  │    │  (PIC clauses)   │    │  EBCDIC→UTF-8    │
│                  │    │                  │    │                  │
└──────────────────┘    └──────────────────┘    └────────┬─────────┘
                                                         │
                                                         ▼
                                               ┌──────────────────┐
                                               │   JSON OUTPUT    │
                                               │                  │
                                               │  Human-readable  │
                                               │  Ready for apps  │
                                               │                  │
                                               └──────────────────┘
```

---

## 2. Understanding Source Data

### 2.1 EBCDIC Data Files

**EBCDIC** (Extended Binary Coded Decimal Interchange Code) is a character encoding used by IBM mainframes. These files are:

- Binary format (not human-readable)
- Fixed-length records
- Use EBCDIC character encoding (different from ASCII/UTF-8)
- May contain packed decimal (COMP-3) fields

**Example file:**
```
AWS.M2.CARDDEMO.CUSTDATA.PS  ← Binary EBCDIC file (25,000 bytes)
                               Contains 50 records × 500 bytes each
```

If you try to open an EBCDIC file in a text editor, you'll see garbled characters because it's binary data encoded differently than modern systems expect.

### 2.2 COBOL Copybooks

**Copybooks** (.cpy files) are COBOL data structure definitions - like a schema or blueprint that tells us how to interpret the binary data.

**Example copybook (CUSTREC.cpy):**
```cobol
      *****************************************************************
      *    Data-structure for Customer entity (RECLN 500)
      *****************************************************************
       01  CUSTOMER-RECORD.
           05  CUST-ID                    PIC 9(09).
           05  CUST-FIRST-NAME            PIC X(25).
           05  CUST-MIDDLE-NAME           PIC X(25).
           05  CUST-LAST-NAME             PIC X(25).
           05  CUST-ADDR-LINE-1           PIC X(50).
           05  CUST-ADDR-LINE-2           PIC X(50).
           05  CUST-ADDR-LINE-3           PIC X(50).
           05  CUST-ADDR-STATE-CD         PIC X(02).
           05  CUST-ADDR-COUNTRY-CD       PIC X(03).
           05  CUST-ADDR-ZIP              PIC X(10).
           05  CUST-PHONE-NUM-1           PIC X(15).
           05  CUST-PHONE-NUM-2           PIC X(15).
           05  CUST-SSN                   PIC 9(09).
           05  CUST-GOVT-ISSUED-ID        PIC X(20).
           05  CUST-DOB-YYYYMMDD          PIC X(10).
           05  CUST-EFT-ACCOUNT-ID        PIC X(10).
           05  CUST-PRI-CARD-HOLDER-IND   PIC X(01).
           05  CUST-FICO-CREDIT-SCORE     PIC 9(03).
           05  FILLER                     PIC X(168).
```

### 2.3 Understanding PIC Clauses

The `PIC` (Picture) clause defines the data type and size:

| PIC Clause | Meaning | Size | Example Value |
|------------|---------|------|---------------|
| `PIC 9(09)` | 9 numeric digits | 9 bytes | `000000001` |
| `PIC X(25)` | 25 alphanumeric characters | 25 bytes | `Immanuel` |
| `PIC S9(10)V99` | Signed number with 2 decimals | 12 bytes | `-0000012345.67` |
| `PIC X(01)` | Single character | 1 byte | `Y` |

The copybook tells us: "Each record is 500 bytes. Bytes 1-9 are the customer ID, bytes 10-34 are the first name, bytes 35-59 are the middle name..." and so on.

---

## 3. Technology Stack

| Component | Version | Purpose |
|-----------|---------|---------|
| **Python** | 3.10 - 3.11 | Runtime environment |
| **PySpark** | 3.5.0 | Distributed data processing framework |
| **Java** | 17+ (21 tested) | JVM runtime required by Spark |
| **Cobrix** | 2.6.9 | COBOL/EBCDIC parsing library for Spark |
| **UV** | Latest | Fast Python package manager |

### 3.1 Why These Versions?

- **Spark 3.5.0**: Matches AWS Glue 5.0 runtime for production compatibility
- **Java 17+**: Required by Glue 5.0; modern Java with better performance
- **Cobrix 2.6.9**: Latest stable version compiled for Scala 2.12 (Spark 3.5 compatible)
- **UV**: Modern package manager with fast dependency resolution

---

## 4. Spark Session Initialization

### 4.1 Creating the Spark Session

```python
from pyspark.sql import SparkSession

COBRIX_PACKAGE = "za.co.absa.cobrix:spark-cobol_2.12:2.6.9"

JAVA_17_OPTIONS = " ".join([
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.util=ALL-UNNAMED",
])

spark = SparkSession.builder \
    .appName("CardDemo_MainframeMigration") \
    .config("spark.jars.packages", COBRIX_PACKAGE) \
    .config("spark.driver.extraJavaOptions", JAVA_17_OPTIONS) \
    .config("spark.executor.extraJavaOptions", JAVA_17_OPTIONS) \
    .master("local[*]") \
    .getOrCreate()
```

### 4.2 What Happens During Initialization

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        SPARK SESSION INITIALIZATION                         │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  1. DEPENDENCY RESOLUTION                                                   │
│     ├── Connect to Maven Central repository                                 │
│     ├── Download za.co.absa.cobrix:spark-cobol_2.12:2.6.9                  │
│     ├── Download transitive dependencies:                                   │
│     │   ├── cobol-parser_2.12                                              │
│     │   ├── scodec-core_2.12                                               │
│     │   ├── antlr4-runtime                                                 │
│     │   └── slf4j-api                                                      │
│     └── Cache JARs in ~/.ivy2/jars/                                        │
│                                                                             │
│  2. JVM CONFIGURATION                                                       │
│     ├── Apply --add-opens flags for Java 17+ compatibility                 │
│     └── Configure memory and parallelism settings                          │
│                                                                             │
│  3. SPARK CLUSTER START                                                     │
│     ├── Start driver process                                               │
│     ├── Start local executor threads (one per CPU core)                    │
│     └── Initialize Spark context                                           │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 4.3 Configuration Options Explained

| Configuration | Purpose |
|---------------|---------|
| `spark.jars.packages` | Automatically download Cobrix from Maven |
| `spark.driver.extraJavaOptions` | JVM flags for the driver process |
| `spark.executor.extraJavaOptions` | JVM flags for executor processes |
| `master("local[*]")` | Use all CPU cores for local processing |

---

## 5. EBCDIC Parsing with Cobrix

### 5.1 Reading EBCDIC Data

```python
df = spark.read \
    .format("cobol") \
    .option("copybook", "input/CUSTREC.cpy") \
    .option("record_format", "F") \
    .option("schema_retention_policy", "collapse_root") \
    .option("generate_record_id", "true") \
    .load("input/AWS.M2.CARDDEMO.CUSTDATA.PS")
```

### 5.2 Cobrix Options Explained

| Option | Value | Purpose |
|--------|-------|---------|
| `format("cobol")` | - | Use Cobrix reader instead of standard Spark readers |
| `copybook` | Path to .cpy file | COBOL structure definition |
| `record_format` | `"F"` | Fixed-length records (most common) |
| `schema_retention_policy` | `"collapse_root"` | Flatten the top-level record structure |
| `generate_record_id` | `"true"` | Add a sequence number to each record |

### 5.3 What Cobrix Does Internally

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  EBCDIC Binary File (25,000 bytes)                                          │
│  ┌─────────┬─────────┬─────────┬─────────┬─────────┐                       │
│  │ Rec 1   │ Rec 2   │ Rec 3   │ ...     │ Rec 50  │                       │
│  │ 500 B   │ 500 B   │ 500 B   │         │ 500 B   │                       │
│  └─────────┴─────────┴─────────┴─────────┴─────────┘                       │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    │ Step 1: Parse copybook
                                    │ (Extract field definitions)
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  Field Map from Copybook                                                     │
│  ┌──────────────────┬────────┬────────┬──────────────────────────────────┐ │
│  │ Field Name       │ Offset │ Length │ Type                             │ │
│  ├──────────────────┼────────┼────────┼──────────────────────────────────┤ │
│  │ CUST_ID          │ 0      │ 9      │ Numeric (PIC 9(09))              │ │
│  │ CUST_FIRST_NAME  │ 9      │ 25     │ Alphanumeric (PIC X(25))         │ │
│  │ CUST_MIDDLE_NAME │ 34     │ 25     │ Alphanumeric (PIC X(25))         │ │
│  │ CUST_LAST_NAME   │ 59     │ 25     │ Alphanumeric (PIC X(25))         │ │
│  │ ...              │ ...    │ ...    │ ...                              │ │
│  └──────────────────┴────────┴────────┴──────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    │ Step 2: Read binary data
                                    │ (Slice bytes per field map)
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  Raw Field Extraction (still EBCDIC encoded)                                 │
│  Record 1: [0xF0F0F0...] [0xC9D4D4...] [0xD4C1C4...] ...                    │
│             ↓             ↓             ↓                                   │
│             000000001     IMMANUEL      MADELINE                            │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    │ Step 3: Decode EBCDIC → UTF-8
                                    │ (Character set conversion)
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  Spark DataFrame                                                             │
│  ┌─────────┬────────────────┬────────────────┬───────────────┬───────────┐ │
│  │ CUST_ID │ CUST_FIRST_NAME│ CUST_MIDDLE_NAME│ CUST_LAST_NAME│ CUST_SSN │ │
│  ├─────────┼────────────────┼────────────────┼───────────────┼───────────┤ │
│  │ 1       │ Immanuel       │ Madeline       │ Kessler       │ 20973888 │ │
│  │ 2       │ Enrico         │ April          │ Rosenbaum     │ 587518382│ │
│  │ 3       │ Larry          │ Cody           │ Homenick      │ 317460867│ │
│  │ ...     │ ...            │ ...            │ ...           │ ...      │ │
│  └─────────┴────────────────┴────────────────┴───────────────┴───────────┘ │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 5.4 EBCDIC to UTF-8 Character Conversion

EBCDIC uses different byte values than ASCII/UTF-8:

| Character | EBCDIC Hex | ASCII/UTF-8 Hex |
|-----------|------------|-----------------|
| `A` | `0xC1` | `0x41` |
| `B` | `0xC2` | `0x42` |
| `0` | `0xF0` | `0x30` |
| `1` | `0xF1` | `0x31` |
| Space | `0x40` | `0x20` |

Cobrix automatically handles this conversion when reading the data.

---

## 6. Data Type Conversions

### 6.1 COBOL to Spark Type Mapping

| COBOL Definition | COBOL Meaning | Spark Type | Example Input | Example Output |
|------------------|---------------|------------|---------------|----------------|
| `PIC 9(09)` | 9 unsigned digits | IntegerType | `000000001` | `1` |
| `PIC 9(11)` | 11 unsigned digits | LongType | `00000000001` | `1` |
| `PIC X(25)` | 25 characters | StringType | `Immanuel` | `"Immanuel"` |
| `PIC S9(10)V99` | Signed, 2 decimals | DecimalType | `0000012345{` | `12345.67` |
| `PIC 9(04) COMP` | Binary integer | IntegerType | Binary bytes | `1234` |
| `PIC S9(09)V99 COMP-3` | Packed decimal | DecimalType | Packed bytes | `-12345.67` |

### 6.2 Packed Decimal (COMP-3) Explained

COMP-3 stores two digits per byte (plus sign in last nibble):

```
Value: +12345.67

COMP-3 representation (4 bytes):
┌────────┬────────┬────────┬────────┐
│  0x01  │  0x23  │  0x45  │  0x67C │
└────────┴────────┴────────┴────────┘
   1 2      3 4      5 6      7 +

Last nibble: C = positive, D = negative
```

Cobrix automatically unpacks these values.

### 6.3 Field Name Transformation

COBOL field names use hyphens; Spark column names use underscores:

```
CUST-FIRST-NAME  →  CUST_FIRST_NAME
ACCT-CURR-BAL    →  ACCT_CURR_BAL
```

---

## 7. JSON Output Generation

### 7.1 Writing DataFrame to JSON

```python
df.coalesce(1).write.mode("overwrite").json("output/customers")
```

### 7.2 Output Structure

```
output/
└── customers/
    ├── _SUCCESS                                    # Marker file
    └── part-00000-xxxx.json                       # Actual data
```

### 7.3 JSON Record Format

**Input (EBCDIC binary - not readable):**
```
<binary data>
```

**Output (JSON - human readable):**
```json
{
  "CUST_ID": 1,
  "CUST_FIRST_NAME": "Immanuel",
  "CUST_MIDDLE_NAME": "Madeline",
  "CUST_LAST_NAME": "Kessler",
  "CUST_ADDR_LINE_1": "618 Deshaun Route",
  "CUST_ADDR_LINE_2": "Apt. 802",
  "CUST_ADDR_LINE_3": "Altenwerthshire",
  "CUST_ADDR_STATE_CD": "NC",
  "CUST_ADDR_COUNTRY_CD": "USA",
  "CUST_ADDR_ZIP": "12546",
  "CUST_PHONE_NUM_1": "(908)119-8310",
  "CUST_PHONE_NUM_2": "(373)693-8684",
  "CUST_SSN": 20973888,
  "CUST_GOVT_ISSUED_ID": "00000000000049368437",
  "CUST_DOB_YYYYMMDD": "1961-06-08",
  "CUST_EFT_ACCOUNT_ID": "0053581756",
  "CUST_PRI_CARD_HOLDER_IND": "Y",
  "CUST_FICO_CREDIT_SCORE": 274
}
```

---

## 8. Full Pipeline Execution

### 8.1 Pipeline Flow (convert_all.py)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           CONVERSION PIPELINE                                │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  INITIALIZE                                                                 │
│  ├── Create Spark session with Cobrix                                      │
│  ├── Load dataset configurations                                           │
│  └── Create output directory                                               │
│                                                                             │
│  FOR EACH dataset in [customers, accounts, cards, ...]:                     │
│  │                                                                          │
│  │  1. VALIDATE INPUT FILES                                                 │
│  │     ├── Check data file exists: input/AWS.M2.CARDDEMO.CUSTDATA.PS  ✓   │
│  │     └── Check copybook exists:  input/CUSTREC.cpy                   ✓   │
│  │                                                                          │
│  │  2. READ EBCDIC DATA                                                     │
│  │     └── spark.read.format("cobol").option("copybook",...).load(...)     │
│  │                                                                          │
│  │  3. VALIDATE DATA                                                        │
│  │     ├── Print schema                                                    │
│  │     └── Count records: df.count() → 50                                  │
│  │                                                                          │
│  │  4. WRITE JSON OUTPUT                                                    │
│  │     └── df.write.json("output/customers/")                              │
│  │                                                                          │
│  │  5. RECORD RESULT                                                        │
│  │     └── {name: "customers", status: "success", records: 50}             │
│  │                                                                          │
│  END FOR                                                                    │
│                                                                             │
│  FINALIZE                                                                   │
│  ├── Print summary report                                                  │
│  ├── Save manifest: output/conversion_manifest.json                        │
│  └── Stop Spark session                                                    │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 8.2 Dataset Configuration

Each dataset is configured with:

```python
@dataclass
class DatasetConfig:
    name: str           # Logical name (e.g., "customers")
    data_file: str      # EBCDIC file (e.g., "AWS.M2.CARDDEMO.CUSTDATA.PS")
    copybook: str       # Copybook file (e.g., "CUSTREC.cpy")
    record_format: str  # "F" for fixed-length
    description: str    # Human-readable description
    record_length: int  # Expected record size in bytes
```

### 8.3 Conversion Results

| Dataset | Data File | Records | Status |
|---------|-----------|---------|--------|
| customers | CUSTDATA.PS | 50 | ✓ Success |
| accounts | ACCTDATA.PS | 50 | ✓ Success |
| cards | CARDDATA.PS | 50 | ✓ Success |
| card_xref | CARDXREF.PS | 50 | ✓ Success |
| daily_transactions | DALYTRAN.PS | 300 | ✓ Success |
| transaction_types | TRANTYPE.PS | 7 | ✓ Success |
| transaction_categories | TRANCATG.PS | 18 | ✓ Success |
| users | USRSEC.PS | 10 | ✓ Success |
| discount_groups | DISCGRP.PS | 51 | ✓ Success |
| category_balances | TCATBALF.PS | 50 | ✓ Success |
| export_data | EXPORT.DATA.PS | 500 | ✓ Success |
| **Total** | | **1,136** | |

---

## 9. Java 17+ Compatibility

### 9.1 The Problem

Java 17 introduced **strong encapsulation** of internal APIs. By default, it blocks access to internal JDK classes that Spark needs for performance optimizations.

**Without the flags, you get errors like:**
```
java.lang.reflect.InaccessibleObjectException:
Unable to make field private final java.nio.ByteBuffer[]
java.nio.DirectByteBuffer.att accessible:
module java.base does not "opens java.nio" to unnamed module
```

### 9.2 The Solution

Add `--add-opens` flags to allow Spark access to internal modules:

```
--add-opens=java.base/java.nio=ALL-UNNAMED
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED
--add-opens=java.base/java.lang=ALL-UNNAMED
--add-opens=java.base/java.util=ALL-UNNAMED
```

### 9.3 What Each Flag Does

| Flag | Purpose |
|------|---------|
| `java.base/java.nio` | Memory buffer operations (NIO) |
| `java.base/sun.nio.ch` | Low-level I/O channel access |
| `java.base/java.lang` | Reflection on core classes |
| `java.base/java.util` | Collection internals access |

### 9.4 AWS Glue 5.0 Compatibility

In AWS Glue 5.0, these flags are automatically configured by the service. Our local setup adds them explicitly to match the production environment.

---

## 10. Running the Migration

### 10.1 Prerequisites Check

```bash
# Check Java version (must be 17+)
java -version
# Expected: openjdk version "17.x.x" or "21.x.x"

# Check Python version (must be 3.10 or 3.11)
python3 --version
# Expected: Python 3.10.x or 3.11.x

# Check UV is installed
uv --version
```

### 10.2 Installation

```bash
cd mainframe-migration

# Install all dependencies
uv sync
```

### 10.3 Running Conversion

**Convert all datasets:**
```bash
uv run python convert_all.py
```

**Convert single dataset (for testing):**
```bash
uv run python local_test.py
```

### 10.4 Verify Output

```bash
# List converted datasets
ls output/

# View sample customer records
cat output/customers/*.json | head -3

# Check conversion manifest
cat output/conversion_manifest.json
```

### 10.5 Expected Output

```
================================================================================
CardDemo Mainframe Data Migration
EBCDIC to JSON Converter
================================================================================

Environment: AWS Glue 5.0 Compatible (Spark 3.5 / Java 17+)
Cobrix Package: za.co.absa.cobrix:spark-cobol_2.12:2.6.9

Processing 11 datasets...

[1/11] Converting customers...
         ✓ Success: 50 records

[2/11] Converting accounts...
         ✓ Success: 50 records

...

================================================================================
CONVERSION SUMMARY
================================================================================

Total Datasets: 11
  Successful: 11
  Failed: 0
  Skipped: 0

Total Records Converted: 1,136
```

---

## Appendix A: Troubleshooting

### A.1 Common Errors

| Error | Cause | Solution |
|-------|-------|----------|
| `ClassNotFoundException: Cobrix` | JAR not loaded | Ensure internet connectivity for Maven download |
| `IllegalAccessError: module java.base` | Java 17+ restrictions | Verify `--add-opens` flags are set |
| `Record length mismatch` | Copybook doesn't match data | Check RECLN comment in copybook |
| `OutOfMemoryError` | Large dataset | Increase `spark.driver.memory` |

### A.2 Debug Mode

```python
# Enable verbose logging
spark.sparkContext.setLogLevel("DEBUG")
```

---

## Appendix B: Performance Tuning

### B.1 Local Performance

| Setting | Default | Tuned |
|---------|---------|-------|
| `spark.driver.memory` | 1g | 4g |
| `spark.executor.memory` | 1g | 4g |
| `spark.sql.shuffle.partitions` | 200 | 10 |

### B.2 Parallelism

```python
# Use all CPU cores
.master("local[*]")

# Or specify exact count
.master("local[4]")  # 4 cores
```

---

*Document Version: 1.0*
*Last Updated: December 2024*
