# Mainframe to DynamoDB Migration

> **Modernizing Mission-Critical Data**: A pattern for migrating mainframe assets to AWS DynamoDB via Glue & PySpark

Local testing environment for converting AWS CardDemo EBCDIC mainframe data to JSON format, mimicking AWS Glue 5.0 runtime.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Data Pipeline](#data-pipeline)
- [Migration Strategy](#migration-strategy)
- [Quick Start](#quick-start)
- [Dataset Reference](#dataset-reference)
- [Business Value](#business-value)
- [Troubleshooting](#troubleshooting)

---

## Overview

### The Challenge: Data Silos

| Constraint | Impact |
|------------|--------|
| **Legacy Format** | Data locked in proprietary binary formats (EBCDIC, VSAM) |
| **Complexity** | Requires COBOL knowledge just to read the data |
| **Cost** | MIPS compute costs consumed on every query |
| **Rigidity** | Adding a field requires recompiling and potential downtime |

### The Solution: Automated Transformation Pipeline

| Component | Technology | Purpose |
|-----------|------------|---------|
| **ETL Engine** | AWS Glue 5.0 | Serverless data processing |
| **Processing** | Apache Spark + PySpark | Distributed computing |
| **Parser** | Cobrix 2.6.9 | EBCDIC/COBOL Copybook handling |
| **Target** | Amazon DynamoDB | NoSQL data store |
| **Intermediate** | JSON | Universal data format |

**Key Innovation**: We use the original COBOL Copybooks to drive the transformation, ensuring 100% accuracy without manual field mapping.

---

## Architecture

### High-Level System Architecture

```mermaid
flowchart TB
    subgraph Mainframe["Legacy Mainframe Environment"]
        direction TB
        VSAM[("VSAM Files<br/>EBCDIC Binary")]
        DB2[("DB2 Tables<br/>Relational Data")]
        COPYBOOKS["COBOL Copybooks<br/>Schema Definitions"]
    end

    subgraph Ingestion["Data Ingestion Layer"]
        direction TB
        S3RAW[("S3 Raw Zone<br/>Binary Files")]
        S3CPY[("S3 Schema Zone<br/>Copybooks")]
    end

    subgraph Processing["AWS Glue Processing"]
        direction TB
        GLUE["AWS Glue Job<br/>PySpark Runtime"]
        COBRIX["Cobrix Parser<br/>EBCDIC Decoder"]
        TRANSFORM["Data Transformer<br/>JSON Generator"]
    end

    subgraph Staging["Staging Layer"]
        direction TB
        S3JSON[("S3 Processed Zone<br/>JSON Files")]
        MANIFEST["Conversion Manifest<br/>Audit Trail"]
        CRC["CRC Checksums<br/>Integrity Validation"]
    end

    subgraph Target["Target Platform"]
        direction TB
        DYNAMO[("Amazon DynamoDB<br/>NoSQL Store")]
        OPENSEARCH[("OpenSearch<br/>Search & Analytics")]
        APPS["Modern Applications<br/>Web, Mobile, API"]
    end

    VSAM -->|"FTP/SFTP"| S3RAW
    DB2 -->|"Export"| S3RAW
    COPYBOOKS -->|"Upload"| S3CPY

    S3RAW --> GLUE
    S3CPY --> GLUE
    GLUE --> COBRIX
    COBRIX --> TRANSFORM
    TRANSFORM --> S3JSON
    TRANSFORM --> MANIFEST
    TRANSFORM --> CRC

    S3JSON --> DYNAMO
    S3JSON --> OPENSEARCH
    DYNAMO --> APPS
    OPENSEARCH --> APPS

    style VSAM fill:#ff922b,stroke:#333,color:#fff
    style DB2 fill:#ff922b,stroke:#333,color:#fff
    style DYNAMO fill:#339af0,stroke:#333,color:#fff
    style OPENSEARCH fill:#339af0,stroke:#333,color:#fff
    style GLUE fill:#20c997,stroke:#333,color:#fff
    style S3JSON fill:#51cf66,stroke:#333,color:#fff
```

### Component Interaction Flow

```mermaid
sequenceDiagram
    participant MF as Mainframe
    participant S3 as S3 Bucket
    participant Glue as AWS Glue
    participant Spark as PySpark
    participant Cobrix as Cobrix Parser
    participant DDB as DynamoDB

    MF->>S3: Upload VSAM Files (.PS)
    MF->>S3: Upload Copybooks (.cpy)
    MF->>S3: Export DB2 Tables

    S3->>Glue: Trigger Job on S3 Event
    Glue->>Spark: Initialize Spark Session
    Spark->>Cobrix: Load Copybook Schema
    Spark->>Cobrix: Read EBCDIC Binary

    loop For Each Record
        Cobrix->>Cobrix: Decode Packed Decimal
        Cobrix->>Cobrix: Convert EBCDIC to ASCII
        Cobrix->>Spark: Return Structured Row
    end

    Spark->>S3: Write JSON with CRC
    Spark->>S3: Write Manifest
    S3->>DDB: Load JSON Documents

    Note over MF,DDB: End-to-End Pipeline Complete
```

---

## Data Pipeline

### VSAM to JSON Conversion Pipeline

```mermaid
flowchart LR
    subgraph Source["Source Files"]
        direction TB
        PS["AWS.M2.CARDDEMO<br/>CUSTDATA.PS<br/>Binary EBCDIC"]
        CPY["CUSTREC.cpy<br/>COBOL Schema"]
    end

    subgraph Engine["convert_all.py"]
        direction TB
        MATCH["Schema Matcher<br/>Auto-pair Data + Copybook"]
        PARSE["EBCDIC Parser<br/>Packed Decimal Handler"]
        TRANSFORM["JSON Generator<br/>Field Mapping"]
    end

    subgraph Output["Output Files"]
        direction TB
        JSON["customers/<br/>part-00000.json"]
        MANIFEST["conversion_manifest.json<br/>Audit Log"]
        SUCCESS["._SUCCESS.crc<br/>Integrity Check"]
    end

    PS --> MATCH
    CPY --> MATCH
    MATCH --> PARSE
    PARSE --> TRANSFORM
    TRANSFORM --> JSON
    TRANSFORM --> MANIFEST
    TRANSFORM --> SUCCESS

    style PS fill:#ff922b,stroke:#333,color:#fff
    style JSON fill:#51cf66,stroke:#333,color:#fff
    style SUCCESS fill:#339af0,stroke:#333,color:#fff
```

### DB2 to JSON Conversion Pipeline

```mermaid
flowchart LR
    subgraph DB2Source["DB2 Source"]
        direction TB
        DDL["TRNTYPE.ddl<br/>Table Schema"]
        TABLE[("CARDDEMO<br/>TRANSACTION_TYPE")]
    end

    subgraph DB2Engine["db2_to_json.py"]
        direction TB
        DDL_PARSE["DDL Parser<br/>Schema Extraction"]
        JDBC["JDBC Reader<br/>or Mock Mode"]
        NORMALIZE["Column Normalizer<br/>Lowercase Names"]
    end

    subgraph DB2Output["Unified Output"]
        direction TB
        TRTYPE["transaction_type/<br/>part-00000.json"]
        TRCAT["transaction_category/<br/>part-00000.json"]
    end

    DDL --> DDL_PARSE
    TABLE --> JDBC
    DDL_PARSE --> NORMALIZE
    JDBC --> NORMALIZE
    NORMALIZE --> TRTYPE
    NORMALIZE --> TRCAT

    style TABLE fill:#ff922b,stroke:#333,color:#fff
    style TRTYPE fill:#51cf66,stroke:#333,color:#fff
    style TRCAT fill:#51cf66,stroke:#333,color:#fff
```

### Data Transformation Detail

```mermaid
flowchart TB
    subgraph Input["Binary Input"]
        HEX["F0F0F0F0F0F0F0F0F1<br/>EBCDIC Encoded Bytes"]
        PACKED["Packed Decimal<br/>COMP-3 Format"]
    end

    subgraph Cobrix["Cobrix Processing"]
        EBCDIC_DEC["EBCDIC Decoder<br/>Character Translation"]
        COMP3_DEC["COMP-3 Decoder<br/>Numeric Extraction"]
        SCHEMA_APP["Schema Application<br/>From Copybook"]
    end

    subgraph Output["JSON Output"]
        JSON_DOC["Clean JSON Document"]
    end

    HEX --> EBCDIC_DEC
    PACKED --> COMP3_DEC
    EBCDIC_DEC --> SCHEMA_APP
    COMP3_DEC --> SCHEMA_APP
    SCHEMA_APP --> JSON_DOC

    style HEX fill:#868e96,stroke:#333,color:#fff
    style JSON_DOC fill:#51cf66,stroke:#333,color:#fff
```

**Before (COBOL Copybook)**:
```cobol
01  CUSTOMER-RECORD.
    05  CUST-ID              PIC 9(09).
    05  CUST-FIRST-NAME      PIC X(25).
    05  CUST-LAST-NAME       PIC X(25).
    05  CUST-FICO-SCORE      PIC 9(03).
```

**After (JSON Output)**:
```json
{
  "CUST_ID": 1,
  "CUST_FIRST_NAME": "Immanuel",
  "CUST_LAST_NAME": "Kessler",
  "CUST_FICO_CREDIT_SCORE": 274
}
```

---

## Migration Strategy

### Incremental Migration Phases

```mermaid
flowchart TB
    subgraph Phase1["Phase 1: Read-Only Replica"]
        direction LR
        MF1[("Mainframe<br/>System of Record")] --> DDB1[("DynamoDB<br/>Read Replica")]
        MF1 --> LEGACY["Legacy Apps<br/>Read from MF"]
        DDB1 --> NEW["New Apps<br/>Read from DDB"]
    end

    subgraph Phase2["Phase 2: Parallel Run"]
        direction LR
        APP2["Applications"] --> MF2[("Mainframe")]
        APP2 --> DDB2[("DynamoDB")]
        MF2 <-.->|"Compare"| DDB2
    end

    subgraph Phase3["Phase 3: Cutover"]
        direction LR
        MF3[("Mainframe<br/>Batch Only")] -->|"Nightly Sync"| DDB3[("DynamoDB<br/>System of Record")]
        DDB3 --> ALL["All Applications"]
    end

    Phase1 --> Phase2
    Phase2 --> Phase3

    style MF1 fill:#ff922b,stroke:#333,color:#fff
    style MF3 fill:#ffd43b,stroke:#333,color:#000
    style DDB3 fill:#339af0,stroke:#333,color:#fff
```

### Phase Details

| Phase | Description | Risk Level | Status |
|-------|-------------|------------|--------|
| **Phase 1** | Read-Only Replica - New apps read from DynamoDB, legacy unchanged | Near Zero | Current Focus |
| **Phase 2** | Parallel Run - Writes to both systems, compare for consistency | Low | Planned |
| **Phase 3** | Cutover - DynamoDB becomes System of Record | Medium | Future |

### Risk Mitigation

```mermaid
flowchart LR
    subgraph Risks["Potential Risks"]
        R1["Data Integrity"]
        R2["Schema Changes"]
        R3["Performance"]
        R4["Complex Relationships"]
    end

    subgraph Mitigations["Mitigation Controls"]
        M1["CRC Checksums<br/>100% Transfer Accuracy"]
        M2["Schema-on-Read<br/>Dynamic Copybook Loading"]
        M3["PySpark Distribution<br/>Process TBs in Minutes"]
        M4["Denormalization<br/>Single Table Design"]
    end

    R1 --> M1
    R2 --> M2
    R3 --> M3
    R4 --> M4

    style R1 fill:#ff6b6b,stroke:#333,color:#fff
    style R2 fill:#ff6b6b,stroke:#333,color:#fff
    style R3 fill:#ff6b6b,stroke:#333,color:#fff
    style R4 fill:#ff6b6b,stroke:#333,color:#fff
    style M1 fill:#51cf66,stroke:#333,color:#fff
    style M2 fill:#51cf66,stroke:#333,color:#fff
    style M3 fill:#51cf66,stroke:#333,color:#fff
    style M4 fill:#51cf66,stroke:#333,color:#fff
```

---

## Quick Start

### Prerequisites

1. **Java 17+** (21 recommended)
   ```bash
   java -version
   # Should show: openjdk version "17.x.x" or higher
   ```

2. **Python 3.10 or 3.11**
   ```bash
   python3 --version
   # Should show: Python 3.10.x or 3.11.x
   ```

3. **UV Package Manager**
   ```bash
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```

### Installation

```bash
cd mainframe-migration
uv sync
```

### Run Conversion

```bash
# Convert all VSAM datasets
uv run python convert_all.py

# Convert DB2 tables
uv run python db2/scripts/db2_to_json.py

# Quick test (customers only)
uv run python local_test.py
```

### Verify Output

```bash
# Check conversion results
cat output/conversion_manifest.json

# View sample customer record
head -1 output/customers/*.json | python -m json.tool
```

---

## Dataset Reference

### VSAM Datasets (Converted)

| Dataset | Source File | Copybook | Records | Description |
|---------|-------------|----------|---------|-------------|
| customers | CUSTDATA.PS | CUSTREC.cpy | 50 | Customer demographics |
| accounts | ACCTDATA.PS | CVACT01Y.cpy | 50 | Account balances & limits |
| cards | CARDDATA.PS | CVACT02Y.cpy | 50 | Credit card details |
| card_xref | CARDXREF.PS | CVACT03Y.cpy | 50 | Card-customer mapping |
| daily_transactions | DALYTRAN.PS | CVTRA06Y.cpy | 300 | Transaction history |
| transaction_types | TRANTYPE.PS | CVTRA03Y.cpy | 7 | Type reference data |
| transaction_categories | TRANCATG.PS | CVTRA04Y.cpy | 18 | Category reference |
| users | USRSEC.PS | CSUSR01Y.cpy | 10 | User credentials |
| discount_groups | DISCGRP.PS | DISCGRP.cpy | 51 | Discount configuration |
| category_balances | TCATBALF.PS | TCATBALF.cpy | 50 | Category summaries |
| export_data | EXPORT.DATA.PS | CVEXPORT.cpy | 500 | Multi-record export |

### DB2 Tables (Converted)

| Table | Source | Records | Description |
|-------|--------|---------|-------------|
| transaction_type | CARDDEMO.TRANSACTION_TYPE | 7 | Transaction type codes |
| transaction_type_category | CARDDEMO.TRANSACTION_TYPE_CATEGORY | 18 | Category mappings |

### Conversion Summary

```
Total Datasets: 13
Total Records: 1,161
Success Rate: 100%
Output Format: JSON (DynamoDB-ready)
```

---

## Business Value

### ROI Summary

| Benefit | Impact |
|---------|--------|
| **Cost Reduction** | Offload read-heavy queries from Mainframe (MIPS savings) to DynamoDB (Pay-per-request) |
| **Agility** | New products launch in days using JSON APIs, vs months of COBOL development |
| **Data Democratization** | Data in standard JSON format readable by Data Lakes, AI/ML tools, Analytics dashboards |
| **Talent Pool** | Python/Cloud skills widely available vs scarce COBOL specialists |

### Technical Advantages

```mermaid
mindmap
    root((Migration Benefits))
        Cost
            Reduced MIPS
            Pay-per-use
            No License Fees
        Speed
            Sub-ms Queries
            Auto-scaling
            Global Distribution
        Flexibility
            Schema-on-Read
            JSON Format
            Any Consumer
        Safety
            CRC Validation
            Audit Trail
            Rollback Ready
```

---

## Project Structure

```
mainframe-migration/
├── input/                      # Source files
│   ├── *.PS                    # EBCDIC binary data files
│   └── *.cpy                   # COBOL copybook schemas
├── output/                     # Converted JSON output
│   ├── customers/
│   ├── accounts/
│   ├── cards/
│   ├── daily_transactions/
│   └── conversion_manifest.json
├── db2/                        # DB2 migration
│   ├── schema/source/          # DDL definitions
│   ├── scripts/                # Conversion scripts
│   └── output/                 # DB2 JSON output
├── docs/                       # Documentation
│   ├── DESIGN.md               # Technical design
│   └── executive-presentation.html
├── convert_all.py              # Main VSAM conversion script
├── local_test.py               # Quick test script
├── pyproject.toml              # Python dependencies
└── README.md                   # This file
```

---

## Troubleshooting

### Common Issues

| Issue | Solution |
|-------|----------|
| `ClassNotFoundException: za.co.absa.cobrix` | Cobrix JAR downloads automatically. Check internet connectivity. |
| `IllegalAccessError: module java.base` | Java 17+ requires `--add-opens` flags. Already configured in scripts. |
| `Record length mismatch` | Verify copybook matches data file. Check RECLN comments. |

### Verify Environment

```bash
# Check Java version (must be 17+)
java -version

# Check Python version
python3 --version

# Clean rebuild
rm -rf .venv output uv.lock
uv sync
uv run python convert_all.py
```

---

## AWS Glue Deployment

For production deployment to AWS Glue 5.0:

1. Upload `convert_all.py` to S3
2. Upload Cobrix JAR to S3
3. Create Glue job with:
   - Glue Version: 5.0
   - Worker Type: G.1X or higher
   - Extra JARs: `s3://bucket/spark-cobol_2.12-2.6.9.jar`

See [docs/DESIGN.md](docs/DESIGN.md) for detailed production guide.

---

## References

- [Cobrix - COBOL Data Source for Spark](https://github.com/AbsaOSS/cobrix)
- [AWS Glue Documentation](https://docs.aws.amazon.com/glue/)
- [AWS CardDemo Application](https://github.com/aws-samples/aws-mainframe-modernization-carddemo)
- [DynamoDB Best Practices](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/best-practices.html)

---

## License

This project is for internal use. The CardDemo application is licensed under Apache 2.0 by AWS.
