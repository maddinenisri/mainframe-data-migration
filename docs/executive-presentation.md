# Mainframe to NoSQL: Technical Deep Dive & Migration Strategy

## Executive Briefing Deck

---

# Table of Contents

1. [Executive Summary](#executive-summary)
2. [Business Value Proposition](#business-value-proposition)
3. [High-Level Architecture](#high-level-architecture)
4. [The Core Engine](#the-core-engine)
5. [Evidence of Success](#evidence-of-success)
6. [Data Integrity & Auditability](#data-integrity--auditability)
7. [DB2 Migration Path](#db2-migration-path)
8. [Production Roadmap](#production-roadmap)
9. [Risk Mitigation](#risk-mitigation)
10. [Appendix](#appendix)

---

# Executive Summary

## The Challenge

Our organization faces increasing costs from mainframe MIPS consumption, limited agility for new feature development, and a shrinking pool of COBOL expertise.

## The Solution

A serverless, cloud-native data migration pipeline that:
- **Decouples data** from expensive mainframe compute
- **Preserves data integrity** using original COBOL definitions
- **Enables modern applications** through industry-standard JSON format

## Key Results (Proof of Concept)

| Metric | Value |
|--------|-------|
| Datasets Converted | 11 |
| Total Records Processed | 1,136 |
| Success Rate | 100% |
| Data Format | JSON (DynamoDB-ready) |

---

# Business Value Proposition

## Cost Reduction

```mermaid
graph LR
    subgraph Before["Before Migration"]
        A[("Mainframe<br/>MIPS $$$$")] --> B["Applications"]
        A --> C["Reports"]
        A --> D["Analytics"]
    end

    subgraph After["After Migration"]
        E[("DynamoDB<br/>Pay-per-use")] --> F["Applications"]
        E --> G["Reports"]
        E --> H["Analytics"]
        I[("Mainframe<br/>Core Only")] --> J["Batch Jobs"]
    end

    style A fill:#ff6b6b,stroke:#333,color:#fff
    style E fill:#51cf66,stroke:#333,color:#fff
    style I fill:#ffd43b,stroke:#333,color:#000
```

## Business Benefits

| Benefit | Impact |
|---------|--------|
| **MIPS Reduction** | Move read workloads off mainframe |
| **Scalability** | Auto-scaling cloud infrastructure |
| **Agility** | JSON enables rapid app development |
| **Talent Pool** | Python/Cloud skills vs COBOL specialists |

---

# High-Level Architecture

## End-to-End Data Flow

```mermaid
flowchart TB
    subgraph Mainframe["Legacy Mainframe"]
        direction TB
        VSAM[("VSAM Files<br/>(EBCDIC Binary)")]
        DB2[("DB2 Tables<br/>(Relational)")]
        CPY["COBOL Copybooks<br/>(Schema Definitions)"]
    end

    subgraph Pipeline["AWS Migration Pipeline"]
        direction TB
        S3IN[("S3 Input<br/>Bucket")]
        GLUE["AWS Glue<br/>(Serverless ETL)"]
        SPARK["Apache Spark<br/>+ Cobrix"]
        S3OUT[("S3 Output<br/>JSON")]
    end

    subgraph Target["Modern Cloud Platform"]
        direction TB
        DYNAMO[("Amazon<br/>DynamoDB")]
        ELASTIC[("Amazon<br/>OpenSearch")]
        APPS["Modern<br/>Applications"]
    end

    VSAM --> S3IN
    DB2 --> S3IN
    CPY --> GLUE
    S3IN --> GLUE
    GLUE --> SPARK
    SPARK --> S3OUT
    S3OUT --> DYNAMO
    S3OUT --> ELASTIC
    DYNAMO --> APPS
    ELASTIC --> APPS

    style VSAM fill:#ff922b,stroke:#333,color:#fff
    style DB2 fill:#ff922b,stroke:#333,color:#fff
    style DYNAMO fill:#339af0,stroke:#333,color:#fff
    style ELASTIC fill:#339af0,stroke:#333,color:#fff
    style GLUE fill:#20c997,stroke:#333,color:#fff
```

## Key Innovation: Schema-on-Read

Instead of manually mapping thousands of fields, we use the **original COBOL Copybooks** as the source of truth.

> **Result**: When mainframe teams change data structures, we simply upload the new Copybook. Zero code changes required.

---

# The Core Engine

## How convert_all.py Works

```mermaid
flowchart LR
    subgraph Input["Input Directory"]
        DATA["AWS.M2.CARDDEMO<br/>.CUSTDATA.PS<br/>(Binary EBCDIC)"]
        COPY["CUSTREC.cpy<br/>(Schema Definition)"]
    end

    subgraph Engine["Conversion Engine"]
        MATCH["Auto-Match<br/>Data + Copybook"]
        PARSE["Parse EBCDIC<br/>(Packed Decimal)"]
        TRANSFORM["Transform to<br/>JSON"]
    end

    subgraph Output["Output Directory"]
        JSON["customers/<br/>part-00000.json"]
        MANIFEST["conversion_<br/>manifest.json"]
        SUCCESS["._SUCCESS.crc<br/>(Integrity Check)"]
    end

    DATA --> MATCH
    COPY --> MATCH
    MATCH --> PARSE
    PARSE --> TRANSFORM
    TRANSFORM --> JSON
    TRANSFORM --> MANIFEST
    TRANSFORM --> SUCCESS

    style DATA fill:#ff922b,stroke:#333,color:#fff
    style JSON fill:#51cf66,stroke:#333,color:#fff
    style SUCCESS fill:#339af0,stroke:#333,color:#fff
```

## Dataset Configuration

The engine automatically processes these mainframe datasets:

| Dataset | Source File | Records | Description |
|---------|-------------|---------|-------------|
| customers | AWS.M2.CARDDEMO.CUSTDATA.PS | 50 | Customer demographics |
| accounts | AWS.M2.CARDDEMO.ACCTDATA.PS | 50 | Account balances |
| cards | AWS.M2.CARDDEMO.CARDDATA.PS | 50 | Credit card info |
| daily_transactions | AWS.M2.CARDDEMO.DALYTRAN.PS | 300 | Transaction records |
| users | AWS.M2.CARDDEMO.USRSEC.PS | 10 | Security data |

---

# Evidence of Success

## COBOL Copybook to JSON Transformation

### Source Definition (CUSTREC.cpy)

```cobol
01  CUSTOMER-RECORD.
    05  CUST-ID                 PIC 9(09).
    05  CUST-FIRST-NAME         PIC X(25).
    05  CUST-LAST-NAME          PIC X(25).
    05  CUST-DOB-YYYYMMDD       PIC X(10).
    05  CUST-FICO-CREDIT-SCORE  PIC 9(03).
```

### Binary Input (Unreadable)

```
AWS.M2.CARDDEMO.CUSTDATA.PS
[500-byte fixed-length EBCDIC records]
```

### Actual JSON Output

```json
{
  "CUST_ID": 1,
  "CUST_FIRST_NAME": "Immanuel",
  "CUST_LAST_NAME": "Kessler",
  "CUST_DOB_YYYYMMDD": "1961-06-08",
  "CUST_FICO_CREDIT_SCORE": 274
}
```

## Conversion Proof

```mermaid
flowchart LR
    subgraph Binary["Mainframe Binary"]
        B1["F0F0F0F0F0F0F0F1<br/>(EBCDIC)"]
    end

    subgraph Readable["Human Readable"]
        R1['"CUST_ID": 1']
    end

    B1 -->|"Cobrix Parser"| R1

    style B1 fill:#868e96,stroke:#333,color:#fff
    style R1 fill:#51cf66,stroke:#333,color:#fff
```

---

# Data Integrity & Auditability

## Built-In Safety Mechanisms

```mermaid
flowchart TB
    subgraph Conversion["Conversion Process"]
        SPARK["Spark Job"]
        WRITE["Write JSON"]
        VALIDATE["Validate CRC"]
    end

    subgraph Verification["Integrity Markers"]
        CRC["._SUCCESS.crc<br/>(Checksum)"]
        SUCCESS["_SUCCESS<br/>(Job Complete)"]
        MANIFEST["conversion_<br/>manifest.json"]
    end

    subgraph Audit["Audit Trail"]
        COUNT["Record Counts"]
        STATUS["Status: success/failed"]
        VERSION["Version Tracking"]
    end

    SPARK --> WRITE
    WRITE --> VALIDATE
    VALIDATE -->|"Pass"| CRC
    VALIDATE -->|"Pass"| SUCCESS
    VALIDATE --> MANIFEST
    MANIFEST --> COUNT
    MANIFEST --> STATUS
    MANIFEST --> VERSION

    style CRC fill:#339af0,stroke:#333,color:#fff
    style SUCCESS fill:#51cf66,stroke:#333,color:#fff
    style MANIFEST fill:#ffd43b,stroke:#333,color:#000
```

## What This Means for You

| Safety Feature | Business Impact |
|----------------|-----------------|
| **CRC Checksums** | No partial/corrupt data enters production |
| **_SUCCESS Files** | Only complete jobs are promoted |
| **Manifest Tracking** | Full audit trail for compliance |
| **Isolated Folders** | accounts/, cards/, users/ - clean partitions |

## Conversion Manifest Sample

```json
{
  "datasets": [
    {"name": "customers", "status": "success", "record_count": 50},
    {"name": "accounts", "status": "success", "record_count": 50},
    {"name": "daily_transactions", "status": "success", "record_count": 300}
  ],
  "total_records": 1136,
  "cobrix_version": "2.6.9",
  "spark_version": "3.5.0"
}
```

---

# DB2 Migration Path

## Handling Relational Data

```mermaid
flowchart TB
    subgraph DB2["DB2 Database"]
        DDL["DDL Definitions<br/>TRNTYPE.ddl"]
        TABLES[("DB2 Tables<br/>CARDDEMO.TRANSACTION_TYPE")]
    end

    subgraph Script["db2_to_json.py"]
        PARSE["Parse DDL<br/>Schema"]
        JDBC["JDBC or<br/>Mock Export"]
        TRANSFORM["Transform to<br/>JSON"]
    end

    subgraph Output["Unified Output"]
        JSON1["transaction_type/<br/>part-00000.json"]
        JSON2["transaction_type_<br/>category/"]
    end

    DDL --> PARSE
    TABLES --> JDBC
    PARSE --> TRANSFORM
    JDBC --> TRANSFORM
    TRANSFORM --> JSON1
    TRANSFORM --> JSON2

    style TABLES fill:#ff922b,stroke:#333,color:#fff
    style JSON1 fill:#51cf66,stroke:#333,color:#fff
    style JSON2 fill:#51cf66,stroke:#333,color:#fff
```

## DB2 Table Example

### DDL Definition (TRNTYPE.ddl)

```sql
CREATE TABLE CARDDEMO.TRANSACTION_TYPE
(   TR_TYPE        CHAR(2) NOT NULL,
    TR_DESCRIPTION VARCHAR(50) NOT NULL,
    PRIMARY KEY(TR_TYPE));
```

### Exported JSON

```json
{"tr_type": "01", "tr_description": "Purchase"}
{"tr_type": "02", "tr_description": "Cash Advance"}
{"tr_type": "04", "tr_description": "Payment"}
```

## Key Benefit: Homogenized Data Layer

Whether the source is:
- VSAM flat files (EBCDIC binary)
- DB2 relational tables

The downstream application sees **identical JSON format**.

---

# Production Roadmap

## Three-Phase Implementation

```mermaid
timeline
    title Migration Phases

    section Phase 1
        Batch Conversion : Current State
                        : Daily dumps via convert_all.py
                        : Load to DynamoDB for read-only apps

    section Phase 2
        Event-Driven : S3 event triggers
                    : Automated Glue jobs
                    : Near real-time updates

    section Phase 3
        Modernization : Decommission mainframe reads
                     : All queries via DynamoDB/OpenSearch
                     : Mainframe for batch only
```

## Phase Details

### Phase 1: Batch Conversion (Current)

```mermaid
flowchart LR
    MF["Mainframe<br/>Nightly Batch"] -->|"FTP/SFTP"| S3["S3 Input"]
    S3 --> GLUE["AWS Glue<br/>convert_all.py"]
    GLUE --> OUT["S3 Output<br/>JSON"]
    OUT --> DDB["DynamoDB"]
    DDB --> APP["Read-Only<br/>Applications"]

    style MF fill:#ff922b,stroke:#333,color:#fff
    style DDB fill:#339af0,stroke:#333,color:#fff
    style APP fill:#51cf66,stroke:#333,color:#fff
```

### Phase 2: Event-Driven Pipeline

```mermaid
flowchart LR
    MF["Mainframe<br/>Batch Complete"] -->|"S3 Upload"| S3["S3 Input"]
    S3 -->|"Event Trigger"| LAMBDA["Lambda<br/>Orchestrator"]
    LAMBDA --> GLUE["AWS Glue<br/>Job"]
    GLUE --> OUT["S3 Output"]
    OUT --> DDB["DynamoDB"]

    style LAMBDA fill:#f783ac,stroke:#333,color:#fff
    style S3 fill:#ffd43b,stroke:#333,color:#000
```

### Phase 3: Full Modernization

```mermaid
flowchart TB
    subgraph Legacy["Mainframe (Reduced)"]
        BATCH["Batch Processing<br/>Only"]
    end

    subgraph Cloud["AWS Cloud (Primary)"]
        DDB[("DynamoDB<br/>Operational Data")]
        ES[("OpenSearch<br/>Search/Analytics")]
        CACHE[("ElastiCache<br/>Hot Data")]
    end

    subgraph Apps["Modern Applications"]
        WEB["Web Apps"]
        MOBILE["Mobile Apps"]
        API["API Services"]
    end

    BATCH -->|"Nightly Sync"| DDB
    DDB --> WEB
    DDB --> MOBILE
    DDB --> API
    ES --> WEB
    ES --> API
    CACHE --> WEB
    CACHE --> MOBILE

    style BATCH fill:#ffd43b,stroke:#333,color:#000
    style DDB fill:#339af0,stroke:#333,color:#fff
    style ES fill:#339af0,stroke:#333,color:#fff
```

---

# Risk Mitigation

## Identified Risks and Controls

```mermaid
flowchart TB
    subgraph Risks["Risk Categories"]
        R1["Data Loss"]
        R2["Schema Changes"]
        R3["Performance"]
        R4["Security"]
    end

    subgraph Controls["Mitigation Controls"]
        C1["CRC Checksums<br/>+ Manifests"]
        C2["Copybook-Driven<br/>Schema-on-Read"]
        C3["Serverless Auto-<br/>Scaling (Glue)"]
        C4["IAM Roles<br/>+ Encryption"]
    end

    R1 --> C1
    R2 --> C2
    R3 --> C3
    R4 --> C4

    style R1 fill:#ff6b6b,stroke:#333,color:#fff
    style R2 fill:#ff6b6b,stroke:#333,color:#fff
    style R3 fill:#ff6b6b,stroke:#333,color:#fff
    style R4 fill:#ff6b6b,stroke:#333,color:#fff
    style C1 fill:#51cf66,stroke:#333,color:#fff
    style C2 fill:#51cf66,stroke:#333,color:#fff
    style C3 fill:#51cf66,stroke:#333,color:#fff
    style C4 fill:#51cf66,stroke:#333,color:#fff
```

## Risk Matrix

| Risk | Likelihood | Impact | Mitigation | Status |
|------|------------|--------|------------|--------|
| Data Loss | Low | High | CRC checksums, _SUCCESS markers | Implemented |
| Schema Drift | Medium | Medium | Copybook-driven parsing | Implemented |
| Performance | Low | Medium | Serverless auto-scaling | Designed |
| Security | Low | High | IAM, KMS encryption | Planned |
| Skill Gap | Medium | Low | Python/Spark (common skills) | Mitigated |

---

# Key Takeaways for Executives

## Why This Approach Works

```mermaid
mindmap
    root((Migration<br/>Strategy))
        Safety
            CRC Checksums
            Audit Manifests
            Isolated Outputs
        Flexibility
            Schema-on-Read
            Copybook Driven
            No Hardcoding
        Scalability
            Serverless Glue
            Auto-scaling
            Pay-per-use
        Maintainability
            Python/Spark
            Open Standards
            JSON Output
```

## Summary Points

1. **Proven Technology**: Successfully converted 1,136 records across 11 datasets
2. **Zero Code Changes**: Copybook updates automatically adapt the pipeline
3. **Full Audit Trail**: Every file tracked with checksums and manifests
4. **Cost Efficient**: Serverless = pay only for what you use
5. **Future Ready**: JSON enables DynamoDB, OpenSearch, and any modern app

---

# Appendix

## Technology Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| ETL Engine | AWS Glue 5.0 | Serverless data processing |
| Processing | Apache Spark 3.5 | Distributed computing |
| COBOL Parser | Cobrix 2.6.9 | EBCDIC/Copybook handling |
| Language | Python 3.11 | Orchestration logic |
| Runtime | Java 17+ | Spark execution |
| Target DB | Amazon DynamoDB | NoSQL data store |
| Search | Amazon OpenSearch | Full-text search |

## File Structure

```
mainframe-migration/
├── convert_all.py          # Main conversion orchestrator
├── local_test.py           # Local testing script
├── input/                  # Mainframe source files
│   ├── *.PS                # EBCDIC data files
│   └── *.cpy               # COBOL copybooks
├── output/                 # Converted JSON
│   ├── customers/
│   ├── accounts/
│   ├── cards/
│   └── conversion_manifest.json
└── db2/                    # DB2 migration
    ├── schema/             # DDL definitions
    ├── scripts/            # Export scripts
    └── output/             # DB2 JSON output
```

## Presenter Notes

### Slide 3 (Evidence of Success)
> "Show, Don't Just Tell": Open the actual `output/customers/part-00000...json` file on screen. Showing readable JSON generated from "unreadable" mainframe binary is powerful for stakeholders.

### Slide 4 (Data Integrity)
> Emphasize the `._SUCCESS.crc` files. Executives worry about data loss. These prove the system has built-in integrity checks that prevent partial/corrupt data from entering the new system.

### Slide 1 & 2 (Architecture)
> Stress that we use Copybooks (`.cpy`) as the source of truth. This means we aren't hardcoding logic that will break next year; we're building a system that adapts to the mainframe team's changes automatically.

---

## Contact & Questions

**Project Team**: Data Platform Engineering

**Code Repository**: `/mainframe-migration/`

**Documentation**: See `convert_all.py` inline comments

---

*Generated for Executive Briefing - Mainframe Modernization Initiative*
