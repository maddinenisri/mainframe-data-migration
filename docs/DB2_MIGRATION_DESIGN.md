# DB2 Mainframe Migration - Design Document

## 1. Executive Summary

This document outlines the design considerations for migrating IBM DB2 for z/OS databases to modern cloud-native databases (PostgreSQL, Amazon Aurora, or Amazon RDS). While the EBCDIC file migration uses Spark + Cobrix for batch processing, DB2 migration requires different approaches based on the migration strategy.

---

## 2. Comparison: VSAM/EBCDIC Files vs DB2 Tables

| Aspect | VSAM/EBCDIC Files | DB2 Tables |
|--------|-------------------|------------|
| **Data Format** | Binary EBCDIC fixed-length | Relational with SQL access |
| **Schema Source** | COBOL Copybooks (.cpy) | DDL/DCL files, DCLGEN output |
| **Access Method** | Sequential/Indexed file I/O | SQL queries via JDBC/ODBC |
| **Relationships** | Implicit (cross-reference files) | Explicit (foreign keys) |
| **Data Types** | PIC clauses, COMP-3 | SQL types (CHAR, VARCHAR, etc.) |
| **Encoding** | EBCDIC (always) | EBCDIC or Unicode (configurable) |
| **Migration Tool** | Spark + Cobrix | JDBC + Spark SQL / AWS DMS |

---

## 3. DB2 Migration Architecture

### 3.1 High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         DB2 MIGRATION ARCHITECTURE                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────┐                           ┌─────────────────────────┐ │
│  │  DB2 for z/OS   │                           │   Target Database       │ │
│  │                 │                           │   (PostgreSQL/Aurora)   │ │
│  │  ┌───────────┐  │     ┌─────────────────┐   │                         │ │
│  │  │  Tables   │──│────▶│  Migration Tool │───│──▶ Tables               │ │
│  │  └───────────┘  │     │  (DMS/Spark)    │   │                         │ │
│  │  ┌───────────┐  │     └─────────────────┘   │                         │ │
│  │  │  Indexes  │  │              │            │     Indexes             │ │
│  │  └───────────┘  │              ▼            │                         │ │
│  │  ┌───────────┐  │     ┌─────────────────┐   │     Constraints         │ │
│  │  │    FKs    │  │     │ Schema Converter│   │                         │ │
│  │  └───────────┘  │     │ (AWS SCT)       │   │     Sequences           │ │
│  │                 │     └─────────────────┘   │                         │ │
│  └─────────────────┘                           └─────────────────────────┘ │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 3.2 Migration Approaches

| Approach | Best For | Tools | Downtime |
|----------|----------|-------|----------|
| **Lift & Shift** | Quick migration, similar DB | AWS DMS | Low |
| **Re-platform** | PostgreSQL/Aurora target | AWS SCT + DMS | Medium |
| **Batch Export** | One-time migration | Spark + JDBC | High |
| **CDC (Change Data Capture)** | Zero-downtime | AWS DMS CDC | Near-zero |

---

## 4. Schema Extraction

### 4.1 Source Artifacts

DB2 schemas are defined in multiple artifact types:

| Artifact | Extension | Purpose | Example |
|----------|-----------|---------|---------|
| **DDL** | `.ddl` | Table/Index creation | `CREATE TABLE...` |
| **DCL** | `.dcl` | DCLGEN COBOL bindings | `EXEC SQL DECLARE...` |
| **CTL** | `.ctl` | Control files for utilities | Database creation |

### 4.2 CardDemo DB2 Tables

From the CardDemo application:

**TRANSACTION_TYPE Table:**
```sql
CREATE TABLE CARDDEMO.TRANSACTION_TYPE (
    TR_TYPE         CHAR(2)      NOT NULL,
    TR_DESCRIPTION  VARCHAR(50)  NOT NULL,
    PRIMARY KEY(TR_TYPE)
) CCSID EBCDIC;
```

**TRANSACTION_TYPE_CATEGORY Table:**
```sql
CREATE TABLE CARDDEMO.TRANSACTION_TYPE_CATEGORY (
    TRC_TYPE_CODE      CHAR(2)      NOT NULL,
    TRC_TYPE_CATEGORY  CHAR(4)      NOT NULL,
    TRC_CAT_DATA       VARCHAR(50)  NOT NULL,
    PRIMARY KEY(TRC_TYPE_CODE, TRC_TYPE_CATEGORY),
    FOREIGN KEY (TRC_TYPE_CODE)
        REFERENCES CARDDEMO.TRANSACTION_TYPE (TR_TYPE)
        ON DELETE RESTRICT
);
```

### 4.3 DCLGEN Output (COBOL Host Variables)

DCLGEN generates COBOL structures for embedded SQL:

```cobol
01  DCLTRANSACTION-TYPE.
    10 DCL-TR-TYPE              PIC X(2).
    10 DCL-TR-DESCRIPTION.
       49 DCL-TR-DESCRIPTION-LEN   PIC S9(4) USAGE COMP.
       49 DCL-TR-DESCRIPTION-TEXT  PIC X(50).
```

This shows how VARCHAR is represented: length field (COMP) + data field.

---

## 5. Data Type Mapping

### 5.1 DB2 to PostgreSQL Type Mapping

| DB2 Type | PostgreSQL Type | Notes |
|----------|-----------------|-------|
| `CHAR(n)` | `CHAR(n)` | Direct mapping |
| `VARCHAR(n)` | `VARCHAR(n)` | Direct mapping |
| `SMALLINT` | `SMALLINT` | Direct mapping |
| `INTEGER` | `INTEGER` | Direct mapping |
| `BIGINT` | `BIGINT` | Direct mapping |
| `DECIMAL(p,s)` | `NUMERIC(p,s)` | Precision preserved |
| `FLOAT` | `DOUBLE PRECISION` | 64-bit float |
| `DATE` | `DATE` | Direct mapping |
| `TIME` | `TIME` | Direct mapping |
| `TIMESTAMP` | `TIMESTAMP` | Direct mapping |
| `CLOB` | `TEXT` | Large text |
| `BLOB` | `BYTEA` | Binary data |
| `GRAPHIC(n)` | `VARCHAR(n)` | DBCS to Unicode |
| `VARGRAPHIC(n)` | `VARCHAR(n)` | DBCS to Unicode |

### 5.2 Special Considerations

#### 5.2.1 EBCDIC Encoding
```sql
-- DB2 z/OS table with EBCDIC encoding
CREATE TABLE ... CCSID EBCDIC;

-- Must convert to UTF-8 during migration
-- AWS DMS handles this automatically
```

#### 5.2.2 VARCHAR Length Semantics
```
DB2:        VARCHAR(50) = up to 50 bytes (EBCDIC)
PostgreSQL: VARCHAR(50) = up to 50 characters (UTF-8)

For EBCDIC single-byte: 1:1 mapping
For DBCS/mixed:         Recalculate based on character count
```

#### 5.2.3 Packed Decimal in DB2
```sql
-- DB2 DECIMAL stored as packed decimal internally
DECIMAL(10,2)  -- 10 digits, 2 decimal places

-- PostgreSQL NUMERIC is exact arithmetic
NUMERIC(10,2)  -- Equivalent precision
```

---

## 6. Constraint and Index Migration

### 6.1 Primary Keys

```sql
-- DB2
PRIMARY KEY(TR_TYPE)

-- PostgreSQL (identical)
PRIMARY KEY(TR_TYPE)
```

### 6.2 Foreign Keys

```sql
-- DB2
FOREIGN KEY (TRC_TYPE_CODE)
    REFERENCES CARDDEMO.TRANSACTION_TYPE (TR_TYPE)
    ON DELETE RESTRICT

-- PostgreSQL
FOREIGN KEY (TRC_TYPE_CODE)
    REFERENCES transaction_type (tr_type)
    ON DELETE RESTRICT
```

**Note:** PostgreSQL uses lowercase identifiers by default.

### 6.3 Indexes

```sql
-- DB2
CREATE UNIQUE INDEX CARDDEMO.XTRAN_TYPE
    ON CARDDEMO.TRANSACTION_TYPE (TR_TYPE ASC)
    USING STOGROUP AWST1STG
    BUFFERPOOL BP0;

-- PostgreSQL (simplified)
CREATE UNIQUE INDEX xtran_type
    ON transaction_type (tr_type ASC);
```

**Dropped clauses:** `STOGROUP`, `BUFFERPOOL`, `ERASE`, `CLOSE` (z/OS specific)

---

## 7. Migration Implementation

### 7.1 Option A: AWS DMS (Recommended for Production)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         AWS DMS MIGRATION FLOW                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  1. SCHEMA CONVERSION (AWS SCT)                                             │
│     ├── Input: DB2 DDL files                                               │
│     ├── Output: PostgreSQL DDL                                             │
│     └── Review: Manual adjustments for incompatibilities                   │
│                                                                             │
│  2. CREATE TARGET SCHEMA                                                    │
│     └── Run converted DDL on PostgreSQL/Aurora                             │
│                                                                             │
│  3. DMS REPLICATION INSTANCE                                                │
│     ├── Source Endpoint: DB2 z/OS (via Connect:Direct or VPN)             │
│     └── Target Endpoint: Aurora PostgreSQL                                 │
│                                                                             │
│  4. MIGRATION TASK                                                          │
│     ├── Full Load: Initial data copy                                       │
│     ├── CDC: Ongoing replication (optional)                                │
│     └── Validation: Row count + checksum verification                      │
│                                                                             │
│  5. CUTOVER                                                                 │
│     ├── Stop source application                                            │
│     ├── Final CDC sync                                                     │
│     └── Switch application to new database                                 │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 7.2 Option B: Spark + JDBC (For Local Testing / Batch)

Similar to our EBCDIC migration but using JDBC:

```python
from pyspark.sql import SparkSession

# DB2 JDBC connection properties
db2_url = "jdbc:db2://mainframe.example.com:5023/CARDDEMO"
db2_properties = {
    "user": "db2user",
    "password": "db2pass",
    "driver": "com.ibm.db2.jcc.DB2Driver",
    "fetchsize": "10000",
}

# Initialize Spark with DB2 driver
spark = SparkSession.builder \
    .appName("DB2_Migration") \
    .config("spark.jars", "libs/db2jcc4.jar") \
    .config("spark.driver.extraJavaOptions", JAVA_17_OPTIONS) \
    .master("local[*]") \
    .getOrCreate()

# Read from DB2
df = spark.read \
    .jdbc(db2_url, "CARDDEMO.TRANSACTION_TYPE", properties=db2_properties)

# Transform if needed
df_transformed = df.withColumnRenamed("TR_TYPE", "tr_type") \
                   .withColumnRenamed("TR_DESCRIPTION", "tr_description")

# Write to PostgreSQL
pg_url = "jdbc:postgresql://localhost:5432/carddemo"
pg_properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver",
}

df_transformed.write \
    .jdbc(pg_url, "transaction_type", mode="overwrite", properties=pg_properties)
```

### 7.3 Option C: Export to JSON (Similar to EBCDIC Approach)

```python
# Read from DB2
df = spark.read.jdbc(db2_url, "CARDDEMO.TRANSACTION_TYPE", properties=db2_properties)

# Write to JSON
df.write.json("output/db2/transaction_type")
```

---

## 8. Key Design Considerations

### 8.1 Character Encoding

| Aspect | DB2 z/OS | PostgreSQL | Action Required |
|--------|----------|------------|-----------------|
| Default encoding | EBCDIC | UTF-8 | Automatic conversion |
| CCSID 500 | Latin-1 EBCDIC | UTF-8 | Code page mapping |
| DBCS (Japanese, etc.) | EBCDIC DBCS | UTF-8 | Character expansion |

**Conversion happens at:**
- JDBC driver level (automatic)
- AWS DMS (automatic)
- Application level (if direct file export)

### 8.2 Identifier Case Sensitivity

```sql
-- DB2 (case-insensitive, stored uppercase)
SELECT * FROM CARDDEMO.TRANSACTION_TYPE;

-- PostgreSQL (case-sensitive, stored lowercase unless quoted)
SELECT * FROM transaction_type;
-- or
SELECT * FROM "TRANSACTION_TYPE";  -- preserve case with quotes
```

**Recommendation:** Convert to lowercase during migration for PostgreSQL conventions.

### 8.3 NULL Handling

```sql
-- DB2: NULL represented as null indicator
-- PostgreSQL: Native NULL support

-- Watch for: COBOL programs that use indicator variables
-- May need application code changes
```

### 8.4 Sequence/Identity Generation

```sql
-- DB2 Identity Column
CREATE TABLE t (
    id INTEGER GENERATED ALWAYS AS IDENTITY,
    ...
);

-- PostgreSQL Equivalent
CREATE TABLE t (
    id SERIAL PRIMARY KEY,  -- or GENERATED ALWAYS AS IDENTITY
    ...
);
```

### 8.5 Transaction Isolation

| DB2 Level | PostgreSQL Equivalent |
|-----------|----------------------|
| Cursor Stability (CS) | Read Committed |
| Read Stability (RS) | Repeatable Read |
| Repeatable Read (RR) | Serializable |
| Uncommitted Read (UR) | Read Uncommitted |

---

## 9. Handling Relationships

### 9.1 Entity Relationship Diagram (CardDemo DB2)

```
┌─────────────────────────┐
│   TRANSACTION_TYPE      │
├─────────────────────────┤
│ TR_TYPE (PK)      CHAR(2)│◀─────────┐
│ TR_DESCRIPTION VARCHAR(50)│         │
└─────────────────────────┘          │
                                      │ FK
┌─────────────────────────────────────┴───────────────┐
│         TRANSACTION_TYPE_CATEGORY                    │
├──────────────────────────────────────────────────────┤
│ TRC_TYPE_CODE (PK, FK)         CHAR(2)              │
│ TRC_TYPE_CATEGORY (PK)         CHAR(4)              │
│ TRC_CAT_DATA                   VARCHAR(50)          │
└──────────────────────────────────────────────────────┘
```

### 9.2 Migration Order (Dependency-Aware)

```
1. TRANSACTION_TYPE          (no dependencies)
2. TRANSACTION_TYPE_CATEGORY (depends on TRANSACTION_TYPE)
```

**Rule:** Migrate parent tables before child tables to maintain referential integrity.

---

## 10. Comparison: EBCDIC vs DB2 Pipeline

### 10.1 Side-by-Side Comparison

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    EBCDIC FILE MIGRATION PIPELINE                           │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────┐   ┌─────────────┐   ┌─────────────┐   ┌─────────────┐    │
│  │ EBCDIC File │──▶│  Copybook   │──▶│   Cobrix    │──▶│    JSON     │    │
│  │ (.PS)       │   │  (.cpy)     │   │  (Spark)    │   │   Output    │    │
│  └─────────────┘   └─────────────┘   └─────────────┘   └─────────────┘    │
│                                                                             │
│  Key: Schema from copybook, binary parsing, character conversion           │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│                      DB2 TABLE MIGRATION PIPELINE                           │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────┐   ┌─────────────┐   ┌─────────────┐   ┌─────────────┐    │
│  │ DB2 Table   │──▶│  DDL/DCL    │──▶│  JDBC/DMS   │──▶│  PostgreSQL │    │
│  │ (SQL)       │   │  Schema     │   │  (Driver)   │   │   Table     │    │
│  └─────────────┘   └─────────────┘   └─────────────┘   └─────────────┘    │
│                                                                             │
│  Key: Schema from DDL, SQL access, automatic encoding via JDBC             │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 10.2 Tool Selection Matrix

| Factor | EBCDIC Files | DB2 Tables |
|--------|--------------|------------|
| **Schema Discovery** | Parse copybooks | Query SYSIBM catalog or DDL |
| **Data Access** | File system read | JDBC connection |
| **Encoding** | Manual (Cobrix) | Automatic (JDBC driver) |
| **Incremental Sync** | File comparison | CDC (DMS) |
| **Relationships** | Inferred | Explicit FKs |
| **Best Tool** | Spark + Cobrix | AWS DMS or Spark + JDBC |

---

## 11. Implementation Checklist

### 11.1 Pre-Migration

- [ ] Inventory all DB2 tables and relationships
- [ ] Extract DDL for all tables, indexes, and constraints
- [ ] Identify CCSID (encoding) for each table
- [ ] Document stored procedures and triggers
- [ ] Analyze VARCHAR vs CHAR usage
- [ ] Map DB2 types to PostgreSQL types
- [ ] Plan migration order (parent → child)

### 11.2 Schema Migration

- [ ] Run AWS SCT or manual DDL conversion
- [ ] Review and fix incompatibilities
- [ ] Create sequences for identity columns
- [ ] Create tables without FKs first
- [ ] Create indexes
- [ ] Add FK constraints last

### 11.3 Data Migration

- [ ] Configure source endpoint (DB2 JDBC)
- [ ] Configure target endpoint (PostgreSQL)
- [ ] Run full load migration
- [ ] Validate row counts
- [ ] Validate data checksums
- [ ] Enable CDC if needed

### 11.4 Post-Migration

- [ ] Enable FK constraints
- [ ] Rebuild indexes/statistics
- [ ] Run application tests
- [ ] Compare query performance
- [ ] Plan cutover window

---

## 12. Sample Implementation Code

### 12.1 Project Structure

```
mainframe-migration/
├── db2/
│   ├── schema/
│   │   ├── source/           # Original DB2 DDL
│   │   │   ├── TRNTYPE.ddl
│   │   │   └── TRNTYCAT.ddl
│   │   └── target/           # Converted PostgreSQL DDL
│   │       ├── transaction_type.sql
│   │       └── transaction_type_category.sql
│   ├── dcl/                  # DCLGEN files (for reference)
│   ├── scripts/
│   │   ├── db2_to_json.py    # Export DB2 to JSON
│   │   └── db2_to_postgres.py # Direct DB2 to PostgreSQL
│   └── output/               # Migrated data
├── input/                    # EBCDIC files (existing)
├── output/                   # JSON output (existing)
└── docs/
    ├── DESIGN.md
    ├── HOW_IT_WORKS.md
    └── DB2_MIGRATION_DESIGN.md  # This document
```

### 12.2 Schema Conversion Example

**Source (DB2):**
```sql
CREATE TABLE CARDDEMO.TRANSACTION_TYPE (
    TR_TYPE         CHAR(2)      NOT NULL,
    TR_DESCRIPTION  VARCHAR(50)  NOT NULL,
    PRIMARY KEY(TR_TYPE)
) IN CARDDEMO.CARDSPC1
  CCSID EBCDIC;

CREATE UNIQUE INDEX CARDDEMO.XTRAN_TYPE
    ON CARDDEMO.TRANSACTION_TYPE (TR_TYPE ASC)
    USING STOGROUP AWST1STG
    BUFFERPOOL BP0;
```

**Target (PostgreSQL):**
```sql
CREATE TABLE transaction_type (
    tr_type         CHAR(2)      NOT NULL,
    tr_description  VARCHAR(50)  NOT NULL,
    PRIMARY KEY(tr_type)
);

CREATE UNIQUE INDEX xtran_type
    ON transaction_type (tr_type ASC);

-- Character set is UTF-8 by default in PostgreSQL
```

### 12.3 Spark Migration Script

```python
"""
DB2 to PostgreSQL migration using Spark.
Mirrors the EBCDIC migration pattern.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import lower, col
import os

# Configuration
DB2_URL = "jdbc:db2://mainframe:5023/CARDDEMO"
DB2_DRIVER = "com.ibm.db2.jcc.DB2Driver"
PG_URL = "jdbc:postgresql://localhost:5432/carddemo"
PG_DRIVER = "org.postgresql.Driver"

JAVA_17_OPTIONS = " ".join([
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.util=ALL-UNNAMED",
])

# Tables to migrate (in dependency order)
TABLES = [
    {"source": "CARDDEMO.TRANSACTION_TYPE", "target": "transaction_type"},
    {"source": "CARDDEMO.TRANSACTION_TYPE_CATEGORY", "target": "transaction_type_category"},
]


def create_spark_session():
    return (
        SparkSession.builder
        .appName("DB2_to_PostgreSQL_Migration")
        .config("spark.jars", "libs/db2jcc4.jar,libs/postgresql-42.6.0.jar")
        .config("spark.driver.extraJavaOptions", JAVA_17_OPTIONS)
        .master("local[*]")
        .getOrCreate()
    )


def migrate_table(spark, source_table, target_table, db2_props, pg_props):
    """Migrate a single table from DB2 to PostgreSQL."""
    print(f"Migrating {source_table} -> {target_table}...")

    # Read from DB2
    df = spark.read.jdbc(DB2_URL, source_table, properties=db2_props)

    # Transform column names to lowercase
    for col_name in df.columns:
        df = df.withColumnRenamed(col_name, col_name.lower())

    # Write to PostgreSQL
    df.write.jdbc(
        PG_URL,
        target_table,
        mode="overwrite",
        properties=pg_props
    )

    count = df.count()
    print(f"  ✓ Migrated {count} records")
    return count


def main():
    db2_props = {
        "user": os.environ.get("DB2_USER", "db2admin"),
        "password": os.environ.get("DB2_PASSWORD", ""),
        "driver": DB2_DRIVER,
    }

    pg_props = {
        "user": os.environ.get("PG_USER", "postgres"),
        "password": os.environ.get("PG_PASSWORD", ""),
        "driver": PG_DRIVER,
    }

    spark = create_spark_session()

    try:
        total = 0
        for table in TABLES:
            count = migrate_table(
                spark,
                table["source"],
                table["target"],
                db2_props,
                pg_props
            )
            total += count

        print(f"\nMigration complete: {total} total records")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
```

---

## 13. Summary: Key Differences

| Aspect | EBCDIC File Migration | DB2 Table Migration |
|--------|----------------------|---------------------|
| **Data Access** | Binary file read | SQL/JDBC connection |
| **Schema Source** | COBOL copybook | DDL/DCLGEN |
| **Parsing Tool** | Cobrix library | JDBC driver |
| **Encoding Handling** | Manual (Cobrix) | Automatic (driver) |
| **Relationship Discovery** | Analyze code/data | Query catalog/DDL |
| **Incremental Updates** | File comparison | CDC/log mining |
| **AWS Service** | Glue + S3 | DMS + Aurora |
| **Complexity** | Higher (binary format) | Lower (SQL standard) |

---

## 14. Recommendations

1. **For Production:** Use AWS DMS + SCT for automated migration with CDC support
2. **For Development/Testing:** Use Spark + JDBC for flexibility and local testing
3. **For One-Time Migration:** Export to JSON first, then load to target
4. **Always:** Migrate parent tables before child tables (FK dependency order)
5. **Validate:** Compare row counts and checksums post-migration

---

*Document Version: 1.0*
*Last Updated: December 2024*
