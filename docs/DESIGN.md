# Mainframe Data Migration - Detailed Design Document

## 1. Executive Summary

This document describes the design and implementation of a local testing environment for migrating AWS CardDemo mainframe EBCDIC data to modern JSON format. The solution mimics AWS Glue 5.0 runtime specifications for seamless transition to production.

## 2. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        LOCAL TESTING ENVIRONMENT                            │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌──────────────┐     ┌──────────────┐     ┌──────────────────────────────┐ │
│  │   EBCDIC     │     │   COBOL      │     │        Apache Spark          │ │
│  │   Data       │────▶│   Copybooks  │────▶│   + Cobrix Library           │ │
│  │   Files      │     │   (.cpy)     │     │   (Java 17 / Spark 3.5)      │ │
│  └──────────────┘     └──────────────┘     └──────────────┬───────────────┘ │
│                                                           │                 │
│                                                           ▼                 │
│                                            ┌──────────────────────────────┐ │
│                                            │     JSON Output Files        │ │
│                                            │     (One per dataset)        │ │
│                                            └──────────────────────────────┘ │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## 3. Technology Stack

| Component | Version | Purpose |
|-----------|---------|---------|
| Python | 3.10 - 3.11 | Runtime environment |
| PySpark | 3.5.0 | Data processing framework |
| Java | 17+ (21 tested) | JVM runtime for Spark |
| Cobrix | 2.6.9 | COBOL/EBCDIC parsing library |
| UV | Latest | Python package management |

### 3.1 Why These Versions?

- **Spark 3.5.0**: Matches AWS Glue 5.0 runtime
- **Java 17+**: Required by Glue 5.0; needs special JVM flags for module access
- **Cobrix 2.6.9**: Latest stable version with Scala 2.12 (Spark 3.5 compatible)
- **UV**: Modern, fast Python package manager with excellent dependency resolution

## 4. Data Model

### 4.1 Source Datasets

The CardDemo application contains 11 EBCDIC datasets:

| Dataset | File | Copybook | Records | Record Length | Description |
|---------|------|----------|---------|---------------|-------------|
| customers | AWS.M2.CARDDEMO.CUSTDATA.PS | CUSTREC.cpy | 50 | 500 | Customer master data |
| accounts | AWS.M2.CARDDEMO.ACCTDATA.PS | CVACT01Y.cpy | 50 | 300 | Account records |
| cards | AWS.M2.CARDDEMO.CARDDATA.PS | CVACT02Y.cpy | 50 | 150 | Credit card data |
| card_xref | AWS.M2.CARDDEMO.CARDXREF.PS | CVACT03Y.cpy | 50 | 50 | Cross-reference |
| daily_transactions | AWS.M2.CARDDEMO.DALYTRAN.PS | CVTRA06Y.cpy | 300 | 350 | Transaction records |
| transaction_types | AWS.M2.CARDDEMO.TRANTYPE.PS | CVTRA03Y.cpy | 7 | 60 | Type reference |
| transaction_categories | AWS.M2.CARDDEMO.TRANCATG.PS | CVTRA04Y.cpy | 18 | 60 | Category reference |
| users | AWS.M2.CARDDEMO.USRSEC.PS | CSUSR01Y.cpy | 10 | 80 | User security |
| discount_groups | AWS.M2.CARDDEMO.DISCGRP.PS | DISCGRP.cpy | 51 | 50 | Discount config |
| category_balances | AWS.M2.CARDDEMO.TCATBALF.PS | TCATBALF.cpy | 50 | 50 | Category balances |
| export_data | AWS.M2.CARDDEMO.EXPORT.DATA.PS | CVEXPORT.cpy | 500 | 500 | Multi-record export |

### 4.2 Entity Relationship Diagram

```
┌─────────────────┐       ┌─────────────────┐       ┌─────────────────┐
│    CUSTOMERS    │       │    ACCOUNTS     │       │     CARDS       │
├─────────────────┤       ├─────────────────┤       ├─────────────────┤
│ CUST_ID (PK)    │◀──┐   │ ACCT_ID (PK)    │◀──┐   │ CARD_NUM (PK)   │
│ CUST_FIRST_NAME │   │   │ ACCT_ACTIVE_STS │   │   │ CARD_ACCT_ID(FK)│──┐
│ CUST_LAST_NAME  │   │   │ ACCT_CURR_BAL   │   │   │ CARD_CVV_CD     │  │
│ CUST_SSN        │   │   │ ACCT_CREDIT_LMT │   │   │ CARD_EXP_DATE   │  │
│ CUST_DOB        │   │   │ ACCT_OPEN_DATE  │   │   │ CARD_STATUS     │  │
│ CUST_FICO_SCORE │   │   │ ACCT_GROUP_ID   │───│──▶└─────────────────┘  │
└─────────────────┘   │   └─────────────────┘   │                        │
                      │                         │                        │
┌─────────────────┐   │   ┌─────────────────┐   │                        │
│   CARD_XREF     │   │   │  TRANSACTIONS   │   │                        │
├─────────────────┤   │   ├─────────────────┤   │                        │
│ XREF_CARD_NUM   │───│──▶│ TRAN_ID (PK)    │   │                        │
│ XREF_CUST_ID(FK)│───┘   │ TRAN_TYPE_CD    │───│───┐                    │
│ XREF_ACCT_ID(FK)│───────│ TRAN_CAT_CD     │   │   │                    │
└─────────────────┘       │ TRAN_AMT        │   │   │                    │
                          │ TRAN_CARD_NUM   │◀──│───│────────────────────┘
                          │ TRAN_MERCHANT   │   │   │
                          └─────────────────┘   │   │
                                                │   │
┌─────────────────┐       ┌─────────────────┐   │   │
│  TRAN_TYPES     │       │  TRAN_CATS      │   │   │
├─────────────────┤       ├─────────────────┤   │   │
│ TRAN_TYPE (PK)  │◀──────│ TRAN_TYPE_CD(FK)│◀──┘   │
│ TRAN_TYPE_DESC  │       │ TRAN_CAT_CD(PK) │◀──────┘
└─────────────────┘       │ TRAN_CAT_DESC   │
                          └─────────────────┘
```

## 5. Implementation Details

### 5.1 Spark Session Configuration

```python
SparkSession.builder
    .appName("CardDemo_MainframeMigration")
    .config("spark.jars.packages", "za.co.absa.cobrix:spark-cobol_2.12:2.6.9")
    .config("spark.driver.extraJavaOptions", JAVA_17_OPTIONS)
    .config("spark.executor.extraJavaOptions", JAVA_17_OPTIONS)
    .master("local[*]")
    .getOrCreate()
```

### 5.2 Java 17+ Module Access

Java 17+ requires explicit module access for Spark internals:

```
--add-opens=java.base/java.nio=ALL-UNNAMED
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED
--add-opens=java.base/java.lang=ALL-UNNAMED
--add-opens=java.base/java.util=ALL-UNNAMED
```

### 5.3 Cobrix Options

| Option | Value | Purpose |
|--------|-------|---------|
| `copybook` | Path to .cpy file | COBOL structure definition |
| `record_format` | "F" | Fixed-length records |
| `schema_retention_policy` | "collapse_root" | Flatten nested structures |
| `generate_record_id` | "true" | Add sequence numbers |

### 5.4 Data Type Mapping

| COBOL Type | Spark Type | Notes |
|------------|------------|-------|
| PIC 9(n) | IntegerType / LongType | Numeric, unsigned |
| PIC S9(n)V99 | DecimalType | Signed with decimals |
| PIC X(n) | StringType | Alphanumeric |
| COMP / COMP-3 | Varies | Binary packed decimal |

## 6. Project Structure

```
mainframe-migration/
├── input/                          # Source files
│   ├── *.PS                        # EBCDIC data files
│   └── *.cpy                       # COBOL copybooks
├── output/                         # Converted JSON
│   ├── customers/
│   ├── accounts/
│   ├── cards/
│   ├── ... (one folder per dataset)
│   └── conversion_manifest.json    # Conversion metadata
├── docs/
│   ├── DESIGN.md                   # This document
│   └── SETUP.md                    # Setup instructions
├── local_test.py                   # Single dataset test script
├── convert_all.py                  # Comprehensive conversion
├── pyproject.toml                  # UV package configuration
└── README.md                       # Quick start guide
```

## 7. Error Handling

### 7.1 Pre-flight Checks
- Verify input files exist before processing
- Validate copybook syntax during Spark read
- Check Java version compatibility

### 7.2 Runtime Errors
- Per-dataset error capture (conversion continues on failure)
- Detailed error messages in manifest
- Exit codes: 0=success, 1=partial/full failure

### 7.3 Common Issues

| Error | Cause | Solution |
|-------|-------|----------|
| `ClassNotFoundException: Cobrix` | JAR not loaded | Use `spark.jars.packages` |
| `IllegalAccessError` | Java module restrictions | Add `--add-opens` flags |
| `Record length mismatch` | Copybook doesn't match data | Verify RECLN in copybook |

## 8. Testing Strategy

### 8.1 Unit Testing
- Individual dataset conversion validation
- Schema verification against copybook
- Record count validation

### 8.2 Integration Testing
- Full pipeline execution
- Cross-reference integrity checks
- Performance benchmarking

### 8.3 Validation Queries

```python
# Verify customer-account relationship
df_xref = spark.read.json("output/card_xref")
df_cust = spark.read.json("output/customers")
orphans = df_xref.join(df_cust,
    df_xref.XREF_CUST_ID == df_cust.CUST_ID,
    "left_anti"
)
assert orphans.count() == 0, "Orphan cross-references found"
```

## 9. Production Migration Path

### 9.1 AWS Glue 5.0 Deployment

```python
# Glue 5.0 job configuration
glue_client.create_job(
    Name='carddemo-migration',
    Role='GlueServiceRole',
    Command={
        'Name': 'glueetl',
        'ScriptLocation': 's3://bucket/scripts/convert_all.py',
        'PythonVersion': '3'
    },
    GlueVersion='5.0',
    DefaultArguments={
        '--extra-jars': 's3://bucket/libs/spark-cobol_2.12-2.6.9.jar',
        '--additional-python-modules': 'pyspark==3.5.0'
    }
)
```

### 9.2 Key Differences

| Aspect | Local | Glue 5.0 |
|--------|-------|----------|
| Data Source | Local files | S3 |
| JAR Location | Maven auto-download | S3 extra-jars |
| Java Options | Manual config | Managed by Glue |
| Parallelism | `local[*]` | Cluster workers |

## 10. Security Considerations

### 10.1 Sensitive Data
- SSN fields present in customer data
- CVV codes in card data
- User passwords in security file

### 10.2 Recommendations
- Enable encryption at rest for output
- Apply column-level masking in production
- Audit access to converted data
- Consider tokenization for PCI compliance

## 11. Performance Metrics

| Dataset | Records | Conversion Time | Throughput |
|---------|---------|-----------------|------------|
| customers | 50 | ~2s | 25 rec/s |
| daily_transactions | 300 | ~3s | 100 rec/s |
| export_data | 500 | ~4s | 125 rec/s |
| **Total** | **1,136** | **~30s** | **~38 rec/s** |

*Note: Local single-node performance. Production Glue clusters will scale linearly.*

## 12. Future Enhancements

1. **Incremental Processing**: Add support for delta loads
2. **Schema Evolution**: Handle copybook version changes
3. **Data Quality**: Integrate with Great Expectations
4. **Parquet Output**: Add Parquet format for analytics
5. **CDC Integration**: Connect to change data capture streams

## 13. References

- [Cobrix Documentation](https://github.com/AbsaOSS/cobrix)
- [AWS Glue 5.0 Release Notes](https://docs.aws.amazon.com/glue/latest/dg/release-notes.html)
- [CardDemo Application](https://github.com/aws-samples/aws-mainframe-modernization-carddemo)
- [COBOL Copybook Specification](https://www.ibm.com/docs/en/cobol-zos)

---

*Document Version: 1.0*
*Last Updated: December 2024*
