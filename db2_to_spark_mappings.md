# DB2 for z/OS to Spark Data Type Mappings

## Numeric Data Types

| DB2 z/OS Type | Size | Range/Precision | Spark Data Type | Notes |
|---------------|------|-----------------|-----------------|-------|
| `SMALLINT` | 2 bytes | -32,768 to 32,767 | `ShortType` | 16-bit signed integer |
| `INTEGER` / `INT` | 4 bytes | -2.1B to 2.1B | `IntegerType` | 32-bit signed integer |
| `BIGINT` | 8 bytes | -9.2E18 to 9.2E18 | `LongType` | 64-bit signed integer |
| `DECIMAL(p,s)` / `DEC` | (p+2)/2 bytes | p=1-31, s=0-p | `DecimalType(p,s)` | Packed decimal format |
| `NUMERIC(p,s)` | (p+2)/2 bytes | p=1-31, s=0-p | `DecimalType(p,s)` | Alias for DECIMAL |
| `REAL` | 4 bytes | ~7 digits precision | `FloatType` | Single precision IEEE 754 |
| `FLOAT` / `DOUBLE` | 8 bytes | ~15 digits precision | `DoubleType` | Double precision IEEE 754 |
| `DOUBLE PRECISION` | 8 bytes | ~15 digits precision | `DoubleType` | Alias for DOUBLE |
| `DECFLOAT(16)` | 8 bytes | 16 significant digits | `DecimalType(16,0)` | IEEE 754r decimal float |
| `DECFLOAT(34)` | 16 bytes | 34 significant digits | `DecimalType(34,0)` | IEEE 754r decimal float |

## Character String Data Types

| DB2 z/OS Type | Size | Spark Data Type | Notes |
|---------------|------|-----------------|-------|
| `CHAR(n)` | n bytes (1-255) | `StringType` | Fixed-length, EBCDIC padded |
| `VARCHAR(n)` | 2 + n bytes (1-32704) | `StringType` | Variable-length with length prefix |
| `LONG VARCHAR` | 2 + n bytes (max 32700) | `StringType` | Deprecated, use VARCHAR |
| `CLOB(n)` | up to 2GB | `StringType` | Character Large Object |
| `CHAR(n) FOR BIT DATA` | n bytes | `BinaryType` | Binary data in CHAR |
| `VARCHAR(n) FOR BIT DATA` | 2 + n bytes | `BinaryType` | Binary data in VARCHAR |

## Graphic (DBCS) String Data Types

| DB2 z/OS Type | Size | Spark Data Type | Notes |
|---------------|------|-----------------|-------|
| `GRAPHIC(n)` | 2n bytes (1-127) | `StringType` | Fixed-length DBCS (CJK) |
| `VARGRAPHIC(n)` | 2 + 2n bytes | `StringType` | Variable-length DBCS |
| `LONG VARGRAPHIC` | 2 + 2n bytes | `StringType` | Deprecated |
| `DBCLOB(n)` | up to 1GB | `StringType` | Double-byte CLOB |

## Binary Data Types

| DB2 z/OS Type | Size | Spark Data Type | Notes |
|---------------|------|-----------------|-------|
| `BINARY(n)` | n bytes (1-255) | `BinaryType` | Fixed-length binary |
| `VARBINARY(n)` | 2 + n bytes (1-32704) | `BinaryType` | Variable-length binary |
| `BLOB(n)` | up to 2GB | `BinaryType` | Binary Large Object |

## Date and Time Data Types

| DB2 z/OS Type | Size | Format | Spark Data Type | Notes |
|---------------|------|--------|-----------------|-------|
| `DATE` | 4 bytes | YYYY-MM-DD | `DateType` | Internal: packed decimal |
| `TIME` | 3 bytes | HH.MM.SS | `StringType` or custom | No direct Spark equivalent |
| `TIMESTAMP` | 10 bytes | YYYY-MM-DD-HH.MM.SS.nnnnnn | `TimestampType` | Microsecond precision |
| `TIMESTAMP(p)` | 10-12 bytes | Up to 12 fractional digits | `TimestampType` | DB2 11+, nanosecond capable |
| `TIMESTAMP WITH TIME ZONE` | 13 bytes | Includes timezone offset | `TimestampType` | DB2 11+ |

## Special Data Types

| DB2 z/OS Type | Size | Spark Data Type | Notes |
|---------------|------|-----------------|-------|
| `ROWID` | 17-40 bytes | `StringType` | Row identifier, not portable |
| `XML` | up to 2GB | `StringType` | XML document storage |
| `BOOLEAN` | 1 byte | `BooleanType` | DB2 12+ only |

---

## DECIMAL Precision Reference

| DB2 Declaration | Precision | Scale | Spark Type | Example Value |
|-----------------|-----------|-------|------------|---------------|
| `DECIMAL(5)` | 5 | 0 | `DecimalType(5,0)` | 12345 |
| `DECIMAL(5,2)` | 5 | 2 | `DecimalType(5,2)` | 123.45 |
| `DECIMAL(10,2)` | 10 | 2 | `DecimalType(10,2)` | 12345678.90 |
| `DECIMAL(15,4)` | 15 | 4 | `DecimalType(15,4)` | 12345678901.2345 |
| `DECIMAL(31,6)` | 31 | 6 | `DecimalType(31,6)` | Max precision |

---

## NULL Handling

| DB2 Nullable | Spark Handling |
|--------------|----------------|
| `NOT NULL` | `nullable=False` in StructField |
| `NULL` (default) | `nullable=True` in StructField |
| `WITH DEFAULT` | Handle default in ETL logic |

---

## CCSID (Code Page) Considerations

| CCSID | Encoding | Description |
|-------|----------|-------------|
| 37 | EBCDIC | US/Canada English |
| 500 | EBCDIC | International Latin-1 |
| 1047 | EBCDIC | Open Systems Latin-1 |
| 1140 | EBCDIC | US/Canada with Euro |
| 1200 | UTF-16 | Unicode |
| 1208 | UTF-8 | Unicode |
| 930 | EBCDIC | Japanese mixed |
| 935 | EBCDIC | Simplified Chinese |
| 937 | EBCDIC | Traditional Chinese |

```python
# CCSID to Python codec mapping
DB2_CCSID_TO_CODEC = {
    37: 'cp037',
    500: 'cp500',
    1047: 'cp1047',
    1140: 'cp1140',
    1200: 'utf-16',
    1208: 'utf-8',
    930: 'cp930',
    935: 'cp935',
    937: 'cp937',
}
```

---

## Python Mapping Dictionary

```python
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    LongType, ShortType, FloatType, DoubleType,
    DecimalType, DateType, TimestampType, BinaryType, BooleanType
)

DB2_TO_SPARK_MAPPING = {
    # Integer Types
    "SMALLINT": ShortType(),
    "INTEGER": IntegerType(),
    "INT": IntegerType(),
    "BIGINT": LongType(),

    # Decimal/Numeric Types (parameterized)
    "DECIMAL": DecimalType,  # Needs (precision, scale)
    "DEC": DecimalType,
    "NUMERIC": DecimalType,

    # Floating Point Types
    "REAL": FloatType(),
    "FLOAT": DoubleType(),
    "DOUBLE": DoubleType(),
    "DOUBLE PRECISION": DoubleType(),
    "DECFLOAT": DecimalType,  # Needs precision

    # Character Types
    "CHAR": StringType(),
    "CHARACTER": StringType(),
    "VARCHAR": StringType(),
    "LONG VARCHAR": StringType(),
    "CLOB": StringType(),

    # Graphic (DBCS) Types
    "GRAPHIC": StringType(),
    "VARGRAPHIC": StringType(),
    "LONG VARGRAPHIC": StringType(),
    "DBCLOB": StringType(),

    # Binary Types
    "BINARY": BinaryType(),
    "VARBINARY": BinaryType(),
    "BLOB": BinaryType(),
    "CHAR FOR BIT DATA": BinaryType(),
    "VARCHAR FOR BIT DATA": BinaryType(),

    # Date/Time Types
    "DATE": DateType(),
    "TIME": StringType(),  # Or custom TimeType
    "TIMESTAMP": TimestampType(),
    "TIMESTAMP WITH TIME ZONE": TimestampType(),

    # Special Types
    "ROWID": StringType(),
    "XML": StringType(),
    "BOOLEAN": BooleanType(),
}
```

---

## PySpark Schema Builder

```python
from pyspark.sql.types import *
import re

def db2_to_spark_type(db2_type: str) -> DataType:
    """Convert DB2 z/OS data type to Spark DataType."""

    db2_type = db2_type.upper().strip()

    # Handle parameterized types first
    # DECIMAL(p,s) or DECIMAL(p)
    decimal_match = re.match(r'(DECIMAL|DEC|NUMERIC)\s*\(\s*(\d+)\s*(?:,\s*(\d+))?\s*\)', db2_type)
    if decimal_match:
        precision = int(decimal_match.group(2))
        scale = int(decimal_match.group(3)) if decimal_match.group(3) else 0
        return DecimalType(precision, scale)

    # DECFLOAT(16) or DECFLOAT(34)
    decfloat_match = re.match(r'DECFLOAT\s*\(\s*(\d+)\s*\)', db2_type)
    if decfloat_match:
        precision = int(decfloat_match.group(1))
        return DecimalType(precision, 0)

    # CHAR(n), VARCHAR(n), etc. - extract base type
    char_match = re.match(r'(CHAR|CHARACTER|VARCHAR|LONG VARCHAR)\s*\(\s*\d+\s*\)', db2_type)
    if char_match:
        if 'FOR BIT DATA' in db2_type:
            return BinaryType()
        return StringType()

    # GRAPHIC types
    if any(t in db2_type for t in ['GRAPHIC', 'VARGRAPHIC', 'DBCLOB']):
        return StringType()

    # BINARY/VARBINARY
    if re.match(r'(BINARY|VARBINARY)\s*\(\s*\d+\s*\)', db2_type):
        return BinaryType()

    # TIMESTAMP with optional precision
    if db2_type.startswith('TIMESTAMP'):
        return TimestampType()

    # Simple type lookups
    simple_types = {
        'SMALLINT': ShortType(),
        'INTEGER': IntegerType(),
        'INT': IntegerType(),
        'BIGINT': LongType(),
        'REAL': FloatType(),
        'FLOAT': DoubleType(),
        'DOUBLE': DoubleType(),
        'DOUBLE PRECISION': DoubleType(),
        'DATE': DateType(),
        'TIME': StringType(),
        'BLOB': BinaryType(),
        'CLOB': StringType(),
        'XML': StringType(),
        'ROWID': StringType(),
        'BOOLEAN': BooleanType(),
    }

    for key, spark_type in simple_types.items():
        if db2_type.startswith(key):
            return spark_type

    # Default fallback
    return StringType()


def parse_db2_ddl_to_schema(ddl: str) -> StructType:
    """Parse DB2 DDL CREATE TABLE statement to Spark schema."""

    fields = []

    # Extract column definitions
    columns_match = re.search(r'\((.*)\)', ddl, re.DOTALL)
    if not columns_match:
        return StructType(fields)

    columns_str = columns_match.group(1)

    # Split by comma but not within parentheses
    column_defs = re.split(r',(?![^()]*\))', columns_str)

    for col_def in column_defs:
        col_def = col_def.strip()
        if not col_def or col_def.upper().startswith(('PRIMARY', 'FOREIGN', 'CONSTRAINT', 'UNIQUE', 'CHECK')):
            continue

        # Extract column name and type
        parts = col_def.split(None, 2)
        if len(parts) >= 2:
            col_name = parts[0].strip('"')
            col_type = ' '.join(parts[1:])

            # Check for NOT NULL
            nullable = 'NOT NULL' not in col_type.upper()

            # Remove NOT NULL and other constraints for type parsing
            col_type = re.sub(r'\s+(NOT\s+)?NULL.*$', '', col_type, flags=re.IGNORECASE)

            spark_type = db2_to_spark_type(col_type.strip())
            fields.append(StructField(col_name, spark_type, nullable))

    return StructType(fields)
```

---

## Common DB2 z/OS Table Patterns

### Customer Table Example
```sql
CREATE TABLE CUSTOMER (
    CUST_ID         INTEGER NOT NULL,          --> IntegerType
    CUST_NAME       VARCHAR(100),              --> StringType
    CUST_BALANCE    DECIMAL(15,2),             --> DecimalType(15,2)
    CUST_STATUS     CHAR(1),                   --> StringType
    CUST_OPEN_DATE  DATE,                      --> DateType
    CUST_CREDIT_LMT DECIMAL(11,2),             --> DecimalType(11,2)
    CUST_SSN        CHAR(9) FOR BIT DATA,      --> BinaryType (encrypted)
    LAST_UPDATE_TS  TIMESTAMP,                 --> TimestampType
    PRIMARY KEY (CUST_ID)
);
```

### Transaction Table Example
```sql
CREATE TABLE TRANSACTION (
    TXN_ID          BIGINT NOT NULL,           --> LongType
    TXN_AMOUNT      DECIMAL(13,2),             --> DecimalType(13,2)
    TXN_TYPE        CHAR(4),                   --> StringType
    TXN_TIMESTAMP   TIMESTAMP(6),              --> TimestampType
    TXN_STATUS      SMALLINT,                  --> ShortType
    TXN_REFERENCE   VARCHAR(50),               --> StringType
    TXN_MEMO        CLOB(1M),                  --> StringType
    PRIMARY KEY (TXN_ID)
);
```

### Account Table Example
```sql
CREATE TABLE ACCOUNT (
    ACCT_NUM        CHAR(16) NOT NULL,         --> StringType
    ACCT_TYPE       CHAR(2),                   --> StringType
    ACCT_BALANCE    DECIMAL(17,2),             --> DecimalType(17,2)
    INTEREST_RATE   DECFLOAT(16),              --> DecimalType(16,0)
    OPEN_DATE       DATE,                      --> DateType
    MATURITY_DATE   DATE,                      --> DateType
    BRANCH_ID       INTEGER,                   --> IntegerType
    ACCT_XML_DATA   XML,                       --> StringType
    PRIMARY KEY (ACCT_NUM)
);
```

---

## DB2 z/OS Specific Considerations

### 1. EBCDIC Encoding
```python
# DB2 z/OS stores CHAR/VARCHAR in EBCDIC by default
# Must convert to UTF-8 for Spark processing

def convert_ebcdic_column(df, column_name, ccsid=37):
    """Convert EBCDIC column to UTF-8 string."""
    from pyspark.sql.functions import udf

    codec = DB2_CCSID_TO_CODEC.get(ccsid, 'cp037')

    @udf(StringType())
    def ebcdic_to_utf8(data):
        if data is None:
            return None
        if isinstance(data, bytes):
            return data.decode(codec)
        return data

    return df.withColumn(column_name, ebcdic_to_utf8(df[column_name]))
```

### 2. Packed Decimal Handling
```python
# DB2 DECIMAL is stored as packed decimal (same as COBOL COMP-3)
# When reading via JDBC, driver handles conversion automatically
# When reading raw unload files, manual unpacking required

def unpack_decimal(packed_bytes: bytes, precision: int, scale: int) -> Decimal:
    """Unpack DB2 packed decimal from raw bytes."""
    from decimal import Decimal

    # Each byte contains 2 digits, last nibble is sign
    digits = []
    for byte in packed_bytes[:-1]:
        digits.append(byte >> 4)
        digits.append(byte & 0x0F)
    # Last byte: high nibble is digit, low nibble is sign
    digits.append(packed_bytes[-1] >> 4)
    sign_nibble = packed_bytes[-1] & 0x0F

    # Build number string
    num_str = ''.join(str(d) for d in digits)
    if scale > 0:
        num_str = num_str[:-scale] + '.' + num_str[-scale:]

    # Apply sign (D = negative, C/F = positive)
    if sign_nibble == 0x0D:
        num_str = '-' + num_str

    return Decimal(num_str)
```

### 3. DATE/TIME Internal Storage
```python
# DB2 DATE is stored as packed decimal YYYYMMDD internally
# DB2 TIME is stored as packed decimal HHMMSS internally
# JDBC driver handles conversion; raw files need parsing

def parse_db2_date(packed_date: int) -> date:
    """Parse DB2 internal date format."""
    from datetime import date
    year = packed_date // 10000
    month = (packed_date % 10000) // 100
    day = packed_date % 100
    return date(year, month, day)

def parse_db2_timestamp(ts_string: str) -> datetime:
    """Parse DB2 timestamp string format."""
    from datetime import datetime
    # Format: YYYY-MM-DD-HH.MM.SS.nnnnnn
    return datetime.strptime(ts_string, '%Y-%m-%d-%H.%M.%S.%f')
```

---

## JDBC Connection Example

```python
# Reading DB2 z/OS via JDBC in Spark
def read_db2_table(spark, table_name: str) -> DataFrame:
    jdbc_url = "jdbc:db2://hostname:port/database"

    return spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table_name) \
        .option("user", "username") \
        .option("password", "password") \
        .option("driver", "com.ibm.db2.jcc.DB2Driver") \
        .option("fetchsize", "10000") \
        .load()
```

---

## Comparison: DB2 z/OS vs VSAM Data Types

| Data Category | DB2 z/OS | VSAM/COBOL | Spark |
|---------------|----------|------------|-------|
| Small Integer | SMALLINT | PIC S9(4) COMP | ShortType |
| Integer | INTEGER | PIC S9(9) COMP | IntegerType |
| Big Integer | BIGINT | PIC S9(18) COMP | LongType |
| Packed Decimal | DECIMAL(p,s) | PIC S9(p-s)V9(s) COMP-3 | DecimalType(p,s) |
| Float | REAL | COMP-1 | FloatType |
| Double | DOUBLE | COMP-2 | DoubleType |
| Fixed String | CHAR(n) | PIC X(n) | StringType |
| Variable String | VARCHAR(n) | N/A | StringType |
| Date | DATE | PIC 9(8) | DateType |
| Timestamp | TIMESTAMP | PIC X(26) | TimestampType |
| Binary | BLOB | PIC X(n) binary | BinaryType |
