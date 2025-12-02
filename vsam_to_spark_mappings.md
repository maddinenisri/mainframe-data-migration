# VSAM to Spark Data Type Mappings

## COBOL Picture Clause to Spark Data Type Conversion

### Numeric Data Types

| COBOL PIC Clause | COBOL Type | VSAM Storage | Spark Data Type | Notes |
|------------------|------------|--------------|-----------------|-------|
| `PIC 9(n)` | Zoned Decimal (Display) | EBCDIC digits | `StringType` or `LongType` | Each digit = 1 byte |
| `PIC 9(n) COMP` | Binary | 2/4/8 bytes | `ShortType`/`IntegerType`/`LongType` | Based on n: 1-4→2bytes, 5-9→4bytes, 10-18→8bytes |
| `PIC 9(n) COMP-1` | Single Float | 4 bytes | `FloatType` | IEEE 754 single precision |
| `PIC 9(n) COMP-2` | Double Float | 8 bytes | `DoubleType` | IEEE 754 double precision |
| `PIC 9(n) COMP-3` | Packed Decimal | (n+1)/2 bytes | `DecimalType(n,0)` | BCD format, last nibble = sign |
| `PIC 9(n) COMP-4` | Binary (same as COMP) | 2/4/8 bytes | `ShortType`/`IntegerType`/`LongType` | Alias for COMP |
| `PIC 9(n) COMP-5` | Native Binary | 2/4/8 bytes | `ShortType`/`IntegerType`/`LongType` | Full binary range |
| `PIC S9(n)` | Signed Zoned Decimal | EBCDIC | `LongType` | Sign in last byte zone |
| `PIC S9(n) COMP` | Signed Binary | 2/4/8 bytes | `ShortType`/`IntegerType`/`LongType` | Two's complement |
| `PIC S9(n) COMP-3` | Signed Packed | (n+1)/2 bytes | `DecimalType(n,0)` | Sign nibble: C=+, D=- |
| `PIC 9(n)V9(m)` | Implied Decimal | n+m bytes | `DecimalType(n+m, m)` | V = implied decimal point |
| `PIC S9(n)V9(m) COMP-3` | Signed Packed Decimal | (n+m+1)/2 bytes | `DecimalType(n+m, m)` | Most common for money |

### Alphanumeric Data Types

| COBOL PIC Clause | COBOL Type | VSAM Storage | Spark Data Type | Notes |
|------------------|------------|--------------|-----------------|-------|
| `PIC X(n)` | Alphanumeric | n bytes EBCDIC | `StringType` | Requires EBCDIC→ASCII conversion |
| `PIC A(n)` | Alphabetic | n bytes EBCDIC | `StringType` | Letters and spaces only |
| `PIC X(n) JUSTIFIED RIGHT` | Right-justified Alpha | n bytes | `StringType` | Pad left with spaces |
| `FILLER` | Filler/Reserved | n bytes | Skip/`NullType` | Usually ignored in conversion |

### Special Data Types

| COBOL PIC Clause | COBOL Type | VSAM Storage | Spark Data Type | Notes |
|------------------|------------|--------------|-----------------|-------|
| `PIC 9(8)` (date) | Date as number | 8 bytes | `DateType` | Format: YYYYMMDD |
| `PIC 9(6)` (date) | Date as number | 6 bytes | `DateType` | Format: YYMMDD (century handling needed) |
| `PIC 9(8)` (time) | Time as number | 8 bytes | `TimestampType` | Format: HHMMSSCC |
| `PIC 9(14)` (timestamp) | Timestamp | 14 bytes | `TimestampType` | Format: YYYYMMDDHHMMSS |
| `PIC X(26)` (ISO timestamp) | ISO Timestamp string | 26 bytes | `TimestampType` | ISO 8601 format |

### Binary/Raw Data Types

| COBOL PIC Clause | COBOL Type | VSAM Storage | Spark Data Type | Notes |
|------------------|------------|--------------|-----------------|-------|
| `PIC X(n)` (binary) | Raw bytes | n bytes | `BinaryType` | When content is non-text |
| `POINTER` | Address | 4/8 bytes | `LongType` | Rarely migrated |
| `OBJECT REFERENCE` | Object pointer | varies | Skip | Not applicable for migration |

---

## COMP Type Size Reference

| Digits (n) | COMP/COMP-4 Size | COMP-5 Size | Spark Type |
|------------|------------------|-------------|------------|
| 1-4 | 2 bytes | 2 bytes | `ShortType` |
| 5-9 | 4 bytes | 4 bytes | `IntegerType` |
| 10-18 | 8 bytes | 8 bytes | `LongType` |

---

## COMP-3 (Packed Decimal) Byte Calculation

```
Bytes = (n + 1) / 2  (rounded up)

Example: PIC S9(7)V99 COMP-3
- Total digits = 7 + 2 = 9
- Bytes = (9 + 1) / 2 = 5 bytes
- Spark: DecimalType(9, 2)
```

---

## Sign Representation

### Zoned Decimal (Display Numeric)
| Sign | Last Byte Zone Nibble |
|------|-----------------------|
| Positive | C or F |
| Negative | D |
| Unsigned | F |

### Packed Decimal (COMP-3)
| Sign | Last Nibble |
|------|-------------|
| Positive | C or F |
| Negative | D |
| Unsigned | F |

---

## Python Mapping Dictionary

```python
VSAM_TO_SPARK_MAPPING = {
    # Alphanumeric
    "PIC X": "StringType",
    "PIC A": "StringType",

    # Zoned Decimal (Display)
    "PIC 9": "LongType",
    "PIC S9": "LongType",

    # Binary (COMP/COMP-4/COMP-5)
    "PIC 9 COMP": {
        "1-4": "ShortType",
        "5-9": "IntegerType",
        "10-18": "LongType"
    },
    "PIC S9 COMP": {
        "1-4": "ShortType",
        "5-9": "IntegerType",
        "10-18": "LongType"
    },

    # Floating Point
    "COMP-1": "FloatType",
    "COMP-2": "DoubleType",

    # Packed Decimal
    "PIC 9 COMP-3": "DecimalType",
    "PIC S9 COMP-3": "DecimalType",

    # With Decimal Places (V notation)
    "PIC 9V9": "DecimalType",
    "PIC S9V9": "DecimalType",
    "PIC 9V9 COMP-3": "DecimalType",
    "PIC S9V9 COMP-3": "DecimalType",
}

# EBCDIC to ASCII conversion required for X and A types
REQUIRES_EBCDIC_CONVERSION = ["PIC X", "PIC A"]

# Binary format (no EBCDIC conversion needed)
BINARY_FORMATS = ["COMP", "COMP-1", "COMP-2", "COMP-3", "COMP-4", "COMP-5"]
```

---

## PySpark Schema Builder Example

```python
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    LongType, ShortType, FloatType, DoubleType,
    DecimalType, DateType, TimestampType, BinaryType
)

def cobol_to_spark_type(pic_clause: str, length: int, decimal_places: int = 0) -> DataType:
    """Convert COBOL PIC clause to Spark DataType."""

    pic = pic_clause.upper().strip()

    # Alphanumeric
    if pic.startswith("PIC X") or pic.startswith("PIC A"):
        return StringType()

    # Packed Decimal (COMP-3)
    if "COMP-3" in pic:
        precision = length + decimal_places
        return DecimalType(precision, decimal_places)

    # Single Precision Float
    if "COMP-1" in pic:
        return FloatType()

    # Double Precision Float
    if "COMP-2" in pic:
        return DoubleType()

    # Binary (COMP, COMP-4, COMP-5)
    if any(c in pic for c in ["COMP-4", "COMP-5", "COMP"]):
        if length <= 4:
            return ShortType()
        elif length <= 9:
            return IntegerType()
        else:
            return LongType()

    # Zoned Decimal with decimal places
    if "V" in pic or decimal_places > 0:
        precision = length + decimal_places
        return DecimalType(precision, decimal_places)

    # Plain Zoned Decimal
    if pic.startswith("PIC 9") or pic.startswith("PIC S9"):
        if length <= 4:
            return ShortType()
        elif length <= 9:
            return IntegerType()
        else:
            return LongType()

    # Default fallback
    return StringType()
```

---

## Common VSAM Record Patterns

### Customer Record Example
```cobol
01 CUSTOMER-RECORD.
   05 CUST-ID           PIC 9(10).           --> LongType
   05 CUST-NAME         PIC X(50).           --> StringType
   05 CUST-BALANCE      PIC S9(9)V99 COMP-3. --> DecimalType(11,2)
   05 CUST-STATUS       PIC X(1).            --> StringType
   05 CUST-OPEN-DATE    PIC 9(8).            --> DateType (YYYYMMDD)
   05 CUST-CREDIT-LIMIT PIC 9(7)V99 COMP-3.  --> DecimalType(9,2)
```

### Transaction Record Example
```cobol
01 TRANSACTION-RECORD.
   05 TXN-ID            PIC 9(15).           --> LongType
   05 TXN-AMOUNT        PIC S9(11)V99 COMP-3.--> DecimalType(13,2)
   05 TXN-TYPE          PIC X(4).            --> StringType
   05 TXN-TIMESTAMP     PIC 9(14).           --> TimestampType
   05 TXN-STATUS        PIC 9(2) COMP.       --> ShortType
```

---

## EBCDIC Code Page Considerations

| Code Page | Region | Notes |
|-----------|--------|-------|
| CP037 | US/Canada | Most common |
| CP273 | Germany | Umlauts |
| CP284 | Spain | Ñ character |
| CP285 | UK | Pound symbol |
| CP500 | International | Multi-language |
| CP1047 | Unix/Open Systems | C-compatible |

```python
# EBCDIC to ASCII conversion in Python
def ebcdic_to_ascii(data: bytes, codepage: str = 'cp037') -> str:
    return data.decode(codepage)
```
