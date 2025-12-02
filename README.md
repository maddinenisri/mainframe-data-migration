# Mainframe Data Migration

Local testing environment for converting AWS CardDemo EBCDIC mainframe data to JSON format, mimicking AWS Glue 5.0 runtime.

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
   # Install UV if not already installed
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```

### Installation

```bash
# Clone and navigate to project
cd mainframe-migration

# Install dependencies (creates .venv automatically)
uv sync
```

### Run Conversion

```bash
# Convert all datasets
uv run python convert_all.py

# Or convert single dataset (customers only)
uv run python local_test.py
```

## Output

Converted JSON files are saved to `output/`:

```
output/
├── customers/              # 50 records
├── accounts/               # 50 records
├── cards/                  # 50 records
├── card_xref/              # 50 records
├── daily_transactions/     # 300 records
├── transaction_types/      # 7 records
├── transaction_categories/ # 18 records
├── users/                  # 10 records
├── discount_groups/        # 51 records
├── category_balances/      # 50 records
├── export_data/            # 500 records
└── conversion_manifest.json
```

## Project Structure

```
mainframe-migration/
├── input/                  # EBCDIC data files & copybooks
├── output/                 # Converted JSON output
├── docs/
│   └── DESIGN.md          # Detailed design document
├── local_test.py          # Single dataset test
├── convert_all.py         # Full conversion script
├── pyproject.toml         # UV/Python configuration
└── README.md              # This file
```

## Configuration

### AWS Glue 5.0 Compatibility

This environment matches AWS Glue 5.0 specifications:

| Component | Version |
|-----------|---------|
| Spark | 3.5.0 |
| Java | 17+ |
| Scala | 2.12 |
| Cobrix | 2.6.9 |

### Java 17+ Requirements

Java 17+ requires module access flags for Spark. These are automatically configured in the scripts:

```python
JAVA_17_OPTIONS = " ".join([
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.util=ALL-UNNAMED",
])
```

## Dataset Reference

| Dataset | Source File | Records | Description |
|---------|-------------|---------|-------------|
| customers | CUSTDATA.PS | 50 | Customer demographics |
| accounts | ACCTDATA.PS | 50 | Account balances & limits |
| cards | CARDDATA.PS | 50 | Credit card details |
| card_xref | CARDXREF.PS | 50 | Card-customer mapping |
| daily_transactions | DALYTRAN.PS | 300 | Transaction history |
| transaction_types | TRANTYPE.PS | 7 | Type reference data |
| transaction_categories | TRANCATG.PS | 18 | Category reference |
| users | USRSEC.PS | 10 | User credentials |
| discount_groups | DISCGRP.PS | 51 | Discount configuration |
| category_balances | TCATBALF.PS | 50 | Category summaries |
| export_data | EXPORT.DATA.PS | 500 | Multi-record export |

## Troubleshooting

### Common Issues

**Issue**: `java.lang.ClassNotFoundException: za.co.absa.cobrix`
**Solution**: Cobrix JAR is downloaded automatically via `spark.jars.packages`. Ensure internet connectivity.

**Issue**: `IllegalAccessError: module java.base does not open`
**Solution**: Java 17+ requires `--add-opens` flags. Already configured in scripts.

**Issue**: `Record length mismatch`
**Solution**: Verify the copybook matches the data file structure. Check RECLN comments in copybooks.

### Verify Java Version

```bash
# Must be 17 or higher
java -version

# If using SDKMAN
sdk use java 21.0.2-open
```

### Clean Rebuild

```bash
# Remove cached dependencies and output
rm -rf .venv output uv.lock
uv sync
uv run python convert_all.py
```

## Sample Output

### Customer Record (JSON)
```json
{
  "CUST_ID": 1,
  "CUST_FIRST_NAME": "Immanuel",
  "CUST_MIDDLE_NAME": "Madeline",
  "CUST_LAST_NAME": "Kessler",
  "CUST_ADDR_LINE_1": "618 Deshaun Route",
  "CUST_ADDR_STATE_CD": "NC",
  "CUST_ADDR_COUNTRY_CD": "USA",
  "CUST_SSN": 20973888,
  "CUST_DOB_YYYYMMDD": "1961-06-08",
  "CUST_FICO_CREDIT_SCORE": 274
}
```

### Transaction Record (JSON)
```json
{
  "DALYTRAN_ID": "0000000000000001",
  "DALYTRAN_TYPE_CD": "01",
  "DALYTRAN_CAT_CD": 5001,
  "DALYTRAN_AMT": 125.50,
  "DALYTRAN_MERCHANT_NAME": "Amazon.com",
  "DALYTRAN_CARD_NUM": "4111111111111111",
  "DALYTRAN_ORIG_TS": "2024-01-15-10.30.45.123456"
}
```

## AWS Glue Deployment

For production deployment to AWS Glue 5.0:

1. Upload `convert_all.py` to S3
2. Upload Cobrix JAR to S3 (or use `--additional-python-modules`)
3. Create Glue job with:
   - Glue Version: 5.0
   - Worker Type: G.1X or higher
   - Extra JARs: s3://bucket/spark-cobol_2.12-2.6.9.jar

See [docs/DESIGN.md](docs/DESIGN.md) for detailed production migration guide.

## License

This project is for internal use. The CardDemo application is licensed under Apache 2.0 by AWS.

## References

- [Cobrix GitHub](https://github.com/AbsaOSS/cobrix)
- [AWS Glue Documentation](https://docs.aws.amazon.com/glue/)
- [CardDemo Application](https://github.com/aws-samples/aws-mainframe-modernization-carddemo)
