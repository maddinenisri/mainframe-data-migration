# DB2 Migration Module

This module provides tools for migrating IBM DB2 for z/OS tables to modern databases (PostgreSQL, JSON).

## Quick Start

### Export DB2 Tables to JSON (Mock Mode)

```bash
# From project root
uv run python db2/scripts/db2_to_json.py
```

### Migrate DB2 to PostgreSQL (Mock Mode)

```bash
# Export to JSON (default when PostgreSQL not available)
uv run python db2/scripts/db2_to_postgres.py

# Or with PostgreSQL target
TARGET=postgres uv run python db2/scripts/db2_to_postgres.py
```

## Project Structure

```
db2/
├── schema/
│   ├── source/                    # Original DB2 DDL
│   │   ├── TRNTYPE.ddl
│   │   └── TRNTYCAT.ddl
│   └── target/                    # Converted PostgreSQL DDL
│       └── 001_create_tables.sql
├── dcl/                           # DCLGEN files (COBOL bindings)
│   ├── DCLTRTYP.dcl
│   └── DCLTRCAT.dcl
├── scripts/
│   ├── db2_to_json.py            # Export DB2 → JSON
│   └── db2_to_postgres.py        # Migrate DB2 → PostgreSQL
├── libs/                          # JDBC drivers (download separately)
│   ├── db2jcc4.jar               # IBM DB2 JDBC driver
│   └── postgresql-42.7.4.jar     # PostgreSQL JDBC driver
├── output/                        # Exported data
└── README.md                      # This file
```

## Operating Modes

### Mock Mode (Default)

Uses embedded sample data for testing without DB2 access:

```bash
# Implicit mock mode (default)
uv run python db2/scripts/db2_to_json.py

# Explicit mock mode
DB2_MODE=mock uv run python db2/scripts/db2_to_json.py
```

### JDBC Mode (Production)

Connects to actual DB2 database:

```bash
# Set connection parameters
export DB2_MODE=jdbc
export DB2_URL="jdbc:db2://mainframe.example.com:5023/CARDDEMO"
export DB2_USER="db2admin"
export DB2_PASSWORD="secret"

# Run export
uv run python db2/scripts/db2_to_json.py
```

## Environment Variables

### DB2 Connection

| Variable | Default | Description |
|----------|---------|-------------|
| `DB2_MODE` | `mock` | `mock` for sample data, `jdbc` for real DB2 |
| `DB2_URL` | `jdbc:db2://localhost:50000/CARDDEMO` | DB2 JDBC URL |
| `DB2_USER` | `db2admin` | DB2 username |
| `DB2_PASSWORD` | (empty) | DB2 password |

### PostgreSQL Connection

| Variable | Default | Description |
|----------|---------|-------------|
| `TARGET` | `json` | `json` for files, `postgres` for PostgreSQL |
| `PG_URL` | `jdbc:postgresql://localhost:5432/carddemo` | PostgreSQL JDBC URL |
| `PG_USER` | `postgres` | PostgreSQL username |
| `PG_PASSWORD` | `postgres` | PostgreSQL password |

## JDBC Driver Setup

### DB2 Driver

Download from IBM Fix Central or Maven:

```bash
mkdir -p db2/libs
# From Maven (if available)
curl -L -o db2/libs/db2jcc4.jar \
  "https://repo1.maven.org/maven2/com/ibm/db2/jcc/db2jcc/11.5.8.0/db2jcc-11.5.8.0.jar"
```

### PostgreSQL Driver

```bash
curl -L -o db2/libs/postgresql-42.7.4.jar \
  "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar"
```

## Tables Migrated

| Table | Records | Description |
|-------|---------|-------------|
| `transaction_type` | 7 | Transaction type codes |
| `transaction_type_category` | 18 | Category details per type |

## Schema Conversion

### DB2 Source

```sql
CREATE TABLE CARDDEMO.TRANSACTION_TYPE (
    TR_TYPE         CHAR(2)      NOT NULL,
    TR_DESCRIPTION  VARCHAR(50)  NOT NULL,
    PRIMARY KEY(TR_TYPE)
) CCSID EBCDIC;
```

### PostgreSQL Target

```sql
CREATE TABLE transaction_type (
    tr_type         CHAR(2)      NOT NULL,
    tr_description  VARCHAR(50)  NOT NULL,
    PRIMARY KEY(tr_type)
);
```

### Key Transformations

1. **Identifiers**: Uppercase → lowercase
2. **CCSID EBCDIC**: Removed (PostgreSQL uses UTF-8)
3. **STOGROUP/BUFFERPOOL**: Removed (z/OS specific)
4. **VARCHAR handling**: JDBC driver converts automatically

## Sample Output

### JSON Format

```json
{
  "tr_type": "01",
  "tr_description": "Purchase"
}
```

### Export Manifest

```json
{
  "tables": [
    {
      "name": "transaction_type",
      "source_table": "CARDDEMO.TRANSACTION_TYPE",
      "status": "success",
      "record_count": 7
    }
  ],
  "total_records": 25,
  "mode": "mock"
}
```

## Comparison: EBCDIC vs DB2 Migration

| Aspect | EBCDIC Files | DB2 Tables |
|--------|--------------|------------|
| Input | Binary .PS files | SQL tables |
| Schema | COBOL copybooks | DDL/DCL files |
| Parser | Cobrix library | JDBC driver |
| Encoding | Manual conversion | Automatic |
| Script | `convert_all.py` | `db2_to_postgres.py` |

## Troubleshooting

### ClassNotFoundException: DB2Driver

```
Ensure db2jcc4.jar is in db2/libs/ and readable
```

### Connection refused

```
Check DB2 hostname, port, and firewall rules
Verify DB2 listener is running
```

### Authentication failed

```
Verify DB2_USER and DB2_PASSWORD environment variables
Check user permissions on CARDDEMO database
```

## References

- [IBM DB2 JDBC Driver](https://www.ibm.com/docs/en/db2/11.5?topic=jdbc-driver)
- [PostgreSQL JDBC Driver](https://jdbc.postgresql.org/)
- [AWS SCT - DB2 to PostgreSQL](https://docs.aws.amazon.com/SchemaConversionTool/)
