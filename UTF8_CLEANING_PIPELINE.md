# UTF-8 CSV Cleaning Pipeline

Systematic UTF-8 character cleaning and SQL generation pipeline for CSV/TSV files.

## Overview

This pipeline processes CSV/TSV files to:
1. **Discover schema** from CSV files using pandas
2. **Detect and replace** problematic UTF-8 characters using targeted regex
3. **Generate SQL statements** for staging and production tables
4. **Track statistics** of all UTF-8 character replacements
5. **Deploy to PostgreSQL** locally or via SSH connection

## Architecture

```
CSV Files → Schema Discovery → UTF-8 Cleaning → SQL Generation → PostgreSQL
                                      ↓
                               Statistics Tracking
```

## Components

### 1. `csv_utf8_cleaner.py`

Main script for CSV parsing, UTF-8 cleaning, and SQL generation.

**Key Features:**
- Schema discovery from CSV using pandas
- Configurable UTF-8 character replacement patterns
- Targeted column cleaning (specify which columns to clean)
- SQL generation for staging and production tables
- Summary statistics of replacements

**Usage:**

```bash
# Single file with specific column
python3 csv_utf8_cleaner.py --input data/contacts.csv --column email --output sql/contacts.sql

# Multiple columns
python3 csv_utf8_cleaner.py --input data/companies.csv --column name --column description

# TSV file
python3 csv_utf8_cleaner.py --input data/data.tsv --separator "\t" --column notes

# Batch processing
python3 csv_utf8_cleaner.py --input-dir data/csv_files/ --column email --output-dir sql/

# With statistics output
python3 csv_utf8_cleaner.py --input data/contacts.csv --column email --stats-output stats/contacts_stats.csv

# Preserve accented characters
python3 csv_utf8_cleaner.py --input data/contacts.csv --column name --preserve-accents
```

**Arguments:**

| Argument | Description | Required |
|----------|-------------|----------|
| `--input` | Input CSV/TSV file path | Yes (or `--input-dir`) |
| `--input-dir` | Directory for batch processing | Yes (or `--input`) |
| `--column` | Column name to clean (can specify multiple) | No (defaults to all string columns) |
| `--output` | Output SQL file path | No (auto-generated) |
| `--output-dir` | Output directory (batch mode) | No (default: `sql/`) |
| `--table` | SQL table name | No (derived from filename) |
| `--schema` | Database schema name | No (default: `staging`) |
| `--separator` | Field separator | No (default: `,`) |
| `--preserve-accents` | Preserve accented characters | No (default: false) |
| `--stats-output` | Statistics output CSV path | No |

### 2. `ssh_postgres_manager.py`

SSH-based PostgreSQL connection manager using paramiko.

**Key Features:**
- SSH tunnel to PostgreSQL
- Execute SQL via SSH
- Execute commands as sudo
- SFTP file transfer
- SQL file execution

**Usage:**

```python
from ssh_postgres_manager import SSHPostgreSQLManager

# Connect via SSH
with SSHPostgreSQLManager(
    ssh_host="your-server.com",
    ssh_user="ubuntu",
    ssh_key_path="~/.ssh/id_rsa",
    pg_host="localhost",
    pg_database="postgres",
    pg_user="postgres",
    pg_password="your_password"
) as ssh_pg:
    # Test connection
    ssh_pg.test_connection()

    # Execute query
    df = ssh_pg.execute_query_df("SELECT * FROM my_table LIMIT 10")

    # Execute SQL file
    ssh_pg.execute_sql_file("sql/schema.sql")

    # Execute command as sudo
    ssh_pg.execute_remote_command("systemctl restart postgresql", sudo=True)
```

### 3. `Makefile`

Orchestrates the entire pipeline with simple commands.

**Usage:**

```bash
# Show help
make help

# Clean single CSV file
make clean-csv FILE=data/contacts.csv COLUMN=email

# Clean multiple columns
make clean-csv FILE=data/companies.csv COLUMN=name COLUMN=description

# Clean TSV file
make clean-tsv FILE=data/data.tsv COLUMN=notes

# Batch processing
make clean-batch DIR=data/csv_files/ COLUMN=email

# Deploy SQL to PostgreSQL
make deploy-sql FILE=sql/contacts.sql

# Deploy via SSH
make deploy-ssh FILE=sql/contacts.sql

# Full pipeline (clean + deploy)
make pipeline FILE=data/contacts.csv COLUMN=email

# Test connections
make test

# View statistics
make stats

# Git status
make git-status
```

**Environment Variables:**

```bash
export PG_HOST=localhost
export PG_PORT=5432
export PG_DATABASE=postgres
export PG_USER=postgres
export PG_PASSWORD=your_password
export SCHEMA=staging
export PRESERVE_ACCENTS=false

# For SSH deployment
export SSH_HOST=your-server.com
export SSH_USER=ubuntu
export SSH_KEY=~/.ssh/id_rsa
```

### 4. `run_utf8_pipeline.sh`

Shell script for pipeline execution with configuration.

**Usage:**

```bash
# Single file
./run_utf8_pipeline.sh single data/contacts.csv email

# Multiple columns (comma-separated)
./run_utf8_pipeline.sh single data/companies.csv "name,description"

# Batch processing
./run_utf8_pipeline.sh batch data/csv_files/ email

# Deploy SQL
./run_utf8_pipeline.sh deploy sql/contacts.sql

# Deploy via SSH
./run_utf8_pipeline.sh deploy-ssh sql/contacts.sql

# Test connections
./run_utf8_pipeline.sh test

# View statistics
./run_utf8_pipeline.sh stats
```

## UTF-8 Character Replacements

The pipeline replaces problematic UTF-8 characters using regex patterns:

### Smart Quotes
- `'` `'` → `'` (smart single quotes → straight single quote)
- `"` `"` → `"` (smart double quotes → straight double quote)
- `«` `»` → `"` (French quotes → straight double quote)

### Dashes
- `–` → `-` (en dash → hyphen)
- `—` → `-` (em dash → hyphen)
- `−` → `-` (minus sign → hyphen)

### Special Characters
- `…` → `...` (ellipsis → three periods)
- `©` → `(c)` (copyright symbol)
- `®` → `(R)` (registered trademark)
- `™` → `(TM)` (trademark)
- `•` → `*` (bullet → asterisk)
- `°` → `deg` (degree sign)

### Spaces
- `\u00A0` → ` ` (non-breaking space → regular space)
- Various Unicode spaces → regular space

### Accented Characters (Optional)
- `é` `è` `ê` `ë` → `e`
- `á` `à` `â` `ä` → `a`
- `í` `ì` `î` `ï` → `i`
- `ó` `ò` `ô` `ö` → `o`
- `ú` `ù` `û` `ü` → `u`
- `ñ` → `n`
- `ç` → `c`

**Note:** Use `--preserve-accents` flag to skip accented character replacements.

## SQL Generation

The pipeline generates SQL for both staging and production tables.

### Staging Table

```sql
CREATE TABLE IF NOT EXISTS staging.my_table_staging (
    id SERIAL PRIMARY KEY,
    column1 VARCHAR(255) NULL,
    column2 INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    status VARCHAR(50) DEFAULT 'pending'
);
```

### Production Table

```sql
CREATE TABLE IF NOT EXISTS staging.my_table (
    column1 VARCHAR(255) NULL,
    column2 INTEGER NOT NULL
);
```

### Insert Statements

```sql
INSERT INTO staging.my_table_staging (column1, column2) VALUES ('value1', 123);
```

### Staging to Production Transfer

```sql
INSERT INTO staging.my_table (column1, column2)
SELECT column1, column2
FROM staging.my_table_staging
WHERE status = 'validated';
```

## Statistics Tracking

The pipeline tracks detailed statistics of UTF-8 character replacements.

**Output CSV Format:**

| column | character | count | rows_modified |
|--------|-----------|-------|---------------|
| email | `'` | 15 | 10 |
| email | `–` | 5 | 5 |
| name | `"` | 23 | 18 |

**Console Output:**

```
UTF-8 CHARACTER REPLACEMENT STATISTICS
=======================================
Total replacements: 43

Column: email
  Rows modified: 10
  Character replacements:
    "'"                            →     15 occurrences
    "–"                            →      5 occurrences

Column: name
  Rows modified: 18
  Character replacements:
    """                            →     23 occurrences
```

## Workflow Examples

### Example 1: Single CSV File

```bash
# 1. Clean CSV and generate SQL
python3 csv_utf8_cleaner.py \
    --input data/contacts.csv \
    --column email \
    --column firstname \
    --column lastname \
    --output sql/contacts.sql \
    --stats-output stats/contacts_stats.csv

# 2. Review SQL
cat sql/contacts.sql

# 3. Deploy to PostgreSQL
make deploy-sql FILE=sql/contacts.sql

# 4. View statistics
cat stats/contacts_stats.csv
```

### Example 2: Batch Processing

```bash
# 1. Batch clean all CSV files
make clean-batch DIR=data/csv_files/ COLUMN=email COLUMN=notes

# 2. View statistics
make stats

# 3. Deploy all SQL files
for sql_file in sql/*.sql; do
    make deploy-sql FILE=$sql_file
done
```

### Example 3: SSH Deployment

```bash
# 1. Set SSH configuration
export SSH_HOST=your-server.com
export SSH_USER=ubuntu
export SSH_KEY=~/.ssh/id_rsa

# 2. Test connection
./run_utf8_pipeline.sh test

# 3. Process and deploy
make pipeline-ssh FILE=data/contacts.csv COLUMN=email
```

### Example 4: TSV Processing

```bash
# Process TSV file
python3 csv_utf8_cleaner.py \
    --input data/export.tsv \
    --separator "\t" \
    --column description \
    --output sql/export.sql
```

## PostgreSQL Connection

### Local Connection

```bash
# Using psql directly
PGPASSWORD=your_password psql \
    -h localhost \
    -p 5432 \
    -U postgres \
    -d postgres \
    -f sql/contacts.sql
```

### SSH Connection

```python
from ssh_postgres_manager import SSHPostgreSQLManager

ssh_pg = SSHPostgreSQLManager(
    ssh_host="your-server.com",
    ssh_user="ubuntu",
    ssh_key_path="~/.ssh/id_rsa",
    pg_host="localhost",
    pg_database="postgres",
    pg_user="postgres",
    pg_password="password"
)

ssh_pg.connect()
ssh_pg.execute_sql_file("sql/contacts.sql")
ssh_pg.disconnect()
```

## Directory Structure

```
.
├── csv_utf8_cleaner.py          # Main cleaning script
├── ssh_postgres_manager.py       # SSH PostgreSQL manager
├── Makefile                      # Pipeline orchestration
├── run_utf8_pipeline.sh          # Shell script runner
├── requirements.txt              # Python dependencies
├── data/                         # Input CSV/TSV files
│   └── csv_files/               # Batch processing directory
├── sql/                          # Generated SQL files
└── stats/                        # UTF-8 replacement statistics
```

## Installation

```bash
# 1. Install Python dependencies
pip install -r requirements.txt

# 2. Create directories
make setup

# 3. Test connections
make test
```

## Requirements

### Python Packages

- `pandas>=2.1.0` - Data manipulation
- `numpy>=1.24.0` - Numerical computing
- `psycopg2-binary>=2.9.9` - PostgreSQL adapter
- `paramiko>=3.3.0` - SSH connections
- `loguru>=0.7.2` - Logging

### System Requirements

- Python 3.8+
- PostgreSQL (local or remote)
- SSH access (for remote deployment)
- psql command-line tool (for local deployment)

## Configuration

### Environment Variables

Create a `.env` file or export variables:

```bash
# PostgreSQL
export PG_HOST=localhost
export PG_PORT=5432
export PG_DATABASE=postgres
export PG_USER=postgres
export PG_PASSWORD=your_password

# SSH (for remote deployment)
export SSH_HOST=your-server.com
export SSH_USER=ubuntu
export SSH_KEY=~/.ssh/id_rsa
export SUDO_PASSWORD=your_sudo_password

# Pipeline options
export SCHEMA=staging
export PRESERVE_ACCENTS=false
export DEPLOY_AFTER_CLEAN=false
```

### Custom UTF-8 Replacements

Edit `csv_utf8_cleaner.py` to add custom replacement patterns:

```python
custom_replacements = {
    r'your-pattern': 'replacement',
    r'\u1234': 'custom',
}

cleaner = UTF8Cleaner(custom_replacements=custom_replacements)
```

## Troubleshooting

### Issue: SSH Connection Failed

**Solution:**
1. Check SSH credentials
2. Verify SSH key permissions: `chmod 600 ~/.ssh/id_rsa`
3. Test SSH manually: `ssh -i ~/.ssh/id_rsa user@host`

### Issue: PostgreSQL Connection Failed

**Solution:**
1. Check PostgreSQL is running
2. Verify credentials
3. Check firewall rules
4. Test connection: `psql -h localhost -U postgres -d postgres`

### Issue: UTF-8 Encoding Errors

**Solution:**
1. Ensure CSV files are UTF-8 encoded
2. Try reading with different encoding: `pd.read_csv(file, encoding='latin1')`
3. Use `--preserve-accents` flag

### Issue: SQL Execution Failed

**Solution:**
1. Review generated SQL file
2. Check table/schema exists
3. Verify column types match
4. Test SQL manually in psql

## Best Practices

1. **Always review statistics** before deploying to production
2. **Test on staging** before production deployment
3. **Backup data** before running SQL updates
4. **Use version control** for generated SQL files
5. **Monitor UTF-8 patterns** and update regex as needed
6. **Validate schema** matches target database
7. **Use staging tables** for validation before production

## Advanced Usage

### Custom Schema Discovery

```python
from csv_utf8_cleaner import CSVSchemaDiscovery

df = pd.read_csv('data/contacts.csv')
schema_df = CSVSchemaDiscovery.discover_schema(df)
print(schema_df)
```

### Programmatic Usage

```python
from csv_utf8_cleaner import UTF8Cleaner, SQLGenerator

# Clean data
cleaner = UTF8Cleaner()
df_clean = cleaner.clean_dataframe(df, target_columns=['email', 'name'])

# Print statistics
cleaner.print_statistics()

# Generate SQL
sql_gen = SQLGenerator(table_name='contacts', schema='staging')
sql = sql_gen.generate_create_table(schema_df, staging=True)
print(sql)
```

### SSH with Sudo Commands

```python
ssh_pg.execute_remote_command(
    "systemctl restart postgresql",
    sudo=True
)
```

## References

- [Pandas Documentation](https://pandas.pydata.org/docs/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [Paramiko Documentation](https://www.paramiko.org/)
- [Unicode Character Reference](https://unicode-table.com/)

## Support

For issues or questions:
1. Check the troubleshooting section
2. Review generated SQL and statistics
3. Check logs in console output
4. Verify PostgreSQL connection

## License

Part of the IC-D-LOAD project for CRM data reconciliation.
