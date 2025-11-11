# Installation Guide

Complete installation instructions for the IC-D-LOAD CRM Reconciliation Pipeline.

---

## Quick Install

```bash
# Navigate to project directory
cd IC-D-LOAD

# Install core dependencies
pip install -r requirements-minimal.txt

# Verify installation
python3 -c "import psycopg2, pandas, loguru, requests; print('✓ All core packages installed')"
```

---

## System Requirements

### Python Version
- **Python 3.9+** (Recommended: Python 3.11)

Check your Python version:
```bash
python3 --version
```

### Operating Systems
- ✅ Linux (Ubuntu, Debian, CentOS, etc.)
- ✅ macOS
- ✅ Windows 10/11

---

## Installation Options

### Option 1: Minimal Installation (Reconciliation Only)

**Best for**: Running reconciliation pipeline only

```bash
pip install -r requirements-minimal.txt
```

**Includes:**
- psycopg2-binary (PostgreSQL)
- pandas (Data processing)
- loguru (Logging)
- requests (API client)
- pyodbc (SQL Server - optional)

### Option 2: Full Installation (Complete Project)

**Best for**: Full IC-D-LOAD project with transformations

```bash
pip install -r requirements.txt
```

**Includes:**
- All minimal requirements
- duckdb (Transformations)
- pydantic (Validation)
- pytest (Testing)
- Development tools

### Option 3: Virtual Environment (Recommended)

**Best for**: Isolated environment, no system conflicts

```bash
# Create virtual environment
python3 -m venv venv

# Activate virtual environment
# On Linux/macOS:
source venv/bin/activate

# On Windows:
venv\Scripts\activate

# Install dependencies
pip install -r requirements-minimal.txt

# Verify
which python3  # Should show venv/bin/python3
```

---

## Manual Installation

If `requirements.txt` fails, install packages individually:

### Core Packages (Required)

```bash
# PostgreSQL adapter
pip install psycopg2-binary

# Data processing
pip install pandas

# Logging
pip install loguru

# HTTP requests
pip install requests
```

### SQL Server (Optional - for legacy CRM)

```bash
pip install pyodbc
```

### Additional Tools (Optional)

```bash
# Transformations
pip install duckdb

# Validation
pip install pydantic

# Testing
pip install pytest pytest-asyncio
```

---

## Platform-Specific Instructions

### Ubuntu/Debian

```bash
# Install system dependencies
sudo apt-get update
sudo apt-get install -y python3-pip python3-dev libpq-dev

# Install Python packages
pip3 install -r requirements-minimal.txt
```

### CentOS/RHEL

```bash
# Install system dependencies
sudo yum install -y python3-pip python3-devel postgresql-devel

# Install Python packages
pip3 install -r requirements-minimal.txt
```

### macOS

```bash
# Install Homebrew (if not installed)
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# Install Python and PostgreSQL
brew install python@3.11 postgresql

# Install Python packages
pip3 install -r requirements-minimal.txt
```

### Windows

```bash
# Install Python from python.org (3.11+)
# Download: https://www.python.org/downloads/

# Install packages
pip install -r requirements-minimal.txt

# If psycopg2 fails, try:
pip install psycopg2-binary --no-binary :all:
```

---

## Database Drivers

### PostgreSQL Driver (psycopg2)

**Already included** in `psycopg2-binary`

Test connection:
```python
import psycopg2
print(f"psycopg2 version: {psycopg2.__version__}")
```

### SQL Server Driver (pyodbc)

**For legacy CRM connection**

#### Linux
```bash
# Install ODBC drivers
curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -
curl https://packages.microsoft.com/config/ubuntu/20.04/prod.list | sudo tee /etc/apt/sources.list.d/mssql-release.list
sudo apt-get update
sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18

# Install pyodbc
pip install pyodbc
```

#### macOS
```bash
# Install ODBC drivers
brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
brew update
brew install msodbcsql18 mssql-tools18

# Install pyodbc
pip install pyodbc
```

#### Windows
```bash
# Download ODBC Driver 18 for SQL Server
# https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server

# Install pyodbc
pip install pyodbc
```

---

## Verification

### Verify All Packages

```bash
# Run verification script
python3 -c "
import sys
packages = ['psycopg2', 'pandas', 'loguru', 'requests']
missing = []

for pkg in packages:
    try:
        __import__(pkg)
        print(f'✓ {pkg}')
    except ImportError:
        print(f'✗ {pkg} - NOT INSTALLED')
        missing.append(pkg)

if missing:
    print(f'\nMissing packages: {missing}')
    sys.exit(1)
else:
    print('\n✓ All core packages installed successfully!')
"
```

### Check Package Versions

```bash
# List installed packages
pip list | grep -E 'psycopg2|pandas|loguru|requests|pyodbc'
```

Expected output:
```
loguru        0.7.2
pandas        2.1.0
psycopg2-binary  2.9.9
requests      2.31.0
```

### Test PostgreSQL Connection

```bash
python3 -c "
from postgres_connection_manager import PostgreSQLManager
pg = PostgreSQLManager()
if pg.test_connection():
    print('✓ PostgreSQL connection successful!')
else:
    print('✗ PostgreSQL connection failed')
pg.close()
"
```

---

## Troubleshooting

### Issue: pip command not found

**Solution:**
```bash
# Ubuntu/Debian
sudo apt-get install python3-pip

# macOS
python3 -m ensurepip --upgrade

# Windows - Reinstall Python with pip
```

### Issue: psycopg2 installation fails

**Error:** `Error: pg_config executable not found`

**Solution:**
```bash
# Ubuntu/Debian
sudo apt-get install libpq-dev python3-dev

# macOS
brew install postgresql

# Or use binary package
pip install psycopg2-binary
```

### Issue: Permission denied

**Solution:**
```bash
# Use --user flag
pip install --user -r requirements-minimal.txt

# Or use virtual environment (recommended)
python3 -m venv venv
source venv/bin/activate
pip install -r requirements-minimal.txt
```

### Issue: pyodbc installation fails on Linux

**Solution:**
```bash
# Install ODBC development files
sudo apt-get install unixodbc-dev

# Then install pyodbc
pip install pyodbc
```

### Issue: SSL certificate error

**Solution:**
```bash
# Upgrade pip and certificates
pip install --upgrade pip setuptools certifi

# Or use --trusted-host
pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org -r requirements-minimal.txt
```

---

## Upgrade Dependencies

### Upgrade All Packages

```bash
# Upgrade pip first
pip install --upgrade pip

# Upgrade all packages
pip install --upgrade -r requirements-minimal.txt
```

### Upgrade Individual Package

```bash
pip install --upgrade psycopg2-binary
pip install --upgrade pandas
```

---

## Uninstallation

### Remove All Packages

```bash
pip uninstall -y -r requirements-minimal.txt
```

### Remove Virtual Environment

```bash
# Deactivate virtual environment
deactivate

# Remove directory
rm -rf venv/
```

---

## Development Setup

### For Development & Testing

```bash
# Install full requirements
pip install -r requirements.txt

# Install in editable mode
pip install -e .

# Install pre-commit hooks
pip install pre-commit
pre-commit install
```

---

## Docker Installation (Alternative)

If you prefer Docker:

```dockerfile
# Create Dockerfile
FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements-minimal.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements-minimal.txt

# Copy project files
COPY . .

CMD ["python3", "crm_reconciliation_pipeline.py"]
```

**Build and run:**
```bash
docker build -t crm-reconciliation .
docker run crm-reconciliation
```

---

## Next Steps

After successful installation:

1. **Test connection**: `python3 postgres_connection_manager.py`
2. **Setup staging**: `python3 setup_staging.py`
3. **Run reconciliation**: `python3 run_reconciliation.py`

---

## Support

For installation issues:
1. Check this guide's Troubleshooting section
2. Verify Python version: `python3 --version`
3. Check pip version: `pip --version`
4. Review error messages carefully
5. Search error messages online

---

**Installation complete!** 🎉

Now proceed to: [BASIC_USAGE_SNIPPETS.md](BASIC_USAGE_SNIPPETS.md)
