---
name: sql-connection-manager
description: Manage SQL Server database connections with ADODB, connection pooling, and retry logic. Use this skill for robust database connectivity in the extraction pipeline.
---

# SQL Connection Manager

## Overview

Provides robust SQL Server database connection management with ADODB support, connection pooling, retry logic with exponential backoff, and comprehensive error handling. Essential infrastructure for reliable data extraction from SQL Server databases.

## When to Use This Skill

- **Establish database connections** with proper configuration
- **Connection pooling** for performance optimization
- **Retry logic** for transient connection failures
- **Credential management** with secure storage
- **Connection string templates** for different environments
- **Monitor connection health** and performance

## Core Capabilities

### 1. Basic Connection

```python
from scripts.connection_manager import ConnectionManager

manager = ConnectionManager(
    server="your_server",
    database="CRMICALPS",
    trusted_connection=True
)

# Get connection
with manager.get_connection() as conn:
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM Company")
    results = cursor.fetchall()
```

### 2. Connection Pooling

```python
# Initialize with connection pool
manager = ConnectionManager(
    server="your_server",
    database="CRMICALPS",
    pool_size=5,
    max_overflow=10
)

# Connections are automatically pooled and reused
conn1 = manager.get_connection()
conn2 = manager.get_connection()  # Reuses pool
```

### 3. Retry Logic

```python
# Automatic retry with exponential backoff
manager = ConnectionManager(
    server="your_server",
    database="CRMICALPS",
    max_retries=3,
    retry_delay=1.0  # seconds
)

# Connection automatically retries on transient failures
conn = manager.get_connection()
```

## Resources

See `scripts/connection_manager.py` for implementation details.
