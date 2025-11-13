# MetaSkill: CRM Schema Discovery & Complex View JOINs

**Last Updated:** 2025-11-11  
**Status:** Production Ready ✅

---

## Overview

This skill documents the **discovery process** and **JOIN strategies** for extracting data from Sage CRM (CRMICALPS) where critical fields like phone numbers and email addresses are **NOT stored in the primary entity tables** but in separate normalized tables/views.

### When to Use This Skill

- Phone/email/fax fields appear in entity documentation but return "invalid column name" errors
- Primary entity tables lack expected contact information fields
- Need to discover the actual database schema vs. documented schema
- Working with normalized CRM systems where contact data is in separate link tables
- Troubleshooting extraction queries that fail due to missing columns

---

## The Problem: Phone & Email Mystery

### Symptom

```sql
-- This query FAILS:
SELECT 
    Comp_CompanyId,
    Comp_Name,
    Comp_PhoneNumber,  -- ❌ Invalid column name!
    Comp_EmailAddress  -- ❌ Invalid column name!
FROM Company
```

**Error:**
```
Invalid column name 'Comp_PhoneNumber'
Invalid column name 'Comp_EmailAddress'
```

### Root Cause

Sage CRM stores phone and email data in **separate normalized tables/views**:
- `Phone` / `PhoneLink` → `vPersonPhone`, `vCompanyPhone`, `vAccountPhone`
- `Email` / `EmailLink` → `vPersonEmail`, `vCompanyEmail`, `vAccountEmail`

This is a **many-to-many** relationship:
- One person can have multiple phone numbers (Business, Mobile, Home, Fax)
- One company can have multiple emails (Business, Support, Sales)

---

## Discovery Process: 4-Step Method

### Step 1: Discover Actual Table Schema

```python
"""
Discover what columns ACTUALLY exist in a table
"""
import pyodbc
import pandas as pd
from config import get_connection_string

def discover_columns(table_name):
    """Get actual columns from INFORMATION_SCHEMA"""
    conn_str = get_connection_string()
    conn = pyodbc.connect(conn_str)
    
    query = f"""
    SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = '{table_name}'
    ORDER BY ORDINAL_POSITION
    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# Check what's REALLY in the Company table
columns = discover_columns('Company')
phone_cols = columns[columns['COLUMN_NAME'].str.contains('Phone|Email', case=False)]
print(phone_cols)  # Will show: NO phone/email columns!
```

**Result:** Company and Person tables have NO direct phone/email columns.

---

### Step 2: Find Related Phone/Email Views

```python
"""
Search for tables/views containing phone or email data
"""
import pyodbc
from config import get_connection_string

conn = pyodbc.connect(get_connection_string())
cursor = conn.cursor()

# Get all tables/views
tables = cursor.tables(tableType='TABLE,VIEW').fetchall()

# Filter for phone/email related
phone_email_tables = [
    t.table_name 
    for t in tables 
    if 'phone' in t.table_name.lower() or 'email' in t.table_name.lower()
]

print("Phone/Email Tables/Views:")
for table in sorted(phone_email_tables):
    print(f"  - {table}")

conn.close()
```

**Key Discoveries:**
- `vPersonPhone` - Person phone numbers
- `vPersonEmail` - Person email addresses
- `vCompanyPhone` - Company phone numbers
- `vCompanyEmail` - Company email addresses
- `Phone` / `PhoneLink` - Raw tables (use views instead)
- `Email` / `EmailLink` - Raw tables (use views instead)

---

### Step 3: Analyze View Structure

```python
"""
Examine the structure of phone/email views
"""
def analyze_view(view_name):
    conn = pyodbc.connect(get_connection_string())
    
    # Get column structure
    query = f"""
    SELECT COLUMN_NAME, DATA_TYPE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = '{view_name}'
    ORDER BY ORDINAL_POSITION
    """
    columns = pd.read_sql(query, conn)
    
    # Get sample data
    sample = pd.read_sql(f"SELECT TOP 1 * FROM {view_name}", conn)
    
    conn.close()
    
    print(f"\n{view_name} Structure:")
    print("=" * 60)
    print("\nColumns:")
    for _, row in columns.iterrows():
        print(f"  {row['COLUMN_NAME']:40s} {row['DATA_TYPE']}")
    
    print("\nSample Row:")
    for col in sample.columns:
        print(f"  {col}: {sample[col].iloc[0]}")

# Analyze key views
analyze_view('vPersonPhone')
analyze_view('vPersonEmail')
analyze_view('vCompanyPhone')
analyze_view('vCompanyEmail')
```

**Critical Findings:**

#### vPersonPhone Structure
```
Phon_FullNumber      - Complete formatted phone number ✓
Phon_PhoneId         - Phone record ID
Phon_AreaCode        - Area code (optional)
Phon_CountryCode     - Country code (optional)
Phon_Number          - Raw number
PLink_RecordId       - Person ID (JOIN KEY!) ✓
PLink_Type           - Type: Business, Mobile, Home, Fax ✓
```

#### vPersonEmail Structure
```
Emai_EmailAddress    - Email address ✓
Emai_EmailId         - Email record ID
ELink_RecordID       - Person ID (JOIN KEY!) ✓
ELink_Type           - Type: Business, Personal ✓
ELink_EntityID       - Entity type (13 = Person)
```

#### vCompanyPhone Structure
```
Phon_FullNumber      - Complete formatted phone number ✓
PLink_RecordId       - Company ID (JOIN KEY!) ✓
PLink_Type           - Type: Business, Fax ✓
```

#### vCompanyEmail Structure
```
Emai_EmailAddress    - Email address ✓
ELink_RecordID       - Company ID (JOIN KEY!) ✓
ELink_Type           - Type: Business ✓
ELink_EntityID       - Entity type (5 = Company)
```

---

### Step 4: Build Correct JOIN Query

Now we know:
1. ✅ Phone/email NOT in primary tables
2. ✅ Use views: `vPersonPhone`, `vPersonEmail`, `vCompanyPhone`, `vCompanyEmail`
3. ✅ JOIN keys: `PLink_RecordId` (phone), `ELink_RecordID` (email)
4. ✅ Filter by type: `PLink_Type` / `ELink_Type`

---

## Solution: Working Extraction Queries

### Company with Phone & Email

```sql
SELECT DISTINCT
    c.Comp_CompanyId,
    c.Comp_Name,
    c.Comp_Type,
    c.Comp_Status,
    c.Comp_WebSite,
    c.Comp_CreatedDate,
    c.Comp_UpdatedDate,
    
    -- Address data
    a.Addr_Address1 AS Address_Street1,
    a.Addr_City AS Address_City,
    a.Addr_Country AS Address_Country,
    
    -- Phone data (Business phone only)
    cp.Phon_FullNumber AS Company_Phone,
    cp.Phon_PhoneId AS Phone_Id,
    cp.PLink_Type AS Phone_Type,
    
    -- Email data (Business email only)
    ce.Emai_EmailAddress AS Company_Email,
    ce.Emai_EmailId AS Email_Id,
    ce.ELink_Type AS Email_Type
    
FROM [CRMICALPS].[dbo].[Company] c
LEFT JOIN [CRMICALPS].[dbo].[Address] a 
    ON c.Comp_PrimaryAddressId = a.Addr_AddressId
    
-- JOIN to phone view (Business phone only)
LEFT JOIN [CRMICALPS].[dbo].[vCompanyPhone] cp 
    ON c.Comp_CompanyId = cp.PLink_RecordId 
    AND cp.PLink_Type = 'Business'
    
-- JOIN to email view (Business email only)
LEFT JOIN [CRMICALPS].[dbo].[vCompanyEmail] ce 
    ON c.Comp_CompanyId = ce.ELink_RecordID 
    AND ce.ELink_Type = 'Business'
    
WHERE c.Comp_CompanyId IS NOT NULL
ORDER BY c.Comp_CompanyId
```

**Key Points:**
- ✅ Use `LEFT JOIN` to keep companies without phone/email
- ✅ Filter by `PLink_Type = 'Business'` to get primary phone
- ✅ Filter by `ELink_Type = 'Business'` to get primary email
- ✅ Use `DISTINCT` to handle multiple phones/emails per company

---

### Person with Phone & Email

```sql
SELECT DISTINCT
    p.Pers_PersonId,
    p.Pers_CompanyId,
    p.Pers_FirstName,
    p.Pers_LastName,
    p.Pers_Title,
    p.Pers_Department,
    p.Pers_Status,
    p.Pers_CreatedDate,
    p.Pers_UpdatedDate,
    
    -- Company data
    c.Comp_Name AS Company_Name,
    c.Comp_WebSite AS Company_WebSite,
    
    -- Address data
    a.Addr_Address1 AS Address_Street1,
    a.Addr_City AS Address_City,
    a.Addr_Country AS Address_Country,
    
    -- Phone data (Business phone)
    pp.Phon_FullNumber AS Person_Phone,
    pp.Phon_PhoneId AS Phone_Id,
    pp.PLink_Type AS Phone_Type,
    
    -- Email data (Business email)
    pe.Emai_EmailAddress AS Person_Email,
    pe.Emai_EmailId AS Email_Id,
    pe.ELink_Type AS Email_Type
    
FROM [CRMICALPS].[dbo].[Person] p
LEFT JOIN [CRMICALPS].[dbo].[Company] c 
    ON p.Pers_CompanyId = c.Comp_CompanyId
LEFT JOIN [CRMICALPS].[dbo].[Address] a 
    ON p.Pers_PrimaryAddressId = a.Addr_AddressId
    
-- JOIN to phone view (Business phone)
LEFT JOIN [CRMICALPS].[dbo].[vPersonPhone] pp 
    ON p.Pers_PersonId = pp.PLink_RecordId 
    AND pp.PLink_Type = 'Business'
    
-- JOIN to email view (Business email)
LEFT JOIN [CRMICALPS].[dbo].[vPersonEmail] pe 
    ON p.Pers_PersonId = pe.ELink_RecordID 
    AND pe.ELink_Type = 'Business'
    
WHERE p.Pers_PersonId IS NOT NULL
ORDER BY p.Pers_PersonId
```

---

## Advanced: Multiple Phone Numbers

### Get ALL Phone Types per Person

```sql
SELECT
    p.Pers_PersonId,
    p.Pers_FirstName,
    p.Pers_LastName,
    
    -- Get all phone types
    MAX(CASE WHEN pp.PLink_Type = 'Business' THEN pp.Phon_FullNumber END) AS Phone_Business,
    MAX(CASE WHEN pp.PLink_Type = 'Mobile' THEN pp.Phon_FullNumber END) AS Phone_Mobile,
    MAX(CASE WHEN pp.PLink_Type = 'Home' THEN pp.Phon_FullNumber END) AS Phone_Home,
    MAX(CASE WHEN pp.PLink_Type = 'Fax' THEN pp.Phon_FullNumber END) AS Phone_Fax
    
FROM [CRMICALPS].[dbo].[Person] p
LEFT JOIN [CRMICALPS].[dbo].[vPersonPhone] pp 
    ON p.Pers_PersonId = pp.PLink_RecordId
WHERE p.Pers_PersonId IS NOT NULL
GROUP BY p.Pers_PersonId, p.Pers_FirstName, p.Pers_LastName
ORDER BY p.Pers_PersonId
```

**Result:**
```
Pers_PersonId | FirstName | LastName | Phone_Business    | Phone_Mobile     | Phone_Home | Phone_Fax
-------------|-----------|----------|-------------------|------------------|------------|-------------
1037         | Nicolas   | Karst    | 04 38 78 37 49   | 06 12 34 56 78  | NULL       | NULL
```

---

## Common Pitfalls & Solutions

### Pitfall 1: Duplicate Rows from Multiple Phones

**Problem:**
```sql
-- Returns 3 rows for one person (3 phone numbers)
SELECT p.*, pp.Phon_FullNumber
FROM Person p
LEFT JOIN vPersonPhone pp ON p.Pers_PersonId = pp.PLink_RecordId
WHERE p.Pers_PersonId = 1037
```

**Solution A:** Filter by phone type
```sql
AND pp.PLink_Type = 'Business'  -- Get only Business phone
```

**Solution B:** Use DISTINCT
```sql
SELECT DISTINCT p.Pers_PersonId, p.Pers_FirstName, pp.Phon_FullNumber
```

**Solution C:** Aggregate with MAX/MIN
```sql
SELECT 
    p.Pers_PersonId,
    MAX(pp.Phon_FullNumber) AS Primary_Phone
GROUP BY p.Pers_PersonId
```

---

### Pitfall 2: Wrong JOIN Key

**Problem:**
```sql
-- WRONG: Using wrong key
LEFT JOIN vPersonEmail pe 
    ON p.Pers_PersonId = pe.Emai_EmailId  -- ❌ WRONG!
```

**Solution:**
```sql
-- CORRECT: Use the Link table's RecordID
LEFT JOIN vPersonEmail pe 
    ON p.Pers_PersonId = pe.ELink_RecordID  -- ✅ CORRECT!
```

**Remember:**
- Phone views: JOIN on `PLink_RecordId`
- Email views: JOIN on `ELink_RecordID`

---

### Pitfall 3: Missing EntityID Filter

For email views that aggregate multiple entity types:

**Problem:**
```sql
-- Gets emails for ALL entities (Person, Company, Case, etc.)
SELECT * FROM vEmailCompanyAndPerson
```

**Solution:**
```sql
-- Filter by EntityID
SELECT * FROM vEmailCompanyAndPerson
WHERE ELink_EntityID = 13  -- 13 = Person, 5 = Company
```

**Entity ID Reference:**
- `5` = Company
- `13` = Person
- `6` = Case
- `7` = Opportunity

---

## Python Extraction Template

### Complete Working Example

```python
"""
Extract entities WITH phone and email from separate views
"""
import pyodbc
import pandas as pd
from pathlib import Path
from datetime import datetime
from config import get_connection_string


def extract_persons_with_contact_info():
    """
    Extract persons with phone and email data from separate views
    """
    query = """
    SELECT DISTINCT
        -- Person base fields
        p.Pers_PersonId,
        p.Pers_FirstName,
        p.Pers_LastName,
        p.Pers_Title,
        p.Pers_Department,
        
        -- Company denormalized
        c.Comp_Name AS Company_Name,
        
        -- Phone (Business only)
        pp.Phon_FullNumber AS Person_Phone,
        pp.PLink_Type AS Phone_Type,
        
        -- Email (Business only)
        pe.Emai_EmailAddress AS Person_Email,
        pe.ELink_Type AS Email_Type
        
    FROM [CRMICALPS].[dbo].[Person] p
    LEFT JOIN [CRMICALPS].[dbo].[Company] c 
        ON p.Pers_CompanyId = c.Comp_CompanyId
    LEFT JOIN [CRMICALPS].[dbo].[vPersonPhone] pp 
        ON p.Pers_PersonId = pp.PLink_RecordId 
        AND pp.PLink_Type = 'Business'
    LEFT JOIN [CRMICALPS].[dbo].[vPersonEmail] pe 
        ON p.Pers_PersonId = pe.ELink_RecordID 
        AND pe.ELink_Type = 'Business'
    WHERE p.Pers_PersonId IS NOT NULL
    ORDER BY p.Pers_PersonId
    """
    
    # Execute query
    conn_str = get_connection_string()
    conn = pyodbc.connect(conn_str)
    df = pd.read_sql(query, conn)
    conn.close()
    
    # Stats
    print(f"✅ Extracted {len(df):,} persons")
    print(f"📞 With phone: {df['Person_Phone'].notna().sum():,}")
    print(f"📧 With email: {df['Person_Email'].notna().sum():,}")
    
    # Save to Bronze
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = Path("bronze_layer") / f"Bronze_Person_{timestamp}.csv"
    output_path.parent.mkdir(exist_ok=True)
    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"💾 Saved to {output_path}")
    
    return df


if __name__ == "__main__":
    df = extract_persons_with_contact_info()
```

---

## Validation Checklist

Before finalizing extraction queries:

### ✅ Schema Discovery
- [ ] Ran `discover_columns()` on primary tables
- [ ] Confirmed phone/email fields are NOT in primary tables
- [ ] Found correct phone/email views
- [ ] Analyzed view structure and JOIN keys

### ✅ Query Construction
- [ ] Used `LEFT JOIN` to preserve records without phone/email
- [ ] Joined on correct keys: `PLink_RecordId` / `ELink_RecordID`
- [ ] Filtered by type: `PLink_Type` / `ELink_Type`
- [ ] Used `DISTINCT` or `GROUP BY` to handle duplicates

### ✅ Testing
- [ ] Tested query returns expected row count
- [ ] Verified phone numbers are populated
- [ ] Verified email addresses are populated
- [ ] Checked for duplicate rows
- [ ] Validated data quality (non-null values match expectations)

### ✅ Production
- [ ] Saved extraction script with timestamp
- [ ] Documented JOIN keys in code comments
- [ ] Added error handling for missing data
- [ ] Logged statistics (rows extracted, phone count, email count)

---

## Real-World Results

### Actual Extraction Statistics (IC_LOAD Project)

**Company Extraction:**
- Total rows: **2,889**
- With phone: **2,198 (76%)**
- With email: **264 (9%)**

**Person Extraction:**
- Total rows: **6,077**
- With phone: **2,269 (37%)**
- With email: **3,271 (54%)**

**Opportunity Extraction:**
- Total rows: **697**
- Total forecast value: **$363,725,335.52**
- Weighted forecast: **$102,854,622.35**

---

## Quick Reference: View to Entity Mapping

| Entity    | Phone View       | Email View       | JOIN Key (Phone)   | JOIN Key (Email)   |
|-----------|------------------|------------------|--------------------|--------------------|
| Company   | vCompanyPhone    | vCompanyEmail    | PLink_RecordId     | ELink_RecordID     |
| Person    | vPersonPhone     | vPersonEmail     | PLink_RecordId     | ELink_RecordID     |
| Account   | vAccountPhone    | vAccountEmail    | PLink_RecordId     | ELink_RecordID     |

**Phone Type Values:** `Business`, `Mobile`, `Home`, `Fax`  
**Email Type Values:** `Business`, `Personal`

---

## Troubleshooting

### Issue: No phone numbers extracted

**Check:**
1. Is `PLink_Type` filter too restrictive? Try removing it temporarily
2. Are there actually phones for these records in the view?
3. Is the JOIN key correct? Should be `PLink_RecordId`

**Debug Query:**
```sql
-- See what phone types exist
SELECT DISTINCT PLink_Type, COUNT(*) as count
FROM vPersonPhone
GROUP BY PLink_Type
```

---

### Issue: Too many rows (duplicates)

**Check:**
1. Missing `DISTINCT` keyword?
2. Person has multiple phones of same type?
3. Missing phone type filter?

**Fix:**
```sql
-- Add DISTINCT
SELECT DISTINCT ...

-- Or filter by type
AND pp.PLink_Type = 'Business'

-- Or aggregate
GROUP BY p.Pers_PersonId
```

---

### Issue: Query too slow

**Optimization:**
1. Add WHERE clause to limit rows
2. Create indexes on JOIN keys (if allowed)
3. Use `WITH (NOLOCK)` for read-only queries
4. Limit to recent records: `WHERE p.Pers_CreatedDate > '2020-01-01'`

---

## Files Created

**Discovery Scripts:**
- `discover_schema.py` - Basic schema discovery
- `discover_full_schema.py` - Complete column listing
- `check_phone_email_views.py` - View structure analysis

**Extraction Scripts:**
- `extract_with_phones.py` - Production extraction with phone/email
- `extract_company_person_opportunity.py` - Multi-entity extraction

**Output Files:**
- `Bronze_Company_YYYYMMDD_HHMMSS.csv`
- `Bronze_Person_YYYYMMDD_HHMMSS.csv`
- `Bronze_Opportunity_YYYYMMDD_HHMMSS.csv`

---

## Lessons Learned

1. **Don't trust documentation** - Always discover actual schema first
2. **Views hide complexity** - Sage CRM uses views to abstract normalized link tables
3. **Phone/Email are many-to-many** - Always filter by type or aggregate
4. **LEFT JOIN preserves data** - Use LEFT JOIN to keep records without phone/email
5. **DISTINCT is your friend** - Use it to handle multiple contact points
6. **Test incrementally** - Build JOINs one at a time, test each step

---

## Related Skills

- `sql-schema-discovery/SKILL.md` - Database schema discovery patterns
- `dataframe-dataclass-converter/SKILL.md` - Converting extracted data to dataclasses
- `pipeline_async/SKILL.md` - Async extraction pipelines

---

## Version History

- **v1.0** (2025-11-11) - Initial documentation of phone/email discovery and JOIN patterns
- Extracted 9,663 total rows with 4,467 phone numbers and 3,535 email addresses
- Solved the "invalid column name" mystery for Sage CRM contact data

---

## Author Notes

> This skill emerged from real-world debugging where documented fields (Comp_PhoneNumber, Pers_EmailAddress) didn't exist in the database. The discovery process revealed that Sage CRM uses a normalized many-to-many model for all contact points (phone, email, address), requiring JOIN queries to views that aggregate the link tables.
> 
> Key insight: When fields are documented but missing, look for related views with "Link" in the table name or entity-prefixed views (vPersonPhone, vCompanyEmail, etc.).

---

**End of MetaSkill Document**



