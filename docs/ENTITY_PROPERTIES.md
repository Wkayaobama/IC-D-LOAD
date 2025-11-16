# IC_Load Entity Properties Complete Reference

This document defines all properties for each entity extractor, including denormalized fields from JOINs.

---

## 1. Company Entity

### Base Properties (from Company table)
- `Comp_CompanyId` (PK)
- `Comp_Name`
- `Comp_Website`
- `Comp_Type`
- `Comp_Status`
- `Comp_Source`
- `Comp_Territory`
- `Comp_Sector`
- `Comp_Revenue`
- `Comp_Employees`
- `Comp_EmailAddress`
- `Comp_PhoneCountryCode`
- `Comp_PhoneCityCode`
- `Comp_PhoneNumber`
- `Comp_FaxCountryCode`
- `Comp_FaxCityCode`
- `Comp_FaxNumber`
- `Comp_CreatedDate`
- `Comp_UpdatedDate`
- `Comp_CreatedBy`
- `Comp_UpdatedBy`

### Denormalized Properties (from Address table JOIN)
- `Addr_AddressId`
- `Addr_Street1`
- `Addr_Street2`
- `Addr_Street3`
- `Addr_City`
- `Addr_State`
- `Addr_PostCode`
- `Addr_Country`

### Metadata (added during extraction)
- `bronze_extracted_at`
- `bronze_source_file`
- `_extraction_source`
- `_record_count`

### SQL Query Pattern
```sql
CREATE OR REPLACE VIEW Processed_Companies AS
SELECT DISTINCT
    -- Base Company Properties
    c.Comp_CompanyId,
    c.Comp_Name,
    c.Comp_Website,
    c.Comp_Type,
    c.Comp_Status,
    c.Comp_Source,
    c.Comp_Territory,
    c.Comp_Sector,
    c.Comp_Revenue,
    c.Comp_Employees,
    c.Comp_EmailAddress,
    c.Comp_PhoneCountryCode,
    c.Comp_PhoneCityCode,
    c.Comp_PhoneNumber,
    c.Comp_CreatedDate,
    c.Comp_UpdatedDate,

    -- Denormalized Address Properties
    a.Addr_AddressId,
    a.Addr_Street1,
    a.Addr_Street2,
    a.Addr_City,
    a.Addr_State,
    a.Addr_PostCode,
    a.Addr_Country,

    -- Metadata
    c.bronze_extracted_at,
    c.bronze_source_file
FROM Bronze_companies c
LEFT JOIN Bronze_addresses a ON c.Comp_CompanyId = a.Addr_CompanyId
WHERE c.Comp_CompanyId IS NOT NULL
ORDER BY c.Comp_CompanyId
```

---

## 2. Person (Contact) Entity

### Base Properties (from Person table)
- `Pers_PersonId` (PK)
- `Pers_Salutation`
- `Pers_FirstName`
- `Pers_LastName`
- `Pers_MiddleName`
- `Pers_Suffix`
- `Pers_Gender`
- `Pers_Title`
- `Pers_TitleCode`
- `Pers_Type`
- `Pers_Status`
- `Pers_Source`
- `Pers_Territory`
- `Pers_EmailAddress`
- `Pers_PhoneCountryCode`
- `Pers_PhoneCityCode`
- `Pers_PhoneNumber`
- `Pers_FaxCountryCode`
- `Pers_FaxCityCode`
- `Pers_FaxNumber`
- `Pers_MobileCountryCode`
- `Pers_MobileCityCode`
- `Pers_MobileNumber`
- `Pers_Department`
- `Pers_CreatedDate`
- `Pers_UpdatedDate`
- `Pers_CreatedBy`
- `Pers_UpdatedBy`

### Denormalized Properties (from Company JOIN)
- `Comp_CompanyId` (FK)
- `Comp_Name`
- `Comp_Website`

### Data Quality Fields
- `email_valid` (computed)

### Metadata
- `bronze_extracted_at`
- `bronze_source_file`

### SQL Query Pattern
```sql
CREATE OR REPLACE VIEW Processed_Persons AS
SELECT DISTINCT
    -- Base Person Properties
    p.Pers_PersonId,
    p.Pers_Salutation,
    p.Pers_FirstName,
    p.Pers_LastName,
    p.Pers_MiddleName,
    p.Pers_Suffix,
    p.Pers_Gender,
    p.Pers_Title,
    p.Pers_TitleCode,
    p.Pers_Type,
    p.Pers_Status,
    p.Pers_EmailAddress,
    p.Pers_PhoneCountryCode,
    p.Pers_PhoneCityCode,
    p.Pers_PhoneNumber,
    p.Pers_MobileCountryCode,
    p.Pers_MobileCityCode,
    p.Pers_MobileNumber,
    p.Pers_Department,
    p.Pers_CreatedDate,
    p.Pers_UpdatedDate,

    -- Data Quality
    p.email_valid,

    -- Denormalized Company Properties
    p.Comp_CompanyId,
    c.Comp_Name,
    c.Comp_Website,

    -- Metadata
    p.bronze_extracted_at,
    p.bronze_source_file
FROM Bronze_persons p
LEFT JOIN Processed_Companies c ON p.Comp_CompanyId = c.Comp_CompanyId
WHERE p.Pers_PersonId IS NOT NULL
ORDER BY p.Pers_PersonId
```

---

## 3. Opportunity (Deal) Entity

### Base Properties (from Opportunity table)
- `Oppo_OpportunityId` (PK)
- `Oppo_Description`
- `Oppo_PrimaryCompanyId` (FK)
- `Oppo_PrimaryPersonId` (FK)
- `Oppo_AssignedUserId` (FK)
- `Oppo_Type`
- `Oppo_Product`
- `Oppo_Source`
- `Oppo_Note`
- `Oppo_CustomerRef`
- `Oppo_Status`
- `Oppo_Stage`
- `Oppo_Forecast`
- `Oppo_Certainty`
- `Oppo_Priority`
- `Oppo_TargetClose`
- `Oppo_ActualClose`
- `Oppo_WinProbability`
- `Oppo_CreatedDate`
- `Oppo_UpdatedDate`
- `Oppo_CreatedBy`
- `Oppo_UpdatedBy`
- `Oppo_Total`
- `oppo_cout` (cost)

### Computed Properties (IC'ALPS Business Logic)
- `Weighted_Forecast` = `Oppo_Forecast` × `Oppo_Certainty`
- `Net_Amount` = `Oppo_Forecast` - `oppo_cout`
- `Net_Weighted_Amount` = `Net_Amount` × `Oppo_Certainty`

### Pipeline Stage Properties (IC'ALPS)
- `Pipeline_Type` (Hardware/Software)
- `Pipeline_Stage_Number` (01-05)
- `Final_Outcome` (No-go, Abandonnée, Perdue, Gagnée)
- `Mapped_Stage` (Closed Won, Closed Lost, In Progress)

### Denormalized Properties (from Company JOIN)
- `Comp_Name`
- `Comp_Website`

### Denormalized Properties (from Person JOIN)
- `Pers_FirstName`
- `Pers_LastName`
- `Pers_EmailAddress`

### Metadata
- `bronze_extracted_at`
- `bronze_source_file`

### SQL Query Pattern
```sql
CREATE OR REPLACE VIEW Processed_Opportunities AS
SELECT
    -- Base Opportunity Properties
    o.Oppo_OpportunityId,
    o.Oppo_Description,
    o.Oppo_PrimaryCompanyId,
    o.Oppo_PrimaryPersonId,
    o.Oppo_AssignedUserId,
    o.Oppo_Type,
    o.Oppo_Product,
    o.Oppo_Source,
    o.Oppo_Note,
    o.Oppo_CustomerRef,
    o.Oppo_Status,
    o.Oppo_Stage,
    o.Oppo_Forecast,
    o.Oppo_Certainty,
    o.Oppo_Priority,
    o.Oppo_TargetClose,
    o.Oppo_ActualClose,
    o.Oppo_WinProbability,
    o.Oppo_CreatedDate,
    o.Oppo_UpdatedDate,
    o.Oppo_Total,
    o.oppo_cout,

    -- Computed Columns (IC'ALPS Business Logic)
    (o.Oppo_Forecast * o.Oppo_Certainty) AS Weighted_Forecast,
    (o.Oppo_Forecast - o.oppo_cout) AS Net_Amount,
    ((o.Oppo_Forecast - o.oppo_cout) * o.Oppo_Certainty) AS Net_Weighted_Amount,

    -- Denormalized Company Properties
    c.Comp_Name,
    c.Comp_Website,

    -- Denormalized Person Properties
    p.Pers_FirstName,
    p.Pers_LastName,
    p.Pers_EmailAddress,

    -- Metadata
    o.bronze_extracted_at,
    o.bronze_source_file
FROM Bronze_opportunities o
LEFT JOIN Processed_Companies c ON o.Oppo_PrimaryCompanyId = c.Comp_CompanyId
LEFT JOIN Processed_Persons p ON o.Oppo_PrimaryPersonId = p.Pers_PersonId
WHERE o.Oppo_OpportunityId IS NOT NULL
ORDER BY o.Oppo_OpportunityId
```

---

## 4. Communication Entity

### Base Properties (from Communication table)
- `Comm_CommunicationId` (PK)
- `Comm_Subject`
- `Comm_From`
- `Comm_TO`
- `Comm_DateTime`
- `Comm_OriginalDateTime`
- `Comm_OriginalToDateTime`
- `comm_type`
- `Comm_Action`
- `Comm_Status`
- `Comm_Note`
- `Comm_Private`
- `Comm_CreatedDate`
- `Comm_UpdatedDate`
- `Comm_CreatedBy`
- `Comm_UpdatedBy`

### Relationship Properties (many:many)
- `Oppo_OpportunityId` (FK)
- `Pers_PersonId` (FK)
- `Comp_CompanyId` (FK)

### Denormalized Properties (from Opportunity JOIN)
- `Oppo_Description`
- `Oppo_Status`

### Denormalized Properties (from Person JOIN)
- `Pers_FirstName`
- `Pers_LastName`
- `Pers_EmailAddress`

### Denormalized Properties (from Company JOIN)
- `Comp_Name`
- `Comp_Website`

### Metadata
- `bronze_extracted_at`
- `bronze_source_file`

### SQL Query Pattern
```sql
CREATE OR REPLACE VIEW Processed_Communications AS
SELECT
    -- Base Communication Properties
    cm.Comm_CommunicationId,
    cm.Comm_Subject,
    cm.Comm_From,
    cm.Comm_TO,
    cm.Comm_DateTime,
    cm.Comm_OriginalDateTime,
    cm.Comm_OriginalToDateTime,
    cm.comm_type,
    cm.Comm_Action,
    cm.Comm_Status,
    cm.Comm_Note,
    cm.Comm_Private,
    cm.Comm_CreatedDate,
    cm.Comm_UpdatedDate,

    -- Relationship IDs
    cm.Oppo_OpportunityId,
    cm.Pers_PersonId,
    cm.Comp_CompanyId,

    -- Denormalized Opportunity Properties
    o.Oppo_Description,
    o.Oppo_Status AS Oppo_Status,

    -- Denormalized Person Properties
    p.Pers_FirstName,
    p.Pers_LastName,
    p.Pers_EmailAddress,

    -- Denormalized Company Properties
    c.Comp_Name,
    c.Comp_Website,

    -- Metadata
    cm.bronze_extracted_at,
    cm.bronze_source_file
FROM Bronze_communications cm
LEFT JOIN Processed_Opportunities o ON cm.Oppo_OpportunityId = o.Oppo_OpportunityId
LEFT JOIN Processed_Persons p ON cm.Pers_PersonId = p.Pers_PersonId
LEFT JOIN Processed_Companies c ON cm.Comp_CompanyId = c.Comp_CompanyId
WHERE cm.Comm_CommunicationId IS NOT NULL
ORDER BY cm.Comm_DateTime DESC
```

---

## 5. Case (Support Ticket) Entity

### Base Properties (from Cases table)
- `Case_CaseId` (PK)
- `Case_PrimaryCompanyId` (FK)
- `Case_PrimaryPersonId` (FK)
- `Case_AssignedUserId` (FK)
- `Case_ChannelId`
- `Case_Description`
- `Case_CustomerRef`
- `Case_Source`
- `Case_SerialNumber`
- `Case_Product`
- `Case_ProblemType`
- `Case_SolutionType`
- `Case_ProblemNote`
- `Case_SolutionNote`
- `Case_Opened`
- `Case_OpenedBy`
- `Case_Closed`
- `Case_ClosedBy`
- `Case_Status`
- `Case_Stage`
- `Case_Priority`
- `Case_TargetClose`
- `Case_CreatedDate`
- `Case_UpdatedDate`
- `Case_CreatedBy`
- `Case_UpdatedBy`

### Denormalized Properties (from Company JOIN)
- `Company_Name`
- `Company_WebSite`

### Denormalized Properties (from Person JOIN)
- `Person_FirstName`
- `Person_LastName`
- `Person_EmailAddress`

### Metadata
- `bronze_extracted_at`
- `bronze_source_file`

### SQL Query Pattern
```sql
CREATE OR REPLACE VIEW Processed_Cases AS
SELECT
    -- Base Case Properties
    c.Case_CaseId,
    c.Case_PrimaryCompanyId,
    c.Case_PrimaryPersonId,
    c.Case_AssignedUserId,
    c.Case_ChannelId,
    c.Case_Description,
    c.Case_CustomerRef,
    c.Case_Source,
    c.Case_SerialNumber,
    c.Case_Product,
    c.Case_ProblemType,
    c.Case_SolutionType,
    c.Case_ProblemNote,
    c.Case_SolutionNote,
    c.Case_Opened,
    c.Case_OpenedBy,
    c.Case_Closed,
    c.Case_ClosedBy,
    c.Case_Status,
    c.Case_Stage,
    c.Case_Priority,
    c.Case_TargetClose,
    c.Case_CreatedDate,
    c.Case_UpdatedDate,

    -- Denormalized Company Properties
    comp.Comp_Name AS Company_Name,
    comp.Comp_Website AS Company_WebSite,

    -- Denormalized Person Properties
    p.Pers_FirstName AS Person_FirstName,
    p.Pers_LastName AS Person_LastName,
    v.Emai_EmailAddress AS Person_EmailAddress,

    -- Metadata
    c.bronze_extracted_at,
    c.bronze_source_file
FROM Bronze_cases c
LEFT JOIN Processed_Companies comp ON c.Case_PrimaryCompanyId = comp.Comp_CompanyId
LEFT JOIN Processed_Persons p ON c.Case_PrimaryPersonId = p.Pers_PersonId
LEFT JOIN Bronze_emails v ON c.Case_PrimaryPersonId = v.Pers_PersonId
WHERE c.Case_CaseId IS NOT NULL
ORDER BY c.Case_Opened DESC
```

---

## 6. Social Network Entity

### Base Properties (from Social Network table)
- `sone_networklink` (URL)
- `Related_TableID` (entity type)
- `Related_RecordID` (entity ID)
- `bord_caption` (platform name)
- `network_type`
- `sone_network` (platform)
- `CreatedDate`
- `UpdatedDate`

### Computed Properties
- `entity_name` (resolved based on Related_TableID)
- `entity_type` (Company/Person/Other)

### Denormalized Properties (conditional on Related_TableID)
- When Related_TableID = 5 (Company):
  - `Comp_Name`
- When Related_TableID = 13 (Person):
  - `Pers_FirstName`
  - `Pers_LastName`

### Metadata
- `bronze_extracted_at`
- `bronze_source_file`

### SQL Query Pattern
```sql
CREATE OR REPLACE VIEW Processed_Social_Networks AS
SELECT
    -- Base Social Network Properties
    sn.sone_networklink,
    sn.Related_TableID,
    sn.Related_RecordID,
    sn.bord_caption,
    sn.network_type,
    sn.sone_network,

    -- Computed Entity Information
    CASE
        WHEN sn.Related_TableID = 5 THEN 'Company'
        WHEN sn.Related_TableID = 13 THEN 'Person'
        ELSE 'Unknown'
    END as entity_type,

    CASE
        WHEN sn.Related_TableID = 5 THEN c.Comp_Name
        WHEN sn.Related_TableID = 13 THEN CONCAT(p.Pers_FirstName, ' ', p.Pers_LastName)
        ELSE 'Unknown'
    END as entity_name,

    -- Denormalized Company Properties (when applicable)
    c.Comp_Name,
    c.Comp_Website,

    -- Denormalized Person Properties (when applicable)
    p.Pers_FirstName,
    p.Pers_LastName,

    -- Metadata
    sn.bronze_extracted_at,
    sn.bronze_source_file
FROM Bronze_social_networks sn
LEFT JOIN Processed_Companies c ON sn.Related_TableID = 5 AND sn.Related_RecordID = c.Comp_CompanyId
LEFT JOIN Processed_Persons p ON sn.Related_TableID = 13 AND sn.Related_RecordID = p.Pers_PersonId
WHERE sn.sone_networklink IS NOT NULL AND sn.sone_networklink != ''
ORDER BY sn.Related_TableID, sn.Related_RecordID
```

---

## 7. Address Entity (NEW)

### Base Properties (from Address table)
- `Addr_AddressId` (PK)
- `Addr_CompanyId` (FK)
- `Addr_PersonId` (FK)
- `Addr_Type` (Billing, Shipping, etc.)
- `Addr_Street1`
- `Addr_Street2`
- `Addr_Street3`
- `Addr_City`
- `Addr_State`
- `Addr_PostCode`
- `Addr_Country`
- `Addr_CountryCode`
- `Addr_CreatedDate`
- `Addr_UpdatedDate`

### Denormalized Properties (from Company JOIN)
- `Comp_Name`
- `Comp_Website`

### Denormalized Properties (from Person JOIN)
- `Pers_FirstName`
- `Pers_LastName`

### Metadata
- `bronze_extracted_at`
- `bronze_source_file`

### SQL Query Pattern
```sql
CREATE OR REPLACE VIEW Processed_Addresses AS
SELECT
    -- Base Address Properties
    a.Addr_AddressId,
    a.Addr_CompanyId,
    a.Addr_PersonId,
    a.Addr_Type,
    a.Addr_Street1,
    a.Addr_Street2,
    a.Addr_Street3,
    a.Addr_City,
    a.Addr_State,
    a.Addr_PostCode,
    a.Addr_Country,
    a.Addr_CountryCode,
    a.Addr_CreatedDate,
    a.Addr_UpdatedDate,

    -- Denormalized Company Properties
    c.Comp_Name AS Company_Name,
    c.Comp_Website AS Company_WebSite,

    -- Denormalized Person Properties
    p.Pers_FirstName AS Person_FirstName,
    p.Pers_LastName AS Person_LastName,

    -- Metadata
    a.bronze_extracted_at,
    a.bronze_source_file
FROM Bronze_addresses a
LEFT JOIN Processed_Companies c ON a.Addr_CompanyId = c.Comp_CompanyId
LEFT JOIN Processed_Persons p ON a.Addr_PersonId = p.Pers_PersonId
WHERE a.Addr_AddressId IS NOT NULL
ORDER BY a.Addr_AddressId
```

---

## Cardinality Rules Summary

| Entity | Related To | Cardinality | Via |
|--------|-----------|-------------|-----|
| Company | Address | 1:many | Addr_CompanyId |
| Company | Person | 1:many | Pers_CompanyId |
| Company | Opportunity | 1:many | Oppo_PrimaryCompanyId |
| Company | Case | 1:many | Case_PrimaryCompanyId |
| Company | Communication | many:many | Comm_CompanyId |
| Person | Company | many:1 | Pers_CompanyId |
| Person | Opportunity | 1:many | Oppo_PrimaryPersonId |
| Person | Case | 1:many | Case_PrimaryPersonId |
| Person | Communication | many:many | Comm_PersonId |
| Opportunity | Company | many:1 | Oppo_PrimaryCompanyId |
| Opportunity | Person | many:1 | Oppo_PrimaryPersonId |
| Opportunity | Communication | 1:many | Comm_OpportunityId |
| Case | Company | many:1 | Case_PrimaryCompanyId |
| Case | Person | many:1 | Case_PrimaryPersonId |
| Communication | Company | many:1 | Comm_CompanyId |
| Communication | Person | many:1 | Comm_PersonId |
| Communication | Opportunity | many:1 | Comm_OpportunityId |
| Address | Company | many:1 | Addr_CompanyId |
| Address | Person | many:1 | Addr_PersonId |
| Social Network | Company | many:1 | Related_TableID=5 |
| Social Network | Person | many:1 | Related_TableID=13 |

---

## Usage in Skills

Each entity extractor skill should use these property definitions to:

1. **Generate SQL extraction queries** with appropriate JOINs
2. **Create dataclasses** with all properties (base + denormalized)
3. **Build DuckDB views** for Silver/Gold layers
4. **Apply business logic** (computed columns for Opportunities)
5. **Document cardinality** for relationship integrity

---

## Update Properties in config.py

Add these to your `config.py` file:

```python
# Complete Entity Properties
COMPANY_PROPERTIES = [
    "Comp_CompanyId", "Comp_Name", "Comp_Website", "Comp_Type",
    "Comp_Status", "Comp_EmailAddress", "Comp_PhoneNumber",
    "Comp_CreatedDate", "Comp_UpdatedDate",
    # Address fields
    "Addr_Street1", "Addr_City", "Addr_State", "Addr_PostCode", "Addr_Country"
]

PERSON_PROPERTIES = [
    "Pers_PersonId", "Pers_Salutation", "Pers_FirstName", "Pers_LastName",
    "Pers_MiddleName", "Pers_Suffix", "Pers_Gender", "Pers_Title",
    "Pers_EmailAddress", "Pers_PhoneNumber", "Pers_MobileNumber",
    "Pers_Department", "Pers_CreatedDate", "Pers_UpdatedDate",
    # Company fields
    "Comp_CompanyId", "Comp_Name"
]

OPPORTUNITY_PROPERTIES = [
    "Oppo_OpportunityId", "Oppo_Description", "Oppo_PrimaryCompanyId",
    "Oppo_PrimaryPersonId", "Oppo_Type", "Oppo_Product", "Oppo_Status",
    "Oppo_Stage", "Oppo_Forecast", "Oppo_Certainty", "Oppo_Priority",
    "Oppo_TargetClose", "Oppo_CreatedDate", "Oppo_Total", "oppo_cout",
    # Computed
    "Weighted_Forecast", "Net_Amount", "Net_Weighted_Amount",
    # Denormalized
    "Comp_Name", "Pers_FirstName", "Pers_LastName"
]

CASE_PROPERTIES = [
    "Case_CaseId", "Case_PrimaryCompanyId", "Case_PrimaryPersonId",
    "Case_Description", "Case_Status", "Case_Stage", "Case_Priority",
    "Case_Opened", "Case_Closed", "Case_Product", "Case_ProblemType",
    "Case_CreatedDate", "Case_UpdatedDate",
    # Denormalized
    "Company_Name", "Person_FirstName", "Person_LastName"
]
```
