"""
Entity Properties Definitions
===============================

This module defines property lists for each entity in the IC_Load system.
Based on ENTITY_PROPERTIES.md specifications.

Each entity has:
- base: Properties from the primary table
- denormalized: Properties from JOINed tables
- computed: Calculated fields
- metadata: Extraction metadata fields
"""

# ============================================================================
# COMPANY PROPERTIES
# ============================================================================

COMPANY_PROPERTIES = {
    'base': [
        'Comp_CompanyId',
        'Comp_PrimaryPersonId',
        'Comp_PrimaryAddressId',
        'Comp_Name',
        'Comp_Type',
        'Comp_Status',
        'Comp_Source',
        'Comp_Territory',
        'Comp_Revenue',
        'Comp_Employees',
        'Comp_Sector',
        'Comp_WebSite',
        'Comp_EmailAddress',
        'Comp_PhoneCountryCode',
        'Comp_PhoneAreaCode',
        'Comp_PhoneNumber',
        'Comp_FaxCountryCode',
        'Comp_FaxAreaCode',
        'Comp_FaxNumber',
        'Comp_CreatedDate',
        'Comp_UpdatedDate',
        'Comp_CreatedBy',
        'Comp_UpdatedBy'
    ],
    'denormalized': [
        'a.[Addr_AddressId] AS Address_Id',
        'a.[Addr_Address1] AS Address_Street1',
        'a.[Addr_Address2] AS Address_Street2',
        'a.[Addr_Address3] AS Address_Street3',
        'a.[Addr_City] AS Address_City',
        'a.[Addr_State] AS Address_State',
        'a.[Addr_Country] AS Address_Country',
        'a.[Addr_PostCode] AS Address_PostCode'
    ],
    'computed': [],
    'metadata': [
        'bronze_extracted_at',
        'bronze_source_file'
    ]
}


# ============================================================================
# PERSON (CONTACT) PROPERTIES
# ============================================================================

PERSON_PROPERTIES = {
    'base': [
        'Pers_PersonId',
        'Pers_CompanyId',
        'Pers_PrimaryAddressId',
        'Pers_Salutation',
        'Pers_FirstName',
        'Pers_LastName',
        'Pers_MiddleName',
        'Pers_Suffix',
        'Pers_Gender',
        'Pers_Title',
        'Pers_TitleCode',
        'Pers_Type',
        'Pers_Department',
        'Pers_Status',
        'Pers_Source',
        'Pers_Territory',
        'Pers_WebSite',
        'Pers_EmailAddress',
        'Pers_PhoneCountryCode',
        'Pers_PhoneAreaCode',
        'Pers_PhoneNumber',
        'Pers_MobileCountryCode',
        'Pers_MobileAreaCode',
        'Pers_MobileNumber',
        'Pers_FaxCountryCode',
        'Pers_FaxAreaCode',
        'Pers_FaxNumber',
        'Pers_CreatedDate',
        'Pers_UpdatedDate',
        'Pers_CreatedBy',
        'Pers_UpdatedBy'
    ],
    'denormalized': [
        'c.[Comp_Name] AS Company_Name',
        'c.[Comp_WebSite] AS Company_WebSite',
        'a.[Addr_AddressId] AS Address_Id',
        'a.[Addr_Address1] AS Address_Street1',
        'a.[Addr_City] AS Address_City',
        'a.[Addr_State] AS Address_State',
        'a.[Addr_Country] AS Address_Country',
        'a.[Addr_PostCode] AS Address_PostCode'
    ],
    'computed': [],
    'metadata': [
        'bronze_extracted_at',
        'bronze_source_file'
    ]
}


# ============================================================================
# OPPORTUNITY (DEAL) PROPERTIES
# ============================================================================

OPPORTUNITY_PROPERTIES = {
    'base': [
        'Oppo_OpportunityId',
        'Oppo_Description',
        'Oppo_PrimaryCompanyId',
        'Oppo_PrimaryPersonId',
        'Oppo_AssignedUserId',
        'Oppo_Type',
        'Oppo_Product',
        'Oppo_Source',
        'Oppo_Note',
        'Oppo_CustomerRef',
        'Oppo_Status',
        'Oppo_Stage',
        'Oppo_Forecast',
        'Oppo_Certainty',
        'Oppo_Priority',
        'Oppo_TargetClose',
        'Oppo_ActualClose',
        'Oppo_WinProbability',
        'Oppo_Total',
        'oppo_cout',
        'Oppo_CreatedDate',
        'Oppo_UpdatedDate',
        'Oppo_CreatedBy',
        'Oppo_UpdatedBy'
    ],
    'denormalized': [
        'c.[Comp_Name] AS Company_Name',
        'c.[Comp_WebSite] AS Company_WebSite',
        'p.[Pers_FirstName] AS Person_FirstName',
        'p.[Pers_LastName] AS Person_LastName',
        'p.[Pers_EmailAddress] AS Person_EmailAddress'
    ],
    'computed': [
        '(base.[Oppo_Forecast] * base.[Oppo_Certainty]) AS Weighted_Forecast',
        '(base.[Oppo_Forecast] - base.[oppo_cout]) AS Net_Amount',
        '((base.[Oppo_Forecast] - base.[oppo_cout]) * base.[Oppo_Certainty]) AS Net_Weighted_Amount'
    ],
    'metadata': [
        'bronze_extracted_at',
        'bronze_source_file'
    ]
}


# ============================================================================
# CASE (SUPPORT TICKET) PROPERTIES
# ============================================================================

CASE_PROPERTIES = {
    'base': [
        'Case_CaseId',
        'Case_PrimaryCompanyId',
        'Case_PrimaryPersonId',
        'Case_AssignedUserId',
        'Case_ChannelId',
        'Case_Description',
        'Case_CustomerRef',
        'Case_Source',
        'Case_SerialNumber',
        'Case_Product',
        'Case_ProblemType',
        'Case_SolutionType',
        'Case_ProblemNote',
        'Case_SolutionNote',
        'Case_Opened',
        'Case_OpenedBy',
        'Case_Closed',
        'Case_ClosedBy',
        'Case_Status',
        'Case_Stage',
        'Case_Priority',
        'Case_TargetClose',
        'Case_CreatedDate',
        'Case_UpdatedDate',
        'Case_CreatedBy',
        'Case_UpdatedBy'
    ],
    'denormalized': [
        'comp.[Comp_Name] AS Company_Name',
        'comp.[Comp_WebSite] AS Company_WebSite',
        'p.[Pers_FirstName] AS Person_FirstName',
        'p.[Pers_LastName] AS Person_LastName',
        'v.[Emai_EmailAddress] AS Person_EmailAddress'
    ],
    'computed': [],
    'metadata': [
        'bronze_extracted_at',
        'bronze_source_file'
    ]
}


# ============================================================================
# COMMUNICATION PROPERTIES
# ============================================================================

COMMUNICATION_PROPERTIES = {
    'base': [
        'Comm_CommunicationId',
        'Comm_Subject',
        'Comm_From',
        'Comm_TO',
        'Comm_DateTime',
        'Comm_OriginalDateTime',
        'Comm_OriginalToDateTime',
        'comm_type',
        'Comm_Action',
        'Comm_Status',
        'Comm_Note',
        'Comm_Private',
        'Comm_CreatedDate',
        'Comm_UpdatedDate',
        'Comm_CreatedBy',
        'Comm_UpdatedBy'
    ],
    'denormalized': [
        'o.[Oppo_OpportunityId] AS Opportunity_Id',
        'o.[Oppo_Description] AS Opportunity_Description',
        'o.[Oppo_Status] AS Opportunity_Status',
        'p.[Pers_PersonId] AS Person_Id',
        'p.[Pers_FirstName] AS Person_FirstName',
        'p.[Pers_LastName] AS Person_LastName',
        'p.[Pers_EmailAddress] AS Person_EmailAddress',
        'c.[Comp_CompanyId] AS Company_Id',
        'c.[Comp_Name] AS Company_Name',
        'c.[Comp_WebSite] AS Company_WebSite'
    ],
    'computed': [],
    'metadata': [
        'bronze_extracted_at',
        'bronze_source_file'
    ]
}


# ============================================================================
# ADDRESS PROPERTIES
# ============================================================================

ADDRESS_PROPERTIES = {
    'base': [
        'Addr_AddressId',
        'Addr_CompanyId',
        'Addr_PersonId',
        'Addr_Type',
        'Addr_Address1',
        'Addr_Address2',
        'Addr_Address3',
        'Addr_City',
        'Addr_State',
        'Addr_Country',
        'Addr_PostCode',
        'Addr_CountryCode',
        'Addr_CreatedDate',
        'Addr_UpdatedDate'
    ],
    'denormalized': [
        'c.[Comp_Name] AS Company_Name',
        'c.[Comp_WebSite] AS Company_WebSite',
        'p.[Pers_FirstName] AS Person_FirstName',
        'p.[Pers_LastName] AS Person_LastName'
    ],
    'computed': [],
    'metadata': [
        'bronze_extracted_at',
        'bronze_source_file'
    ]
}


# ============================================================================
# SOCIAL NETWORK PROPERTIES
# ============================================================================

SOCIAL_NETWORK_PROPERTIES = {
    'base': [
        'sone_networklink',
        'Related_TableID',
        'Related_RecordID',
        'bord_caption',
        'network_type',
        'sone_network',
        'CreatedDate',
        'UpdatedDate'
    ],
    'denormalized': [
        'c.[Comp_Name] AS Company_Name',
        'c.[Comp_WebSite] AS Company_WebSite',
        'p.[Pers_FirstName] AS Person_FirstName',
        'p.[Pers_LastName] AS Person_LastName'
    ],
    'computed': [
        "CASE WHEN base.[Related_TableID] = 5 THEN 'Company' WHEN base.[Related_TableID] = 13 THEN 'Person' ELSE 'Unknown' END AS entity_type",
        "CASE WHEN base.[Related_TableID] = 5 THEN c.[Comp_Name] WHEN base.[Related_TableID] = 13 THEN CONCAT(p.[Pers_FirstName], ' ', p.[Pers_LastName]) ELSE 'Unknown' END AS entity_name"
    ],
    'metadata': [
        'bronze_extracted_at',
        'bronze_source_file'
    ]
}



