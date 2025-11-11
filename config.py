"""
Configuration for IC_Load

This file contains your actual database connection settings.
It is in .gitignore and will NOT be committed to version control.
"""

# SQL Server Configuration
SQL_SERVER = r"(localdb)\MSSQLLocalDB"  # LocalDB instance
SQL_DATABASE = "CRMICALPS"
SQL_TRUSTED_CONNECTION = True  # Use Windows Authentication

# If not using Trusted Connection, uncomment and fill:
# SQL_USERNAME = "your_username"
# SQL_PASSWORD = "your_password"

# Output Configuration
BRONZE_LAYER_PATH = "bronze_layer"
SILVER_LAYER_PATH = "silver_layer"
GOLD_LAYER_PATH = "gold_layer"
MODELS_PATH = "models"

# Query Configuration
QUERY_TIMEOUT = 300  # seconds
MAX_RECORDS_PER_EXTRACTION = None  # None = no limit, or set integer like 10000

# Entity Properties of Interest
CASE_PROPERTIES = [
    "Case_CaseId",
    "Case_PrimaryCompanyId",
    "Case_PrimaryPersonId",
    "Case_AssignedUserId",
    "Case_ChannelId",
    "Case_Description",
    "Case_Status",
    "Case_Stage",
    "Case_Priority",
    "Case_Opened",
    "Case_Closed",
    "Case_CreatedDate",
    "Case_UpdatedDate"
]

CONTACT_PROPERTIES = [
    "Pers_PersonId",
    "Pers_Salutation",
    "Pers_FirstName",
    "Pers_LastName",
    "Pers_MiddleName",
    "Pers_Suffix",
    "Pers_Gender",
    "Pers_Title"
]

COMMUNICATION_PROPERTIES = [
    "Comm_CommunicationId",
    "Comm_OriginalDateTime",
    "Comm_OriginalToDateTime",
    "Comm_Type",
    "Comm_Action",
    "Comm_Status"
]

# Helper function to build connection string
def get_connection_string():
    """Build SQL Server connection string from config"""
    # Try modern drivers first, fallback to older ones
    # The script will automatically try all available drivers
    import pyodbc

    # Get first available SQL Server driver
    available_drivers = [d for d in pyodbc.drivers() if 'SQL Server' in d]

    if not available_drivers:
        raise RuntimeError("No SQL Server ODBC drivers found! Please install ODBC Driver for SQL Server.")

    # Prefer newer drivers
    preferred_order = [
        "ODBC Driver 17 for SQL Server",
        "ODBC Driver 13 for SQL Server",
        "ODBC Driver 11 for SQL Server",
        "SQL Server Native Client 11.0",
        "SQL Server"
    ]

    driver = None
    for pref in preferred_order:
        if pref in available_drivers:
            driver = pref
            break

    if not driver:
        driver = available_drivers[0]  # Use first available

    parts = [
        f"DRIVER={{{driver}}}",
        f"SERVER={SQL_SERVER}",
        f"DATABASE={SQL_DATABASE}"
    ]

    if SQL_TRUSTED_CONNECTION:
        parts.append("Trusted_Connection=yes")
    else:
        parts.append(f"UID={SQL_USERNAME}")
        parts.append(f"PWD={SQL_PASSWORD}")

    # For LocalDB, add connection timeout
    if "localdb" in SQL_SERVER.lower():
        parts.append("Connection Timeout=30")

    return ";".join(parts)
