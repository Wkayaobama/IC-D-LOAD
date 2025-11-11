"""
Configuration Template for IC_Load

Copy this file to config.py and update with your actual values.
config.py is in .gitignore and will not be committed.
"""

# SQL Server Configuration
SQL_SERVER = "your_server_name"  # e.g., "localhost" or "SERVER\\INSTANCE"
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
    parts = [
        "DRIVER={SQL Server}",
        f"SERVER={SQL_SERVER}",
        f"DATABASE={SQL_DATABASE}"
    ]

    if SQL_TRUSTED_CONNECTION:
        parts.append("Trusted_Connection=yes")
    else:
        parts.append(f"UID={SQL_USERNAME}")
        parts.append(f"PWD={SQL_PASSWORD}")

    return ";".join(parts)
