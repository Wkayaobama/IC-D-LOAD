#!/usr/bin/env python3
"""
Case Extractor

Extracts Case/Support Ticket data from SQL Server with denormalized Company/Person info.
"""

from dataclasses import dataclass
from typing import List, Optional
from datetime import datetime
import pandas as pd
import sys
sys.path.append('../sql-connection-manager/scripts')
sys.path.append('../dataframe-dataclass-converter/scripts')


@dataclass
class Case:
    """Case entity with denormalized Company and Person fields"""
    case_id: int
    primary_company_id: Optional[int] = None
    primary_person_id: Optional[int] = None
    assigned_user_id: Optional[int] = None
    status: Optional[str] = None
    stage: Optional[str] = None
    priority: Optional[str] = None
    description: Optional[str] = None
    opened: Optional[datetime] = None
    closed: Optional[datetime] = None

    # Denormalized Company fields
    company_name: Optional[str] = None
    company_website: Optional[str] = None

    # Denormalized Person fields
    person_first_name: Optional[str] = None
    person_last_name: Optional[str] = None
    person_email: Optional[str] = None


class CaseExtractor:
    """Extract Case data from SQL Server"""

    QUERY = """
    SELECT
        c.[Case_CaseId],
        c.[Case_PrimaryCompanyId],
        comp.[Comp_Name] AS Company_Name,
        comp.[Comp_WebSite] AS Company_WebSite,
        c.[Case_PrimaryPersonId],
        p.[Pers_FirstName] AS Person_FirstName,
        p.[Pers_LastName] AS Person_LastName,
        v.[Emai_EmailAddress] AS Person_EmailAddress,
        c.[Case_AssignedUserId],
        c.[Case_Description],
        c.[Case_Status],
        c.[Case_Stage],
        c.[Case_Priority],
        c.[Case_Opened],
        c.[Case_Closed]
    FROM [CRMICALPS].[dbo].[vCases] c
    LEFT JOIN [CRMICALPS].[dbo].[Company] comp
        ON c.[Case_PrimaryCompanyId] = comp.[Comp_CompanyId]
    LEFT JOIN [CRMICALPS].[dbo].[Person] p
        ON c.[Case_PrimaryPersonId] = p.[Pers_PersonId]
    LEFT JOIN [CRMICALPS].[dbo].[vEmailCompanyAndPerson] v
        ON c.[Case_PrimaryPersonId] = v.[Pers_PersonId]
    """

    def __init__(self, connection_string: str):
        from connection_manager import ConnectionManager
        self.conn_manager = ConnectionManager.from_connection_string(connection_string)

    def extract(self, filter_clause: str = "") -> List[Case]:
        """Extract cases from database"""
        query = self.QUERY
        if filter_clause:
            query += f" {filter_clause}"

        with self.conn_manager.get_connection() as conn:
            df = pd.read_sql(query, conn)

        # Convert DataFrame to dataclasses
        from dataframe_converter import DataFrameConverter
        converter = DataFrameConverter()
        cases = converter.dataframe_to_dataclasses(df, Case)

        return cases

    def save_to_bronze(self, cases: List[Case], output_path: str = "Bronze_Cases.csv"):
        """Save cases to Bronze layer CSV"""
        from dataframe_converter import DataFrameConverter
        converter = DataFrameConverter()
        df = converter.dataclasses_to_dataframe(cases)
        df.to_csv(output_path, index=False)
        print(f"Saved {len(cases)} cases to {output_path}")


if __name__ == "__main__":
    connection_string = "your_connection_string"
    extractor = CaseExtractor(connection_string)

    print("Extracting cases...")
    cases = extractor.extract()
    print(f"Extracted {len(cases)} cases")

    extractor.save_to_bronze(cases)
