"""
Extract Companies, Persons, and Opportunities WITH Phone Numbers
=================================================================

This script uses the actual working database schema and adds phone/email
data via LEFT JOINs to the phone/email views.
"""

import sys
import pyodbc
import pandas as pd
from pathlib import Path
from loguru import logger
from config import get_connection_string


def extract_companies_with_phones():
    """Extract companies with phone and email data"""
    logger.info("=" * 80)
    logger.info("Extracting Companies WITH Phone/Email")
    logger.info("=" * 80)
    
    query = """
    SELECT DISTINCT
        c.Comp_CompanyId,
        c.Comp_PrimaryPersonId,
        c.Comp_PrimaryAddressId,
        c.Comp_Name,
        c.Comp_Type,
        c.Comp_Status,
        c.Comp_Source,
        c.Comp_Territory,
        c.Comp_Revenue,
        c.Comp_Employees,
        c.Comp_Sector,
        c.Comp_WebSite,
        c.Comp_CreatedDate,
        c.Comp_UpdatedDate,
        
        -- Address data
        a.Addr_AddressId AS Address_Id,
        a.Addr_Address1 AS Address_Street1,
        a.Addr_City AS Address_City,
        a.Addr_State AS Address_State,
        a.Addr_Country AS Address_Country,
        a.Addr_PostCode AS Address_PostCode,
        
        -- Phone data (Business phone)
        cp.Phon_FullNumber AS Company_Phone,
        cp.Phon_PhoneId AS Phone_Id,
        cp.PLink_Type AS Phone_Type,
        
        -- Email data (Business email)
        ce.Emai_EmailAddress AS Company_Email,
        ce.Emai_EmailId AS Email_Id,
        ce.ELink_Type AS Email_Type
        
    FROM [CRMICALPS].[dbo].[Company] c
    LEFT JOIN [CRMICALPS].[dbo].[Address] a 
        ON c.Comp_PrimaryAddressId = a.Addr_AddressId
    LEFT JOIN [CRMICALPS].[dbo].[vCompanyPhone] cp 
        ON c.Comp_CompanyId = cp.PLink_RecordId 
        AND cp.PLink_Type = 'Business'
    LEFT JOIN [CRMICALPS].[dbo].[vCompanyEmail] ce 
        ON c.Comp_CompanyId = ce.ELink_RecordID 
        AND ce.ELink_Type = 'Business'
    WHERE c.Comp_CompanyId IS NOT NULL
    ORDER BY c.Comp_CompanyId
    """
    
    conn_str = get_connection_string()
    conn = pyodbc.connect(conn_str)
    
    logger.info("Extracting company data...")
    df = pd.read_sql(query, conn)
    conn.close()
    
    logger.info(f"✅ Extracted {len(df):,} company rows")
    logger.info(f"📞 Companies with phone: {df['Company_Phone'].notna().sum():,}")
    logger.info(f"📧 Companies with email: {df['Company_Email'].notna().sum():,}")
    
    # Save to Bronze
    from datetime import datetime
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = Path("bronze_layer") / f"Bronze_Company_{timestamp}.csv"
    output_path.parent.mkdir(exist_ok=True)
    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    logger.info(f"💾 Saved to {output_path}")
    
    return df


def extract_persons_with_phones():
    """Extract persons with phone and email data"""
    logger.info("\n" + "=" * 80)
    logger.info("Extracting Persons WITH Phone/Email")
    logger.info("=" * 80)
    
    query = """
    SELECT DISTINCT
        p.Pers_PersonId,
        p.Pers_CompanyId,
        p.Pers_PrimaryAddressId,
        p.Pers_Salutation,
        p.Pers_FirstName,
        p.Pers_LastName,
        p.Pers_MiddleName,
        p.Pers_Gender,
        p.Pers_Title,
        p.Pers_Department,
        p.Pers_Status,
        p.Pers_Source,
        p.Pers_Territory,
        p.Pers_WebSite,
        p.Pers_CreatedDate,
        p.Pers_UpdatedDate,
        
        -- Company data
        c.Comp_Name AS Company_Name,
        c.Comp_WebSite AS Company_WebSite,
        
        -- Address data
        a.Addr_AddressId AS Address_Id,
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
    LEFT JOIN [CRMICALPS].[dbo].[vPersonPhone] pp 
        ON p.Pers_PersonId = pp.PLink_RecordId 
        AND pp.PLink_Type = 'Business'
    LEFT JOIN [CRMICALPS].[dbo].[vPersonEmail] pe 
        ON p.Pers_PersonId = pe.ELink_RecordID 
        AND pe.ELink_Type = 'Business'
    WHERE p.Pers_PersonId IS NOT NULL
    ORDER BY p.Pers_PersonId
    """
    
    conn_str = get_connection_string()
    conn = pyodbc.connect(conn_str)
    
    logger.info("Extracting person data...")
    df = pd.read_sql(query, conn)
    conn.close()
    
    logger.info(f"✅ Extracted {len(df):,} person rows")
    logger.info(f"📞 Persons with phone: {df['Person_Phone'].notna().sum():,}")
    logger.info(f"📧 Persons with email: {df['Person_Email'].notna().sum():,}")
    
    # Save to Bronze
    from datetime import datetime
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = Path("bronze_layer") / f"Bronze_Person_{timestamp}.csv"
    output_path.parent.mkdir(exist_ok=True)
    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    logger.info(f"💾 Saved to {output_path}")
    
    return df


def extract_opportunities():
    """Extract opportunities with company and person data"""
    logger.info("\n" + "=" * 80)
    logger.info("Extracting Opportunities")
    logger.info("=" * 80)
    
    query = """
    SELECT
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
        o.Oppo_Opened,
        o.Oppo_Closed,
        o.Oppo_Status,
        o.Oppo_Stage,
        o.Oppo_Forecast,
        o.Oppo_Certainty,
        o.Oppo_Priority,
        o.Oppo_TargetClose,
        o.Oppo_Total,
        o.oppo_cout,
        o.Oppo_CreatedDate,
        o.Oppo_UpdatedDate,
        
        -- Computed columns
        (o.Oppo_Forecast * o.Oppo_Certainty / 100.0) AS Weighted_Forecast,
        (o.Oppo_Forecast - ISNULL(o.oppo_cout, 0)) AS Net_Amount,
        ((o.Oppo_Forecast - ISNULL(o.oppo_cout, 0)) * o.Oppo_Certainty / 100.0) AS Net_Weighted_Amount,
        
        -- Company data
        c.Comp_Name AS Company_Name,
        c.Comp_WebSite AS Company_WebSite,
        
        -- Person data
        p.Pers_FirstName AS Person_FirstName,
        p.Pers_LastName AS Person_LastName
        
    FROM [CRMICALPS].[dbo].[Opportunity] o
    LEFT JOIN [CRMICALPS].[dbo].[Company] c 
        ON o.Oppo_PrimaryCompanyId = c.Comp_CompanyId
    LEFT JOIN [CRMICALPS].[dbo].[Person] p 
        ON o.Oppo_PrimaryPersonId = p.Pers_PersonId
    WHERE o.Oppo_OpportunityId IS NOT NULL
    ORDER BY o.Oppo_OpportunityId
    """
    
    conn_str = get_connection_string()
    conn = pyodbc.connect(conn_str)
    
    logger.info("Extracting opportunity data...")
    df = pd.read_sql(query, conn)
    conn.close()
    
    logger.info(f"✅ Extracted {len(df):,} opportunity rows")
    logger.info(f"💰 Total forecast value: ${df['Oppo_Forecast'].sum():,.2f}")
    logger.info(f"💰 Weighted forecast: ${df['Weighted_Forecast'].sum():,.2f}")
    
    # Save to Bronze
    from datetime import datetime
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = Path("bronze_layer") / f"Bronze_Opportunity_{timestamp}.csv"
    output_path.parent.mkdir(exist_ok=True)
    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    logger.info(f"💾 Saved to {output_path}")
    
    return df


def main():
    """Main extraction workflow"""
    logger.info("\n" + "=" * 70)
    logger.info("Company, Person & Opportunity Extraction")
    logger.info("WITH Phone Numbers and Email Addresses")
    logger.info("=" * 70 + "\n")
    
    try:
        # Extract all three entities
        company_df = extract_companies_with_phones()
        person_df = extract_persons_with_phones()
        opportunity_df = extract_opportunities()
        
        # Summary
        logger.info("\n" + "=" * 80)
        logger.info("EXTRACTION SUMMARY")
        logger.info("=" * 80)
        logger.info(f"\n✅ Company: {len(company_df):,} rows")
        logger.info(f"   - With phone: {company_df['Company_Phone'].notna().sum():,}")
        logger.info(f"   - With email: {company_df['Company_Email'].notna().sum():,}")
        
        logger.info(f"\n✅ Person: {len(person_df):,} rows")
        logger.info(f"   - With phone: {person_df['Person_Phone'].notna().sum():,}")
        logger.info(f"   - With email: {person_df['Person_Email'].notna().sum():,}")
        
        logger.info(f"\n✅ Opportunity: {len(opportunity_df):,} rows")
        logger.info(f"   - Total forecast: ${opportunity_df['Oppo_Forecast'].sum():,.2f}")
        logger.info(f"   - Weighted forecast: ${opportunity_df['Weighted_Forecast'].sum():,.2f}")
        
        total_rows = len(company_df) + len(person_df) + len(opportunity_df)
        logger.info(f"\n📊 Total rows extracted: {total_rows:,}")
        
        logger.info("\n" + "=" * 80)
        logger.info("✅ Extraction completed successfully!")
        logger.info("=" * 80 + "\n")
        
    except Exception as e:
        logger.error(f"\n❌ Extraction failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.warning("\n\n⚠️ Execution interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"\n\n❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

