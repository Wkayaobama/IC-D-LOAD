-- Verify Staging Tables in PostgreSQL
-- Run this in psql, pgAdmin, DBeaver, or any PostgreSQL client

-- 1. Check if staging schema exists
SELECT schema_name
FROM information_schema.schemata
WHERE schema_name = 'staging';

-- 2. List all tables in staging schema
SELECT
    table_schema,
    table_name,
    table_type
FROM information_schema.tables
WHERE table_schema = 'staging'
ORDER BY table_name;

-- 3. Count rows in each staging table
SELECT 'companies_reconciliation' as table_name, COUNT(*) as row_count FROM staging.companies_reconciliation
UNION ALL
SELECT 'contacts_reconciliation', COUNT(*) FROM staging.contacts_reconciliation
UNION ALL
SELECT 'deals_reconciliation', COUNT(*) FROM staging.deals_reconciliation
UNION ALL
SELECT 'communications_reconciliation', COUNT(*) FROM staging.communications_reconciliation
UNION ALL
SELECT 'reconciliation_log', COUNT(*) FROM staging.reconciliation_log;

-- 4. Show structure of companies_reconciliation table
SELECT
    column_name,
    data_type,
    character_maximum_length,
    is_nullable,
    column_default
FROM information_schema.columns
WHERE table_schema = 'staging'
  AND table_name = 'companies_reconciliation'
ORDER BY ordinal_position;

-- 5. Show all indexes
SELECT
    schemaname,
    tablename,
    indexname,
    indexdef
FROM pg_indexes
WHERE schemaname = 'staging'
ORDER BY tablename, indexname;

-- 6. List all schemas (to verify staging is there)
SELECT schema_name
FROM information_schema.schemata
ORDER BY schema_name;
