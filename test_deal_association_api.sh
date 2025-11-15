#!/bin/bash
# ============================================================================
# Deal-Company Association API Test Script
# ============================================================================
# Purpose: Test HubSpot API for creating Deal-Company associations
# Association Type: 6 (Deal with primary Company)
# Filter: Deals with icalps_deal_id starting with '4'
#
# Prerequisites:
# - HUBSPOT_TOKEN environment variable set
# - PostgreSQL connection configured in postgres_connection_manager.py
# - At least one deal with icalps_deal_id starting with '4'
#
# Usage:
#   export HUBSPOT_TOKEN="your_private_app_token"
#   ./test_deal_association_api.sh [deal_id] [company_id]
#
# If deal_id and company_id not provided, script will query database for test deal
# ============================================================================

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
HUBSPOT_API_BASE="https://api.hubapi.com"
ASSOCIATION_TYPE=6  # Deal with primary Company

# ============================================================================
# Functions
# ============================================================================

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if HubSpot token is set
check_token() {
    if [[ -z "${HUBSPOT_TOKEN:-}" ]]; then
        log_error "HUBSPOT_TOKEN environment variable not set"
        log_info "Usage: export HUBSPOT_TOKEN='your_private_app_token'"
        exit 1
    fi
    log_success "HubSpot token found"
}

# Get test deal from database
get_test_deal_from_db() {
    log_info "Querying database for test deal..."

    python3 - <<EOF
import sys
from postgres_connection_manager import PostgreSQLManager

try:
    pg = PostgreSQLManager()

    query = """
    SELECT
        hs_object_id as deal_id,
        associatedcompanyid as company_id,
        dealname,
        icalps_deal_id
    FROM hubspot.deals
    WHERE icalps_deal_id::TEXT LIKE '4%'
      AND associatedcompanyid IS NOT NULL
    LIMIT 1;
    """

    results = pg.execute_query(query)

    if not results:
        print("ERROR: No deals found with icalps_deal_id starting with '4'", file=sys.stderr)
        sys.exit(1)

    deal = results[0]
    print(f"{deal['deal_id']}|{deal['company_id']}|{deal['dealname']}|{deal['icalps_deal_id']}")

except Exception as e:
    print(f"ERROR: Failed to query database: {e}", file=sys.stderr)
    sys.exit(1)
EOF
}

# Create association via HubSpot API
create_association() {
    local deal_id=$1
    local company_id=$2

    log_info "Creating association:"
    log_info "  Deal ID: ${deal_id}"
    log_info "  Company ID: ${company_id}"
    log_info "  Association Type: ${ASSOCIATION_TYPE} (Deal with primary Company)"

    # HubSpot API endpoint
    local endpoint="${HUBSPOT_API_BASE}/crm/v3/objects/deals/${deal_id}/associations/companies/${company_id}/${ASSOCIATION_TYPE}"

    log_info "API Endpoint: ${endpoint}"

    # Make API call
    local response
    local http_code

    response=$(curl -s -w "\n%{http_code}" -X PUT "${endpoint}" \
        -H "Authorization: Bearer ${HUBSPOT_TOKEN}" \
        -H "Content-Type: application/json")

    http_code=$(echo "$response" | tail -n1)
    local body=$(echo "$response" | sed '$d')

    echo ""
    log_info "HTTP Status Code: ${http_code}"

    # Check response
    if [[ "$http_code" == "200" ]] || [[ "$http_code" == "201" ]]; then
        log_success "Association created successfully!"
        echo ""
        log_info "Response:"
        echo "$body" | python3 -m json.tool 2>/dev/null || echo "$body"
        return 0
    elif [[ "$http_code" == "409" ]]; then
        log_warning "Association already exists (409 Conflict)"
        echo ""
        log_info "Response:"
        echo "$body" | python3 -m json.tool 2>/dev/null || echo "$body"
        return 0
    else
        log_error "Failed to create association"
        echo ""
        log_error "Response:"
        echo "$body" | python3 -m json.tool 2>/dev/null || echo "$body"
        return 1
    fi
}

# Verify association in database
verify_association_in_db() {
    local deal_id=$1
    local company_id=$2

    log_info "Verifying association in database..."

    python3 - <<EOF "$deal_id" "$company_id"
import sys
from postgres_connection_manager import PostgreSQLManager

deal_id = sys.argv[1]
company_id = sys.argv[2]

try:
    pg = PostgreSQLManager()

    # Check if association table exists
    check_table_query = """
    SELECT EXISTS (
        SELECT FROM information_schema.tables
        WHERE table_schema = 'hubspot'
        AND table_name = 'association_company_deal'
    );
    """

    table_exists = pg.execute_query(check_table_query)[0]['exists']

    if not table_exists:
        print("⚠ WARNING: association_company_deal table not found in database", file=sys.stderr)
        print("  Association may exist in HubSpot but not synced to database yet", file=sys.stderr)
        sys.exit(0)

    # Query association
    query = f"""
    SELECT
        from_object_id as company_id,
        to_object_id as deal_id,
        association_type_id,
        created_at
    FROM hubspot.association_company_deal
    WHERE to_object_id = {deal_id}
      AND from_object_id = {company_id}
      AND association_type_id = 6;
    """

    results = pg.execute_query(query)

    if results:
        print(f"✓ Association found in database:")
        print(f"  Company ID: {results[0]['company_id']}")
        print(f"  Deal ID: {results[0]['deal_id']}")
        print(f"  Association Type: {results[0]['association_type_id']}")
        print(f"  Created At: {results[0]['created_at']}")
    else:
        print("⚠ WARNING: Association not found in database (may not be synced yet)", file=sys.stderr)

except Exception as e:
    print(f"ERROR: Failed to verify in database: {e}", file=sys.stderr)
    sys.exit(1)
EOF
}

# ============================================================================
# Main Execution
# ============================================================================

main() {
    echo "============================================================================"
    echo "HubSpot Deal-Company Association API Test"
    echo "============================================================================"
    echo ""

    # Check prerequisites
    check_token

    # Get deal and company IDs
    local deal_id
    local company_id
    local deal_name
    local legacy_id

    if [[ $# -eq 2 ]]; then
        # IDs provided as arguments
        deal_id=$1
        company_id=$2
        log_info "Using provided IDs:"
        log_info "  Deal ID: ${deal_id}"
        log_info "  Company ID: ${company_id}"
    else
        # Query database for test deal
        local db_result
        if ! db_result=$(get_test_deal_from_db); then
            log_error "Failed to retrieve test deal from database"
            exit 1
        fi

        IFS='|' read -r deal_id company_id deal_name legacy_id <<< "$db_result"

        log_success "Retrieved test deal from database:"
        log_info "  Deal ID (HubSpot): ${deal_id}"
        log_info "  Deal ID (Legacy): ${legacy_id}"
        log_info "  Deal Name: ${deal_name}"
        log_info "  Company ID: ${company_id}"
    fi

    echo ""
    echo "----------------------------------------------------------------------------"
    echo "Creating Association"
    echo "----------------------------------------------------------------------------"
    echo ""

    # Create association
    if create_association "$deal_id" "$company_id"; then
        echo ""
        echo "----------------------------------------------------------------------------"
        echo "Verifying Association"
        echo "----------------------------------------------------------------------------"
        echo ""

        # Verify in database (optional - may not be synced immediately)
        verify_association_in_db "$deal_id" "$company_id" || true

        echo ""
        echo "============================================================================"
        log_success "API TEST COMPLETE"
        echo "============================================================================"
        echo ""
        log_info "Next Steps:"
        log_info "  1. Verify association in HubSpot UI"
        log_info "  2. If successful, proceed with batch SQL processing"
        log_info "  3. Run queries from deal_company_association_workflow.sql"
        echo ""
        exit 0
    else
        echo ""
        echo "============================================================================"
        log_error "API TEST FAILED"
        echo "============================================================================"
        exit 1
    fi
}

# Run main function
main "$@"
