#!/usr/bin/env bash
#
# UTF-8 CSV Cleaning Pipeline Runner
# ===================================
#
# This script orchestrates the UTF-8 cleaning pipeline for CSV/TSV files.
# It processes files, generates SQL, and optionally deploys to PostgreSQL.
#
# Usage:
#   ./run_utf8_pipeline.sh single data/contacts.csv email
#   ./run_utf8_pipeline.sh batch data/csv_files/ name,description
#   ./run_utf8_pipeline.sh deploy sql/contacts.sql
#
# Configuration:
#   Set environment variables or edit the configuration section below.

set -e  # Exit on error
set -u  # Exit on undefined variable

# ============================================================================
# Configuration
# ============================================================================

# Directories
INPUT_DIR="${INPUT_DIR:-data}"
OUTPUT_DIR="${OUTPUT_DIR:-sql}"
STATS_DIR="${STATS_DIR:-stats}"

# PostgreSQL Configuration
PG_HOST="${PG_HOST:-localhost}"
PG_PORT="${PG_PORT:-5432}"
PG_DATABASE="${PG_DATABASE:-postgres}"
PG_USER="${PG_USER:-postgres}"
PG_PASSWORD="${PG_PASSWORD:-}"

# SSH Configuration (for remote deployment)
SSH_HOST="${SSH_HOST:-}"
SSH_USER="${SSH_USER:-}"
SSH_KEY="${SSH_KEY:-$HOME/.ssh/id_rsa}"

# Pipeline Options
SCHEMA="${SCHEMA:-staging}"
PRESERVE_ACCENTS="${PRESERVE_ACCENTS:-false}"
DEPLOY_AFTER_CLEAN="${DEPLOY_AFTER_CLEAN:-false}"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# ============================================================================
# Helper Functions
# ============================================================================

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

show_usage() {
    cat << EOF
UTF-8 CSV Cleaning Pipeline Runner
===================================

Usage:
    $0 <command> [arguments]

Commands:
    single <file> <column>           Process single CSV file
    batch <directory> <column>       Process all CSV files in directory
    deploy <sql_file>                Deploy SQL to PostgreSQL
    deploy-ssh <sql_file>            Deploy SQL via SSH
    test                             Test connections
    stats                            Show statistics
    help                             Show this help message

Examples:
    $0 single data/contacts.csv email
    $0 single data/companies.csv "name,description"
    $0 batch data/csv_files/ email
    $0 deploy sql/contacts.sql
    $0 deploy-ssh sql/contacts.sql
    $0 test

Environment Variables:
    PG_HOST              PostgreSQL host (default: localhost)
    PG_PORT              PostgreSQL port (default: 5432)
    PG_DATABASE          Database name (default: postgres)
    PG_USER              PostgreSQL user (default: postgres)
    PG_PASSWORD          PostgreSQL password
    SSH_HOST             SSH host for remote deployment
    SSH_USER             SSH username
    SSH_KEY              SSH key path (default: ~/.ssh/id_rsa)
    SCHEMA               Database schema (default: staging)
    PRESERVE_ACCENTS     Preserve accented characters (default: false)
    DEPLOY_AFTER_CLEAN   Auto-deploy after cleaning (default: false)

EOF
}

setup_directories() {
    log_info "Setting up directories..."
    mkdir -p "$OUTPUT_DIR"
    mkdir -p "$STATS_DIR"
    mkdir -p "$INPUT_DIR"
    log_info "✓ Directories ready"
}

# ============================================================================
# Pipeline Commands
# ============================================================================

process_single_csv() {
    local file="$1"
    local columns="$2"

    if [[ ! -f "$file" ]]; then
        log_error "File not found: $file"
        exit 1
    fi

    local basename=$(basename "$file" .csv)
    local output_sql="$OUTPUT_DIR/${basename}.sql"
    local stats_csv="$STATS_DIR/${basename}_stats.csv"

    log_info "Processing CSV: $file"
    log_info "Target columns: $columns"

    # Build column arguments
    local column_args=""
    IFS=',' read -ra COLS <<< "$columns"
    for col in "${COLS[@]}"; do
        column_args="$column_args --column $col"
    done

    # Build preserve-accents flag
    local accent_flag=""
    if [[ "$PRESERVE_ACCENTS" == "true" ]]; then
        accent_flag="--preserve-accents"
    fi

    # Run cleaner
    python3 csv_utf8_cleaner.py \
        --input "$file" \
        $column_args \
        --output "$output_sql" \
        --stats-output "$stats_csv" \
        --schema "$SCHEMA" \
        $accent_flag

    log_info "✓ CSV processed successfully"
    log_info "  SQL output: $output_sql"
    log_info "  Statistics: $stats_csv"

    # Auto-deploy if enabled
    if [[ "$DEPLOY_AFTER_CLEAN" == "true" ]]; then
        log_info "Auto-deploying SQL..."
        deploy_sql "$output_sql"
    fi
}

process_batch_csv() {
    local directory="$1"
    local columns="$2"

    if [[ ! -d "$directory" ]]; then
        log_error "Directory not found: $directory"
        exit 1
    fi

    log_info "Processing CSV files in: $directory"
    log_info "Target columns: $columns"

    # Build column arguments
    local column_args=""
    IFS=',' read -ra COLS <<< "$columns"
    for col in "${COLS[@]}"; do
        column_args="$column_args --column $col"
    done

    # Build preserve-accents flag
    local accent_flag=""
    if [[ "$PRESERVE_ACCENTS" == "true" ]]; then
        accent_flag="--preserve-accents"
    fi

    # Run batch cleaner
    python3 csv_utf8_cleaner.py \
        --input-dir "$directory" \
        $column_args \
        --output-dir "$OUTPUT_DIR" \
        --stats-output "$STATS_DIR/batch_stats.csv" \
        --schema "$SCHEMA" \
        $accent_flag

    log_info "✓ Batch processing complete"
    log_info "  SQL files: $OUTPUT_DIR/"
    log_info "  Statistics: $STATS_DIR/batch_stats.csv"
}

deploy_sql() {
    local sql_file="$1"

    if [[ ! -f "$sql_file" ]]; then
        log_error "SQL file not found: $sql_file"
        exit 1
    fi

    log_info "Deploying SQL to PostgreSQL..."
    log_info "  Database: $PG_DATABASE"
    log_info "  Host: $PG_HOST:$PG_PORT"

    export PGPASSWORD="$PG_PASSWORD"

    psql \
        -h "$PG_HOST" \
        -p "$PG_PORT" \
        -U "$PG_USER" \
        -d "$PG_DATABASE" \
        -f "$sql_file"

    log_info "✓ SQL deployed successfully"
}

deploy_sql_ssh() {
    local sql_file="$1"

    if [[ ! -f "$sql_file" ]]; then
        log_error "SQL file not found: $sql_file"
        exit 1
    fi

    if [[ -z "$SSH_HOST" ]]; then
        log_error "SSH_HOST not configured"
        exit 1
    fi

    log_info "Deploying SQL via SSH to $SSH_HOST..."

    # Upload SQL file
    log_info "Uploading SQL file..."
    scp -i "$SSH_KEY" "$sql_file" "$SSH_USER@$SSH_HOST:/tmp/deploy.sql"

    # Execute SQL
    log_info "Executing SQL..."
    ssh -i "$SSH_KEY" "$SSH_USER@$SSH_HOST" \
        "PGPASSWORD='$PG_PASSWORD' psql -h $PG_HOST -p $PG_PORT -U $PG_USER -d $PG_DATABASE -f /tmp/deploy.sql"

    # Cleanup
    log_info "Cleaning up..."
    ssh -i "$SSH_KEY" "$SSH_USER@$SSH_HOST" "rm /tmp/deploy.sql"

    log_info "✓ SQL deployed successfully via SSH"
}

test_connections() {
    log_info "Testing connections..."

    # Test local PostgreSQL
    log_info "1. Testing PostgreSQL connection..."
    export PGPASSWORD="$PG_PASSWORD"

    if psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DATABASE" -c "SELECT version();" > /dev/null 2>&1; then
        log_info "   ✓ PostgreSQL connection OK"
    else
        log_error "   ✗ PostgreSQL connection FAILED"
    fi

    # Test SSH (if configured)
    if [[ -n "$SSH_HOST" ]]; then
        log_info "2. Testing SSH connection..."
        if ssh -i "$SSH_KEY" "$SSH_USER@$SSH_HOST" "echo 'SSH OK'" > /dev/null 2>&1; then
            log_info "   ✓ SSH connection OK"
        else
            log_error "   ✗ SSH connection FAILED"
        fi
    else
        log_warn "2. SSH not configured (skipping)"
    fi

    # Test Python script
    log_info "3. Testing Python script..."
    if python3 csv_utf8_cleaner.py --help > /dev/null 2>&1; then
        log_info "   ✓ Python script OK"
    else
        log_error "   ✗ Python script FAILED"
    fi

    log_info "✓ Connection tests complete"
}

show_statistics() {
    log_info "UTF-8 Replacement Statistics"
    echo "=============================="

    if [[ -f "$STATS_DIR/batch_stats.csv" ]]; then
        log_info "Batch statistics:"
        head -20 "$STATS_DIR/batch_stats.csv"
    else
        log_warn "No batch statistics found"
    fi

    echo ""
    log_info "Individual file statistics:"
    ls -lh "$STATS_DIR"/*.csv 2>/dev/null || log_warn "No statistics files found"
}

# ============================================================================
# Main
# ============================================================================

main() {
    setup_directories

    local command="${1:-help}"

    case "$command" in
        single)
            if [[ $# -lt 3 ]]; then
                log_error "Usage: $0 single <file> <column>"
                exit 1
            fi
            process_single_csv "$2" "$3"
            ;;

        batch)
            if [[ $# -lt 3 ]]; then
                log_error "Usage: $0 batch <directory> <column>"
                exit 1
            fi
            process_batch_csv "$2" "$3"
            ;;

        deploy)
            if [[ $# -lt 2 ]]; then
                log_error "Usage: $0 deploy <sql_file>"
                exit 1
            fi
            deploy_sql "$2"
            ;;

        deploy-ssh)
            if [[ $# -lt 2 ]]; then
                log_error "Usage: $0 deploy-ssh <sql_file>"
                exit 1
            fi
            deploy_sql_ssh "$2"
            ;;

        test)
            test_connections
            ;;

        stats)
            show_statistics
            ;;

        help|--help|-h)
            show_usage
            ;;

        *)
            log_error "Unknown command: $command"
            show_usage
            exit 1
            ;;
    esac
}

# Run main function
main "$@"
