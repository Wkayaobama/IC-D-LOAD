# Makefile for UTF-8 CSV Cleaning Pipeline
# =========================================
#
# This Makefile orchestrates the CSV/TSV UTF-8 cleaning pipeline:
# 1. Clean UTF-8 characters from CSV files
# 2. Generate SQL for staging and production
# 3. Execute SQL on PostgreSQL (local or SSH)
# 4. Generate summary statistics
#
# Usage:
#   make clean-csv FILE=data/input.csv COLUMN=email
#   make clean-batch DIR=data/csv_files COLUMN=description
#   make deploy-sql FILE=sql/output.sql
#   make pipeline FILE=data/input.csv COLUMN=email
#   make help

.PHONY: help clean-csv clean-batch clean-tsv generate-sql deploy-sql deploy-ssh pipeline test

# Default configuration
PYTHON := python3
INPUT_DIR := data
OUTPUT_DIR := sql
STATS_DIR := stats
SEPARATOR := ","
SCHEMA := staging
PRESERVE_ACCENTS := false

# PostgreSQL configuration (override via environment or command line)
PG_HOST ?= localhost
PG_PORT ?= 5432
PG_DATABASE ?= postgres
PG_USER ?= postgres
PG_PASSWORD ?=

# SSH configuration (for remote PostgreSQL)
SSH_HOST ?=
SSH_USER ?=
SSH_KEY ?= ~/.ssh/id_rsa

# Colors for output
GREEN := \033[0;32m
YELLOW := \033[0;33m
RED := \033[0;31m
NC := \033[0m # No Color

# ============================================================================
# Help
# ============================================================================

help:
	@echo "UTF-8 CSV Cleaning Pipeline - Makefile"
	@echo "======================================"
	@echo ""
	@echo "Targets:"
	@echo "  make clean-csv FILE=<file> COLUMN=<col>    Clean single CSV file"
	@echo "  make clean-batch DIR=<dir> COLUMN=<col>    Clean all CSV files in directory"
	@echo "  make clean-tsv FILE=<file> COLUMN=<col>    Clean TSV file"
	@echo "  make deploy-sql FILE=<sql>                 Deploy SQL to PostgreSQL (local)"
	@echo "  make deploy-ssh FILE=<sql>                 Deploy SQL via SSH"
	@echo "  make pipeline FILE=<file> COLUMN=<col>     Run full pipeline (clean + deploy)"
	@echo "  make test                                  Test connections"
	@echo "  make stats                                 View statistics"
	@echo "  make clean                                 Clean generated files"
	@echo ""
	@echo "Examples:"
	@echo "  make clean-csv FILE=data/contacts.csv COLUMN=email"
	@echo "  make clean-batch DIR=data/csv_files COLUMN=name COLUMN=description"
	@echo "  make deploy-sql FILE=sql/contacts.sql"
	@echo "  make pipeline FILE=data/contacts.csv COLUMN=email TABLE=contacts"
	@echo ""
	@echo "Configuration:"
	@echo "  PYTHON=$(PYTHON)"
	@echo "  INPUT_DIR=$(INPUT_DIR)"
	@echo "  OUTPUT_DIR=$(OUTPUT_DIR)"
	@echo "  SCHEMA=$(SCHEMA)"

# ============================================================================
# Setup
# ============================================================================

setup:
	@echo "$(GREEN)Setting up directories...$(NC)"
	mkdir -p $(OUTPUT_DIR)
	mkdir -p $(STATS_DIR)
	mkdir -p data
	@echo "$(GREEN)✓ Directories created$(NC)"

install:
	@echo "$(GREEN)Installing Python dependencies...$(NC)"
	$(PYTHON) -m pip install -r requirements.txt
	@echo "$(GREEN)✓ Dependencies installed$(NC)"

# ============================================================================
# CSV Cleaning
# ============================================================================

clean-csv: setup
ifndef FILE
	@echo "$(RED)Error: FILE parameter required$(NC)"
	@echo "Usage: make clean-csv FILE=data/input.csv COLUMN=column_name"
	@exit 1
endif
ifndef COLUMN
	@echo "$(RED)Error: COLUMN parameter required$(NC)"
	@echo "Usage: make clean-csv FILE=data/input.csv COLUMN=column_name"
	@exit 1
endif
	@echo "$(GREEN)Cleaning CSV file: $(FILE)$(NC)"
	@echo "Target column(s): $(COLUMN)"
	$(PYTHON) csv_utf8_cleaner.py \
		--input $(FILE) \
		--column $(COLUMN) \
		--output $(OUTPUT_DIR)/$$(basename $(FILE) .csv).sql \
		--stats-output $(STATS_DIR)/$$(basename $(FILE) .csv)_stats.csv \
		--separator "$(SEPARATOR)" \
		--schema $(SCHEMA) \
		$(if $(filter true,$(PRESERVE_ACCENTS)),--preserve-accents)
	@echo "$(GREEN)✓ CSV cleaned and SQL generated$(NC)"

clean-tsv: SEPARATOR := \t
clean-tsv: clean-csv

clean-batch: setup
ifndef DIR
	@echo "$(RED)Error: DIR parameter required$(NC)"
	@echo "Usage: make clean-batch DIR=data/csv_files COLUMN=column_name"
	@exit 1
endif
ifndef COLUMN
	@echo "$(RED)Error: COLUMN parameter required$(NC)"
	@echo "Usage: make clean-batch DIR=data/csv_files COLUMN=column_name"
	@exit 1
endif
	@echo "$(GREEN)Batch cleaning CSV files in: $(DIR)$(NC)"
	@echo "Target column(s): $(COLUMN)"
	$(PYTHON) csv_utf8_cleaner.py \
		--input-dir $(DIR) \
		--column $(COLUMN) \
		--output-dir $(OUTPUT_DIR) \
		--stats-output $(STATS_DIR)/batch_stats.csv \
		--separator "$(SEPARATOR)" \
		--schema $(SCHEMA) \
		$(if $(filter true,$(PRESERVE_ACCENTS)),--preserve-accents)
	@echo "$(GREEN)✓ Batch processing complete$(NC)"

# ============================================================================
# SQL Deployment
# ============================================================================

deploy-sql:
ifndef FILE
	@echo "$(RED)Error: FILE parameter required$(NC)"
	@echo "Usage: make deploy-sql FILE=sql/output.sql"
	@exit 1
endif
	@echo "$(GREEN)Deploying SQL to PostgreSQL...$(NC)"
	@echo "Database: $(PG_DATABASE)"
	PGPASSWORD=$(PG_PASSWORD) psql \
		-h $(PG_HOST) \
		-p $(PG_PORT) \
		-U $(PG_USER) \
		-d $(PG_DATABASE) \
		-f $(FILE)
	@echo "$(GREEN)✓ SQL deployed successfully$(NC)"

deploy-ssh:
ifndef FILE
	@echo "$(RED)Error: FILE parameter required$(NC)"
	@echo "Usage: make deploy-ssh FILE=sql/output.sql"
	@exit 1
endif
ifndef SSH_HOST
	@echo "$(RED)Error: SSH_HOST not configured$(NC)"
	@echo "Set SSH_HOST, SSH_USER, and optionally SSH_KEY"
	@exit 1
endif
	@echo "$(GREEN)Deploying SQL via SSH to $(SSH_HOST)...$(NC)"
	@echo "Uploading SQL file..."
	scp -i $(SSH_KEY) $(FILE) $(SSH_USER)@$(SSH_HOST):/tmp/deploy.sql
	@echo "Executing SQL..."
	ssh -i $(SSH_KEY) $(SSH_USER)@$(SSH_HOST) \
		"PGPASSWORD=$(PG_PASSWORD) psql -h $(PG_HOST) -p $(PG_PORT) -U $(PG_USER) -d $(PG_DATABASE) -f /tmp/deploy.sql"
	@echo "Cleaning up..."
	ssh -i $(SSH_KEY) $(SSH_USER)@$(SSH_HOST) "rm /tmp/deploy.sql"
	@echo "$(GREEN)✓ SQL deployed successfully via SSH$(NC)"

# ============================================================================
# Full Pipeline
# ============================================================================

pipeline: clean-csv
ifndef FILE
	@echo "$(RED)Error: FILE parameter required$(NC)"
	@exit 1
endif
	@echo "$(GREEN)Running full pipeline...$(NC)"
	@$(MAKE) deploy-sql FILE=$(OUTPUT_DIR)/$$(basename $(FILE) .csv).sql
	@echo "$(GREEN)✓ Pipeline complete!$(NC)"

pipeline-ssh: clean-csv
ifndef FILE
	@echo "$(RED)Error: FILE parameter required$(NC)"
	@exit 1
endif
	@echo "$(GREEN)Running full pipeline with SSH deployment...$(NC)"
	@$(MAKE) deploy-ssh FILE=$(OUTPUT_DIR)/$$(basename $(FILE) .csv).sql
	@echo "$(GREEN)✓ Pipeline complete!$(NC)"

# ============================================================================
# Testing
# ============================================================================

test:
	@echo "$(GREEN)Testing connections...$(NC)"
	@echo "1. Testing local PostgreSQL connection..."
	-PGPASSWORD=$(PG_PASSWORD) psql \
		-h $(PG_HOST) \
		-p $(PG_PORT) \
		-U $(PG_USER) \
		-d $(PG_DATABASE) \
		-c "SELECT version();"
	@echo ""
	@echo "2. Testing Python script..."
	$(PYTHON) csv_utf8_cleaner.py --help
	@echo "$(GREEN)✓ Tests complete$(NC)"

test-ssh:
ifndef SSH_HOST
	@echo "$(RED)Error: SSH_HOST not configured$(NC)"
	@exit 1
endif
	@echo "$(GREEN)Testing SSH connection to $(SSH_HOST)...$(NC)"
	ssh -i $(SSH_KEY) $(SSH_USER)@$(SSH_HOST) "echo 'SSH connection OK' && psql --version"
	@echo "$(GREEN)✓ SSH test complete$(NC)"

# ============================================================================
# Statistics & Reporting
# ============================================================================

stats:
	@echo "$(GREEN)UTF-8 Replacement Statistics$(NC)"
	@echo "=============================="
	@if [ -f $(STATS_DIR)/batch_stats.csv ]; then \
		echo "Batch statistics:"; \
		cat $(STATS_DIR)/batch_stats.csv | head -20; \
	else \
		echo "No batch statistics found. Run clean-batch first."; \
	fi
	@echo ""
	@echo "Individual file statistics:"
	@ls -lh $(STATS_DIR)/*.csv 2>/dev/null || echo "No statistics files found"

git-status:
	@echo "$(GREEN)Git Status$(NC)"
	@git status
	@echo ""
	@echo "$(GREEN)Recent Commits$(NC)"
	@git log --oneline -5

git-diff:
	@echo "$(GREEN)Git Diff$(NC)"
	@git diff

# ============================================================================
# Cleanup
# ============================================================================

clean:
	@echo "$(YELLOW)Cleaning generated files...$(NC)"
	rm -rf $(OUTPUT_DIR)/*.sql
	rm -rf $(STATS_DIR)/*.csv
	@echo "$(GREEN)✓ Cleanup complete$(NC)"

clean-all: clean
	@echo "$(YELLOW)Removing all generated directories...$(NC)"
	rm -rf $(OUTPUT_DIR)
	rm -rf $(STATS_DIR)
	@echo "$(GREEN)✓ Deep cleanup complete$(NC)"

# ============================================================================
# Example Workflows
# ============================================================================

example-single:
	@echo "$(GREEN)Example: Cleaning single CSV file$(NC)"
	@echo "This would run:"
	@echo "  make clean-csv FILE=data/contacts.csv COLUMN=email"

example-batch:
	@echo "$(GREEN)Example: Batch cleaning CSV files$(NC)"
	@echo "This would run:"
	@echo "  make clean-batch DIR=data/csv_files COLUMN=name COLUMN=description"

example-pipeline:
	@echo "$(GREEN)Example: Full pipeline with deployment$(NC)"
	@echo "This would run:"
	@echo "  make pipeline FILE=data/contacts.csv COLUMN=email TABLE=contacts"

# ============================================================================
# Development
# ============================================================================

dev-setup: install setup
	@echo "$(GREEN)Development environment ready!$(NC)"

format:
	@echo "$(GREEN)Formatting Python code...$(NC)"
	$(PYTHON) -m black csv_utf8_cleaner.py ssh_postgres_manager.py
	@echo "$(GREEN)✓ Code formatted$(NC)"

lint:
	@echo "$(GREEN)Linting Python code...$(NC)"
	$(PYTHON) -m flake8 csv_utf8_cleaner.py ssh_postgres_manager.py --max-line-length=100
	@echo "$(GREEN)✓ Linting complete$(NC)"
