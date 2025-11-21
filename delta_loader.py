#!/usr/bin/env python3
"""
Delta/Differential Loading System
==================================

Implements incremental loading by comparing CSV snapshots and only processing changes.
This replaces the R script differential loading logic with a more maintainable approach.

Meta-Cognitive Principles:
1. Configuration over hardcoding
2. State management for incremental processing
3. Composable functions
4. Proper logging and debugging

Usage:
    from delta_loader import DeltaLoader

    loader = DeltaLoader(entity_type='communications')
    delta = loader.detect_changes(
        current_csv='data/communications_2024_11_21.csv',
        schema_columns=['Record ID', 'Communication Subject', ...]
    )
    loader.load_delta_to_staging(delta)
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Set
from dataclasses import dataclass, field, asdict
import hashlib
import json
from datetime import datetime
from loguru import logger
import psycopg2
from psycopg2.extras import execute_batch
from postgres_connection_manager import PostgreSQLManager


# =============================================================================
# STATE MANAGEMENT - Tracks what was loaded previously
# =============================================================================

@dataclass
class LoadState:
    """
    Tracks the state of the last successful load.

    Meta-Cognitive: This enables incremental processing
    - Stores fingerprint (hash) of last loaded data
    - Tracks record counts
    - Records timestamp of last load
    """
    entity_type: str
    last_load_timestamp: datetime
    last_csv_path: str
    last_row_count: int
    last_data_fingerprint: str  # Hash of data
    column_schema: List[str]
    primary_keys: List[str]
    metadata: Dict = field(default_factory=dict)

    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON storage."""
        data = asdict(self)
        data['last_load_timestamp'] = self.last_load_timestamp.isoformat()
        return data

    @classmethod
    def from_dict(cls, data: Dict) -> 'LoadState':
        """Load from dictionary."""
        data['last_load_timestamp'] = datetime.fromisoformat(data['last_load_timestamp'])
        return cls(**data)

    def save(self, state_dir: Path = Path('state')):
        """Save state to JSON file."""
        state_dir.mkdir(exist_ok=True)
        state_file = state_dir / f"{self.entity_type}_load_state.json"

        with open(state_file, 'w') as f:
            json.dump(self.to_dict(), f, indent=2)

        logger.info(f"Saved load state for {self.entity_type} to {state_file}")

    @classmethod
    def load(cls, entity_type: str, state_dir: Path = Path('state')) -> Optional['LoadState']:
        """Load state from JSON file."""
        state_file = state_dir / f"{entity_type}_load_state.json"

        if not state_file.exists():
            logger.info(f"No previous state found for {entity_type}")
            return None

        with open(state_file, 'r') as f:
            data = json.load(f)

        logger.info(f"Loaded previous state for {entity_type} from {state_file}")
        return cls.from_dict(data)


@dataclass
class DeltaResult:
    """
    Results of delta detection.

    Contains:
    - new_records: Records that don't exist in previous load
    - modified_records: Records that exist but have changed
    - deleted_records: Records that existed before but are now missing
    - unchanged_records: Records that haven't changed (optional, for logging)
    """
    entity_type: str
    new_records: pd.DataFrame
    modified_records: pd.DataFrame
    deleted_record_ids: List
    unchanged_record_ids: List = field(default_factory=list)
    detection_timestamp: datetime = field(default_factory=datetime.now)

    @property
    def has_changes(self) -> bool:
        """Check if any changes detected."""
        return (
            len(self.new_records) > 0 or
            len(self.modified_records) > 0 or
            len(self.deleted_record_ids) > 0
        )

    def summary(self) -> Dict:
        """Get summary statistics."""
        return {
            'entity_type': self.entity_type,
            'new_count': len(self.new_records),
            'modified_count': len(self.modified_records),
            'deleted_count': len(self.deleted_record_ids),
            'unchanged_count': len(self.unchanged_record_ids),
            'has_changes': self.has_changes,
            'detection_timestamp': self.detection_timestamp.isoformat()
        }


# =============================================================================
# DELTA DETECTION ENGINE
# =============================================================================

class DeltaLoader:
    """
    Differential/incremental data loader.

    Meta-Cognitive Design:
    - Only processes changed data (not full reloads)
    - Uses content hashing for change detection
    - Maintains state between runs
    - Configurable per entity type
    """

    def __init__(
        self,
        entity_type: str,
        primary_keys: List[str],
        pg_manager: Optional[PostgreSQLManager] = None,
        state_dir: Path = Path('state')
    ):
        """
        Initialize delta loader.

        Args:
            entity_type: Type of entity (e.g., 'communications', 'deals')
            primary_keys: Column(s) that uniquely identify records
            pg_manager: PostgreSQL connection manager
            state_dir: Directory to store state files
        """
        self.entity_type = entity_type
        self.primary_keys = primary_keys if isinstance(primary_keys, list) else [primary_keys]
        self.pg_manager = pg_manager or PostgreSQLManager()
        self.state_dir = Path(state_dir)
        self.state_dir.mkdir(exist_ok=True)

        # Load previous state
        self.previous_state = LoadState.load(entity_type, self.state_dir)

        logger.info(f"Initialized DeltaLoader for {entity_type}")
        logger.info(f"Primary keys: {self.primary_keys}")

    def compute_record_fingerprint(self, row: pd.Series) -> str:
        """
        Compute hash fingerprint for a single record.

        Meta-Cognitive: Hash-based change detection
        - Fast comparison (don't compare every column)
        - Detects any change in content
        - Deterministic (same data → same hash)
        """
        # Convert row to string and hash
        # Sort to ensure consistent ordering
        row_str = str(sorted(row.items()))
        return hashlib.md5(row_str.encode()).hexdigest()

    def compute_dataset_fingerprint(self, df: pd.DataFrame) -> str:
        """Compute fingerprint for entire dataset."""
        # Use pandas' built-in hash function (fast)
        return str(pd.util.hash_pandas_object(df).sum())

    def add_fingerprints(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add fingerprint column to dataframe."""
        df = df.copy()
        df['_record_fingerprint'] = df.apply(self.compute_record_fingerprint, axis=1)
        return df

    def detect_changes(
        self,
        current_csv: str,
        schema_columns: List[str],
        separator: str = ','
    ) -> DeltaResult:
        """
        Detect changes between current CSV and previous load.

        Meta-Cognitive: Delta detection algorithm
        1. Load current CSV
        2. Load previous CSV (if exists)
        3. Compare fingerprints to find:
           - New records (in current, not in previous)
           - Modified records (in both, different fingerprint)
           - Deleted records (in previous, not in current)

        Args:
            current_csv: Path to current CSV file
            schema_columns: Expected columns (for validation)
            separator: CSV separator

        Returns:
            DeltaResult with new/modified/deleted records
        """
        logger.info(f"Detecting changes for {self.entity_type}")
        logger.info(f"Current CSV: {current_csv}")

        # Load current data
        current_df = pd.read_csv(current_csv, sep=separator, dtype=object)

        # Validate schema
        missing_cols = set(schema_columns) - set(current_df.columns)
        if missing_cols:
            raise ValueError(f"Missing columns in CSV: {missing_cols}")

        logger.info(f"Loaded current data: {len(current_df)} records")

        # If no previous state, everything is new
        if self.previous_state is None:
            logger.info("No previous state found - all records are new")

            new_state = LoadState(
                entity_type=self.entity_type,
                last_load_timestamp=datetime.now(),
                last_csv_path=str(current_csv),
                last_row_count=len(current_df),
                last_data_fingerprint=self.compute_dataset_fingerprint(current_df),
                column_schema=schema_columns,
                primary_keys=self.primary_keys
            )
            new_state.save(self.state_dir)

            return DeltaResult(
                entity_type=self.entity_type,
                new_records=current_df,
                modified_records=pd.DataFrame(columns=current_df.columns),
                deleted_record_ids=[]
            )

        # Load previous data
        previous_csv = self.previous_state.last_csv_path
        if not Path(previous_csv).exists():
            logger.warning(f"Previous CSV not found: {previous_csv}")
            logger.info("Treating all records as new")

            return DeltaResult(
                entity_type=self.entity_type,
                new_records=current_df,
                modified_records=pd.DataFrame(columns=current_df.columns),
                deleted_record_ids=[]
            )

        previous_df = pd.read_csv(previous_csv, sep=separator, dtype=object)
        logger.info(f"Loaded previous data: {len(previous_df)} records")

        # Add fingerprints
        current_with_fp = self.add_fingerprints(current_df)
        previous_with_fp = self.add_fingerprints(previous_df)

        # Create composite key for matching
        def make_composite_key(df):
            if len(self.primary_keys) == 1:
                return df[self.primary_keys[0]].astype(str)
            else:
                return df[self.primary_keys].astype(str).agg('_'.join, axis=1)

        current_with_fp['_composite_key'] = make_composite_key(current_with_fp)
        previous_with_fp['_composite_key'] = make_composite_key(previous_with_fp)

        # Find new, modified, deleted records
        current_keys = set(current_with_fp['_composite_key'])
        previous_keys = set(previous_with_fp['_composite_key'])

        new_keys = current_keys - previous_keys
        deleted_keys = previous_keys - current_keys
        common_keys = current_keys & previous_keys

        logger.info(f"Key analysis:")
        logger.info(f"  New keys: {len(new_keys)}")
        logger.info(f"  Deleted keys: {len(deleted_keys)}")
        logger.info(f"  Common keys: {len(common_keys)}")

        # Extract new records
        new_records = current_df[current_with_fp['_composite_key'].isin(new_keys)].copy()

        # Find modified records (common keys but different fingerprints)
        modified_records_list = []
        unchanged_keys = []

        for key in common_keys:
            current_fp = current_with_fp[current_with_fp['_composite_key'] == key]['_record_fingerprint'].iloc[0]
            previous_fp = previous_with_fp[previous_with_fp['_composite_key'] == key]['_record_fingerprint'].iloc[0]

            if current_fp != previous_fp:
                # Record was modified
                modified_record = current_df[current_with_fp['_composite_key'] == key]
                modified_records_list.append(modified_record)
            else:
                unchanged_keys.append(key)

        modified_records = pd.concat(modified_records_list, ignore_index=True) if modified_records_list else pd.DataFrame(columns=current_df.columns)

        logger.info(f"Change detection results:")
        logger.info(f"  New records: {len(new_records)}")
        logger.info(f"  Modified records: {len(modified_records)}")
        logger.info(f"  Deleted records: {len(deleted_keys)}")
        logger.info(f"  Unchanged records: {len(unchanged_keys)}")

        # Save new state
        new_state = LoadState(
            entity_type=self.entity_type,
            last_load_timestamp=datetime.now(),
            last_csv_path=str(current_csv),
            last_row_count=len(current_df),
            last_data_fingerprint=self.compute_dataset_fingerprint(current_df),
            column_schema=schema_columns,
            primary_keys=self.primary_keys,
            metadata={
                'new_count': len(new_records),
                'modified_count': len(modified_records),
                'deleted_count': len(deleted_keys)
            }
        )
        new_state.save(self.state_dir)

        return DeltaResult(
            entity_type=self.entity_type,
            new_records=new_records,
            modified_records=modified_records,
            deleted_record_ids=list(deleted_keys),
            unchanged_record_ids=unchanged_keys
        )

    def load_delta_to_staging(
        self,
        delta: DeltaResult,
        staging_table: str,
        schema: str = 'staging'
    ):
        """
        Load delta (changes only) to PostgreSQL staging table.

        Meta-Cognitive: Only load what changed
        - INSERT new records
        - UPDATE modified records
        - Mark deleted records (soft delete)

        Args:
            delta: Delta detection results
            staging_table: Name of staging table
            schema: Database schema
        """
        if not delta.has_changes:
            logger.info("No changes detected - skipping load")
            return

        logger.info(f"Loading delta to {schema}.{staging_table}")

        full_table_name = f"{schema}.{staging_table}"

        # Load new records
        if len(delta.new_records) > 0:
            logger.info(f"Inserting {len(delta.new_records)} new records")

            # Add metadata columns
            new_with_meta = delta.new_records.copy()
            new_with_meta['_load_timestamp'] = datetime.now()
            new_with_meta['_load_type'] = 'insert'
            new_with_meta['_is_deleted'] = False

            # Use pandas to_sql for simplicity
            # For large datasets, use COPY or execute_batch
            with self.pg_manager.get_connection() as conn:
                new_with_meta.to_sql(
                    staging_table,
                    conn,
                    schema=schema,
                    if_exists='append',
                    index=False,
                    method='multi'
                )

            logger.info(f"✓ Inserted {len(delta.new_records)} new records")

        # Update modified records
        if len(delta.modified_records) > 0:
            logger.info(f"Updating {len(delta.modified_records)} modified records")

            # Strategy: DELETE old version, INSERT new version
            # (Simpler than UPDATE for many columns)

            with self.pg_manager.get_connection() as conn:
                cursor = conn.cursor()

                # Delete old versions
                pk_col = self.primary_keys[0]  # Assuming single PK for simplicity
                modified_ids = delta.modified_records[pk_col].tolist()

                delete_query = f"DELETE FROM {full_table_name} WHERE {pk_col} = ANY(%s)"
                cursor.execute(delete_query, (modified_ids,))

                logger.info(f"Deleted {cursor.rowcount} old versions")

                # Insert new versions
                modified_with_meta = delta.modified_records.copy()
                modified_with_meta['_load_timestamp'] = datetime.now()
                modified_with_meta['_load_type'] = 'update'
                modified_with_meta['_is_deleted'] = False

                modified_with_meta.to_sql(
                    staging_table,
                    conn,
                    schema=schema,
                    if_exists='append',
                    index=False,
                    method='multi'
                )

                conn.commit()

            logger.info(f"✓ Updated {len(delta.modified_records)} modified records")

        # Mark deleted records
        if len(delta.deleted_record_ids) > 0:
            logger.info(f"Marking {len(delta.deleted_record_ids)} deleted records")

            with self.pg_manager.get_connection() as conn:
                cursor = conn.cursor()

                # Soft delete: mark as deleted
                pk_col = self.primary_keys[0]
                update_query = f"""
                    UPDATE {full_table_name}
                    SET _is_deleted = TRUE,
                        _load_timestamp = %s,
                        _load_type = 'delete'
                    WHERE {pk_col} = ANY(%s)
                """

                cursor.execute(update_query, (datetime.now(), list(delta.deleted_record_ids)))
                conn.commit()

            logger.info(f"✓ Marked {len(delta.deleted_record_ids)} records as deleted")

        logger.info(f"✓ Delta load complete for {self.entity_type}")


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================

def load_entity_incrementally(
    entity_type: str,
    current_csv: str,
    schema_columns: List[str],
    primary_keys: List[str],
    staging_table: str,
    separator: str = ',',
    schema: str = 'staging'
) -> DeltaResult:
    """
    Convenience function for incremental entity loading.

    Usage:
        delta = load_entity_incrementally(
            entity_type='communications',
            current_csv='data/communications_2024_11_21.csv',
            schema_columns=['Record ID', 'Subject', ...],
            primary_keys=['Record ID'],
            staging_table='communications_staging'
        )
    """
    loader = DeltaLoader(
        entity_type=entity_type,
        primary_keys=primary_keys
    )

    delta = loader.detect_changes(
        current_csv=current_csv,
        schema_columns=schema_columns,
        separator=separator
    )

    logger.info(f"Delta summary: {delta.summary()}")

    if delta.has_changes:
        loader.load_delta_to_staging(
            delta=delta,
            staging_table=staging_table,
            schema=schema
        )

    return delta


# =============================================================================
# EXAMPLE USAGE
# =============================================================================

if __name__ == "__main__":
    # Example: Load communications incrementally

    communications_schema = [
        'Record ID', 'Communication Subject', 'communication type',
        'icAlps_company ID', 'Communication_CreateDate', 'Communication Notes',
        'Communication_From', 'Communication_To', 'Associated Contact',
        'Communication_PersonID', 'Company_Com', 'Associated Company',
        'Communication_Record ID', 'Associated Contact IDs',
        'Company_Com IDs', 'Associated Company IDs'
    ]

    delta = load_entity_incrementally(
        entity_type='communications',
        current_csv='data/communications_2024_11_21.csv',
        schema_columns=communications_schema,
        primary_keys=['Communication_Record ID'],
        staging_table='communications_staging'
    )

    print("\nDelta Summary:")
    print(json.dumps(delta.summary(), indent=2))
