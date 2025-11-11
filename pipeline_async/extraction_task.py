"""
Extraction Task
===============

Async Task implementation for entity extraction that integrates with the pipeline_async framework.

This module provides:
- ExtractionTask: Async task for extracting a single entity
- MultiEntityExtractionTask: Async task for extracting multiple entities
- Integration with Memory/SQL/Chain models

Usage:
    from extraction_task import ExtractionTask
    from entity_config import get_entity_config
    from pipeline_async.model.memory import MemoryModel
    from config import get_connection_string

    # Create task
    case_config = get_entity_config('Case')
    model = MemoryModel()
    task = ExtractionTask(
        entity_config=case_config,
        connection_string=get_connection_string(),
        model=model
    )

    # Run extraction
    await task.run()

    # Access extracted data from model
    cases = model.get_alldata(Case)
"""

import sys
from pathlib import Path
from typing import Optional, List, Type, Any
from dataclasses import is_dataclass
import asyncio

# Add paths
sys.path.append(str(Path(__file__).parent))

from loguru import logger

# Import from existing modules
from .entity_config import EntityConfig, get_entity_config
from .generic_extractor import GenericExtractor

# Import Task base (user provided)
from .task.base import Task
from .model.base import ModelBase


class ExtractionTask(Task):
    """
    Async task for extracting a single entity and adding to a model.

    This task:
    - Extracts entity data using GenericExtractor
    - Converts to dataclasses if dataclass_type provided
    - Adds data to model (Memory/SQL/Chain)
    - Saves to Bronze layer CSV
    - Reports progress
    """

    def __init__(
        self,
        entity_config: EntityConfig,
        connection_string: str,
        model: ModelBase,
        dataclass_type: Optional[Type] = None,
        filter_clause: Optional[str] = None,
        limit: Optional[int] = None,
        save_bronze: bool = True,
        bronze_layer_path: str = "bronze_layer"
    ):
        """
        Initialize extraction task.

        Args:
            entity_config: EntityConfig for the entity to extract
            connection_string: SQL Server connection string (reused)
            model: Model to add extracted data to (Memory/SQL/Chain)
            dataclass_type: Dataclass type to convert to (optional)
            filter_clause: Additional WHERE clause filter
            limit: Limit number of rows (for testing)
            save_bronze: Whether to save to Bronze layer CSV
            bronze_layer_path: Base directory for Bronze layer
        """
        super().__init__(model=model)
        self.entity_config = entity_config
        self.connection_string = connection_string
        self.dataclass_type = dataclass_type
        self.filter_clause = filter_clause
        self.limit = limit
        self.save_bronze = save_bronze
        self.bronze_layer_path = bronze_layer_path

        # Create extractor
        self.extractor = GenericExtractor(entity_config, connection_string)

        # State
        self.extracted_count = 0
        self.added_count = 0
        self.bronze_path = None

    @property
    def name(self) -> str:
        """Task name for logging"""
        return f"Extract {self.entity_config.name}"

    async def run(self, progress: bool = True) -> bool:
        """
        Execute the extraction task.

        Args:
            progress: Show progress messages

        Returns:
            True if extraction succeeded, False otherwise
        """
        if progress:
            logger.info(f"Starting extraction: {self.entity_config.name}")

        try:
            # Extract data
            if self.dataclass_type:
                # Extract to dataclasses
                instances = await asyncio.to_thread(
                    self.extractor.extract_to_dataclasses,
                    dataclass_type=self.dataclass_type,
                    filter_clause=self.filter_clause,
                    include_metadata=True,
                    limit=self.limit
                )
                self.extracted_count = len(instances)

                # Add to model
                for instance in instances:
                    added = self.model.add(instance, check_exists=True)
                    if added:
                        self.added_count += 1

                # Save to Bronze if requested
                if self.save_bronze:
                    self.bronze_path = await asyncio.to_thread(
                        self.extractor.save_to_bronze,
                        data=instances,
                        bronze_layer_path=self.bronze_layer_path
                    )

            else:
                # Extract to DataFrame
                df = await asyncio.to_thread(
                    self.extractor.extract_to_dataframe,
                    filter_clause=self.filter_clause,
                    include_metadata=True,
                    limit=self.limit
                )
                self.extracted_count = len(df)
                self.added_count = self.extracted_count  # No deduplication for DataFrames

                # Save to Bronze
                if self.save_bronze:
                    self.bronze_path = await asyncio.to_thread(
                        self.extractor.save_to_bronze,
                        data=df,
                        bronze_layer_path=self.bronze_layer_path
                    )

            if progress:
                logger.info(
                    f"✓ {self.entity_config.name}: "
                    f"Extracted {self.extracted_count}, "
                    f"Added {self.added_count} to model"
                )
                if self.bronze_path:
                    logger.info(f"  Saved to: {self.bronze_path}")

            return True

        except Exception as e:
            logger.error(f"✗ {self.entity_config.name} extraction failed: {str(e)}")
            return False

    def get_stats(self) -> dict:
        """Get extraction statistics"""
        return {
            'entity': self.entity_config.name,
            'extracted': self.extracted_count,
            'added_to_model': self.added_count,
            'duplicates_skipped': self.extracted_count - self.added_count,
            'bronze_path': self.bronze_path
        }


class MultiEntityExtractionTask(Task):
    """
    Async task for extracting multiple entities in sequence.

    This task:
    - Coordinates extraction of multiple entities
    - Maintains entity dependencies (extract Company before Person)
    - Aggregates statistics across all extractions
    - Can run extractions in parallel or sequence
    """

    def __init__(
        self,
        entity_names: List[str],
        connection_string: str,
        model: ModelBase,
        dataclass_types: Optional[dict] = None,
        filter_clauses: Optional[dict] = None,
        limits: Optional[dict] = None,
        save_bronze: bool = True,
        bronze_layer_path: str = "bronze_layer",
        parallel: bool = False
    ):
        """
        Initialize multi-entity extraction task.

        Args:
            entity_names: List of entity names to extract (e.g., ['Company', 'Person', 'Case'])
            connection_string: SQL Server connection string (reused)
            model: Model to add extracted data to (Memory/SQL/Chain)
            dataclass_types: Dict mapping entity name to dataclass type (optional)
            filter_clauses: Dict mapping entity name to filter clause (optional)
            limits: Dict mapping entity name to row limit (optional)
            save_bronze: Whether to save to Bronze layer CSV
            bronze_layer_path: Base directory for Bronze layer
            parallel: Run extractions in parallel (use with caution due to FK dependencies)
        """
        super().__init__(model=model)
        self.entity_names = entity_names
        self.connection_string = connection_string
        self.dataclass_types = dataclass_types or {}
        self.filter_clauses = filter_clauses or {}
        self.limits = limits or {}
        self.save_bronze = save_bronze
        self.bronze_layer_path = bronze_layer_path
        self.parallel = parallel

        # State
        self.tasks: List[ExtractionTask] = []
        self.results: dict = {}

    @property
    def name(self) -> str:
        """Task name for logging"""
        return f"Extract {len(self.entity_names)} entities"

    async def run(self, progress: bool = True) -> bool:
        """
        Execute multi-entity extraction.

        Args:
            progress: Show progress messages

        Returns:
            True if all extractions succeeded, False if any failed
        """
        if progress:
            logger.info(f"Starting multi-entity extraction: {', '.join(self.entity_names)}")

        # Create tasks
        self.tasks = []
        for entity_name in self.entity_names:
            config = get_entity_config(entity_name)
            task = ExtractionTask(
                entity_config=config,
                connection_string=self.connection_string,
                model=self.model,
                dataclass_type=self.dataclass_types.get(entity_name),
                filter_clause=self.filter_clauses.get(entity_name),
                limit=self.limits.get(entity_name),
                save_bronze=self.save_bronze,
                bronze_layer_path=self.bronze_layer_path
            )
            self.tasks.append(task)

        # Run tasks
        if self.parallel:
            # Run in parallel (use with caution - may violate FK constraints)
            results = await asyncio.gather(
                *[task.run(progress=progress) for task in self.tasks],
                return_exceptions=True
            )
            success = all(r is True for r in results)
        else:
            # Run sequentially (respects FK dependencies)
            success = True
            for task in self.tasks:
                result = await task.run(progress=progress)
                if not result:
                    success = False
                    if progress:
                        logger.warning(f"Continuing despite {task.entity_config.name} failure")

        # Collect statistics
        self.results = {
            task.entity_config.name: task.get_stats()
            for task in self.tasks
        }

        if progress:
            self._print_summary()

        return success

    def _print_summary(self):
        """Print extraction summary"""
        logger.info("\n" + "=" * 70)
        logger.info("Multi-Entity Extraction Summary")
        logger.info("=" * 70)

        total_extracted = 0
        total_added = 0

        for entity_name, stats in self.results.items():
            extracted = stats['extracted']
            added = stats['added_to_model']
            total_extracted += extracted
            total_added += added

            logger.info(f"\n{entity_name}:")
            logger.info(f"  Extracted: {extracted}")
            logger.info(f"  Added to model: {added}")
            logger.info(f"  Duplicates skipped: {stats['duplicates_skipped']}")
            if stats['bronze_path']:
                logger.info(f"  Bronze: {stats['bronze_path']}")

        logger.info("\n" + "-" * 70)
        logger.info(f"Total extracted: {total_extracted}")
        logger.info(f"Total added to model: {total_added}")
        logger.info("=" * 70 + "\n")

    def get_stats(self) -> dict:
        """Get aggregated statistics"""
        return {
            'entities': list(self.results.keys()),
            'total_extracted': sum(s['extracted'] for s in self.results.values()),
            'total_added': sum(s['added_to_model'] for s in self.results.values()),
            'individual_stats': self.results
        }


# ============================================================================
# EXAMPLE USAGE
# ============================================================================

async def example_single_entity():
    """Example: Extract single entity"""
    from pipeline_async.model.memory import MemoryModel
    import sys
    sys.path.append('..')
    from config import get_connection_string

    print("\n" + "=" * 70)
    print("Example 1: Extract Single Entity (Case)")
    print("=" * 70 + "\n")

    # Setup
    case_config = get_entity_config('Case')
    model = MemoryModel()
    connection_string = get_connection_string()

    # Create task
    task = ExtractionTask(
        entity_config=case_config,
        connection_string=connection_string,
        model=model,
        limit=10  # Test with 10 rows
    )

    # Run
    success = await task.run(progress=True)
    print(f"\nTask completed: {'✓ Success' if success else '✗ Failed'}")
    print(f"Stats: {task.get_stats()}")


async def example_multi_entity():
    """Example: Extract multiple entities"""
    from pipeline_async.model.memory import MemoryModel
    import sys
    sys.path.append('..')
    from config import get_connection_string

    print("\n" + "=" * 70)
    print("Example 2: Extract Multiple Entities")
    print("=" * 70 + "\n")

    # Setup
    model = MemoryModel()
    connection_string = get_connection_string()

    # Create task
    task = MultiEntityExtractionTask(
        entity_names=['Company', 'Person', 'Case'],
        connection_string=connection_string,
        model=model,
        limits={'Company': 10, 'Person': 10, 'Case': 10},  # Test limits
        parallel=False  # Sequential to respect FK dependencies
    )

    # Run
    success = await task.run(progress=True)
    print(f"\nAll tasks completed: {'✓ Success' if success else '✗ Failed'}")


if __name__ == "__main__":
    # Run examples
    asyncio.run(example_single_entity())
    asyncio.run(example_multi_entity())
