"""
Pipeline Async
==============

Modular extraction pipeline with async task execution.

This package provides:
- Entity configuration system (properties + SQL queries)
- Generic extractors (reusable across entities)
- Async tasks (single and multi-entity extraction)
- Model integration (Memory/SQL/Chain storage)
- Connection parameter reuse

Usage:
    from pipeline_async import extract_entity, extract_multiple_entities
    from pipeline_async.model.memory import MemoryModel
    from config import get_connection_string

    # Extract single entity
    model = MemoryModel()
    await extract_entity('Case', get_connection_string(), model, limit=100)

    # Extract multiple entities
    await extract_multiple_entities(
        ['Company', 'Person', 'Case'],
        get_connection_string(),
        model
    )
"""

from .entity_config import (
    EntityConfig,
    get_entity_config,
    list_entities,
    ENTITY_CONFIGS
)

from .generic_extractor import GenericExtractor

from .extraction_task import (
    ExtractionTask,
    MultiEntityExtractionTask
)

# Convenience functions
async def extract_entity(
    entity_name: str,
    connection_string: str,
    model,
    dataclass_type=None,
    filter_clause=None,
    limit=None,
    save_bronze=True,
    bronze_layer_path="bronze_layer"
):
    """
    Convenience function to extract a single entity.

    Args:
        entity_name: Name of entity ('Case', 'Company', etc.)
        connection_string: SQL Server connection string
        model: Model to add data to (Memory/SQL/Chain)
        dataclass_type: Dataclass type to convert to (optional)
        filter_clause: Additional WHERE clause filter
        limit: Limit number of rows
        save_bronze: Whether to save to Bronze layer
        bronze_layer_path: Base directory for Bronze layer

    Returns:
        ExtractionTask instance with results

    Example:
        >>> from pipeline_async import extract_entity
        >>> from pipeline_async.model.memory import MemoryModel
        >>> model = MemoryModel()
        >>> task = await extract_entity('Case', conn_str, model, limit=100)
        >>> print(task.get_stats())
    """
    config = get_entity_config(entity_name)
    task = ExtractionTask(
        entity_config=config,
        connection_string=connection_string,
        model=model,
        dataclass_type=dataclass_type,
        filter_clause=filter_clause,
        limit=limit,
        save_bronze=save_bronze,
        bronze_layer_path=bronze_layer_path
    )
    await task.run(progress=True)
    return task


async def extract_multiple_entities(
    entity_names,
    connection_string: str,
    model,
    dataclass_types=None,
    filter_clauses=None,
    limits=None,
    save_bronze=True,
    bronze_layer_path="bronze_layer",
    parallel=False
):
    """
    Convenience function to extract multiple entities.

    Args:
        entity_names: List of entity names to extract
        connection_string: SQL Server connection string
        model: Model to add data to (Memory/SQL/Chain)
        dataclass_types: Dict mapping entity name to dataclass type
        filter_clauses: Dict mapping entity name to filter clause
        limits: Dict mapping entity name to row limit
        save_bronze: Whether to save to Bronze layer
        bronze_layer_path: Base directory for Bronze layer
        parallel: Run extractions in parallel

    Returns:
        MultiEntityExtractionTask instance with results

    Example:
        >>> from pipeline_async import extract_multiple_entities
        >>> from pipeline_async.model.memory import MemoryModel
        >>> model = MemoryModel()
        >>> task = await extract_multiple_entities(
        ...     ['Company', 'Person', 'Case'],
        ...     conn_str,
        ...     model,
        ...     limits={'Company': 100, 'Person': 100, 'Case': 100}
        ... )
        >>> print(task.get_stats())
    """
    task = MultiEntityExtractionTask(
        entity_names=entity_names,
        connection_string=connection_string,
        model=model,
        dataclass_types=dataclass_types,
        filter_clauses=filter_clauses,
        limits=limits,
        save_bronze=save_bronze,
        bronze_layer_path=bronze_layer_path,
        parallel=parallel
    )
    await task.run(progress=True)
    return task


__all__ = [
    'EntityConfig',
    'get_entity_config',
    'list_entities',
    'ENTITY_CONFIGS',
    'GenericExtractor',
    'ExtractionTask',
    'MultiEntityExtractionTask',
    'extract_entity',
    'extract_multiple_entities'
]
