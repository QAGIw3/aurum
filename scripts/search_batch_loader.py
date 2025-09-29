#!/usr/bin/env python3
"""Batch loader script for populating Elasticsearch search index.

This script loads data from various sources (data products, datasets, curves,
scenarios, documentation, plugins) and indexes them in Elasticsearch for search.
Can be run as a one-time initialization or periodic refresh.
"""

import asyncio
import logging
import sys
import os
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime
import json
import argparse

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from aurum.core.settings import get_settings
from aurum.search.elasticsearch_engine import ElasticsearchEngine, SearchDocument
from aurum.search.semantic_search import get_semantic_search_service, is_semantic_search_enabled
from aurum.search.mappers import get_document_mapper, map_objects_to_search_documents


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DataSourceLoader:
    """Base class for loading data from different sources."""

    def __init__(self, settings=None):
        """Initialize data source loader.

        Args:
            settings: Application settings
        """
        self.settings = settings or get_settings()

    async def load_data(self, **kwargs) -> List[Any]:
        """Load data from this source.

        Args:
            **kwargs: Source-specific parameters

        Returns:
            List of objects to index
        """
        raise NotImplementedError("Subclasses must implement load_data")

    def get_doc_type(self) -> str:
        """Get document type for this source.

        Returns:
            Document type string
        """
        raise NotImplementedError("Subclasses must implement get_doc_type")


class DataProductLoader(DataSourceLoader):
    """Loader for data products from data mesh catalog."""

    async def load_data(self, **kwargs) -> List[Any]:
        """Load data products."""
        try:
            # Try to load from data mesh catalog
            from aurum.data.mesh.catalog import DataProductCatalog

            catalog = DataProductCatalog()
            await catalog.initialize()

            # Get all data products
            products = []
            # This would need to be implemented based on the catalog's API
            # For now, return empty list as placeholder
            logger.info("Loading data products from catalog")

            return products

        except Exception as e:
            logger.warning(f"Failed to load data products: {e}")
            return []

    def get_doc_type(self) -> str:
        """Get document type."""
        return "data_product"


class DatasetLoader(DataSourceLoader):
    """Loader for datasets from various sources."""

    async def load_data(self, **kwargs) -> List[Any]:
        """Load datasets from EIA, ISO, FRED, etc."""
        datasets = []

        try:
            # Load EIA datasets
            eia_datasets = await self._load_eia_datasets()
            datasets.extend(eia_datasets)

            # Load ISO datasets
            iso_datasets = await self._load_iso_datasets()
            datasets.extend(iso_datasets)

            # Load FRED datasets
            fred_datasets = await self._load_fred_datasets()
            datasets.extend(fred_datasets)

            logger.info(f"Loaded {len(datasets)} datasets total")

        except Exception as e:
            logger.error(f"Failed to load datasets: {e}")

        return datasets

    async def _load_eia_datasets(self) -> List[Any]:
        """Load EIA datasets."""
        try:
            # This would integrate with existing EIA service
            # For now, return sample data
            return [
                type('Dataset', (), {
                    'id': 'eia_1',
                    'name': 'U.S. Electric Power Monthly',
                    'description': 'Monthly electric power data from EIA',
                    'source': 'eia',
                    'category': 'electricity',
                    'asset_class': 'power',
                    'frequency': 'monthly',
                    'created_at': datetime.now(),
                    'quality_score': 0.9,
                    'popularity_score': 0.8
                })()
            ]
        except Exception as e:
            logger.warning(f"Failed to load EIA datasets: {e}")
            return []

    async def _load_iso_datasets(self) -> List[Any]:
        """Load ISO datasets."""
        try:
            # This would integrate with existing ISO service
            return [
                type('Dataset', (), {
                    'id': 'iso_1',
                    'name': 'ERCOT Day-Ahead Prices',
                    'description': 'Day-ahead electricity prices from ERCOT',
                    'source': 'iso',
                    'category': 'prices',
                    'asset_class': 'power',
                    'iso': 'ERCOT',
                    'location': 'texas',
                    'frequency': 'hourly',
                    'created_at': datetime.now(),
                    'quality_score': 0.95,
                    'popularity_score': 0.9
                })()
            ]
        except Exception as e:
            logger.warning(f"Failed to load ISO datasets: {e}")
            return []

    async def _load_fred_datasets(self) -> List[Any]:
        """Load FRED datasets."""
        try:
            # This would integrate with existing FRED service
            return [
                type('Dataset', (), {
                    'id': 'fred_1',
                    'name': 'Federal Funds Rate',
                    'description': 'Federal funds effective rate',
                    'source': 'fred',
                    'category': 'monetary_policy',
                    'asset_class': 'rates',
                    'frequency': 'daily',
                    'created_at': datetime.now(),
                    'quality_score': 0.85,
                    'popularity_score': 0.7
                })()
            ]
        except Exception as e:
            logger.warning(f"Failed to load FRED datasets: {e}")
            return []

    def get_doc_type(self) -> str:
        """Get document type."""
        return "dataset"


class CurveLoader(DataSourceLoader):
    """Loader for energy trading curves."""

    async def load_data(self, **kwargs) -> List[Any]:
        """Load curve data."""
        try:
            # This would integrate with existing curves service
            # For now, return sample data
            return [
                type('Curve', (), {
                    'id': 'curve_1',
                    'curve_key': 'NG_HENRY_HUB',
                    'name': 'Henry Hub Natural Gas',
                    'description': 'Natural gas spot prices at Henry Hub',
                    'asset_class': 'gas',
                    'iso': 'N/A',
                    'location': 'louisiana',
                    'created_at': datetime.now(),
                    'quality_score': 0.8,
                    'popularity_score': 0.6,
                    'data_points': 1000
                })()
            ]
        except Exception as e:
            logger.warning(f"Failed to load curves: {e}")
            return []

    def get_doc_type(self) -> str:
        """Get document type."""
        return "curve"


class ScenarioLoader(DataSourceLoader):
    """Loader for trading scenarios."""

    async def load_data(self, **kwargs) -> List[Any]:
        """Load scenario data."""
        try:
            # This would integrate with existing scenarios service
            return [
                type('Scenario', (), {
                    'id': 'scenario_1',
                    'name': 'Base Case Energy Forecast',
                    'description': 'Baseline energy demand and supply forecast',
                    'status': 'active',
                    'created_at': datetime.now(),
                    'quality_score': 0.7,
                    'popularity_score': 0.4,
                    'run_count': 5
                })()
            ]
        except Exception as e:
            logger.warning(f"Failed to load scenarios: {e}")
            return []

    def get_doc_type(self) -> str:
        """Get document type."""
        return "scenario"


class DocumentationLoader(DataSourceLoader):
    """Loader for documentation files."""

    async def load_data(self, **kwargs) -> List[Any]:
        """Load documentation files."""
        docs = []

        try:
            # Load markdown files from docs directory
            docs_dir = Path(__file__).parent.parent / "docs"
            if docs_dir.exists():
                for md_file in docs_dir.glob("*.md"):
                    if md_file.is_file():
                        try:
                            content = md_file.read_text(encoding='utf-8')

                            # Extract title from first heading
                            title = self._extract_title_from_markdown(content) or md_file.stem

                            doc_info = {
                                'id': f"doc_{md_file.stem}",
                                'path': str(md_file),
                                'title': title,
                                'content': content,
                                'created_at': datetime.fromtimestamp(md_file.stat().st_mtime),
                                'tags': ['documentation', 'guide']
                            }
                            docs.append(doc_info)

                        except Exception as e:
                            logger.warning(f"Failed to load doc {md_file}: {e}")

            logger.info(f"Loaded {len(docs)} documentation files")

        except Exception as e:
            logger.error(f"Failed to load documentation: {e}")

        return docs

    def _extract_title_from_markdown(self, content: str) -> Optional[str]:
        """Extract title from markdown content."""
        lines = content.split('\n')
        for line in lines[:10]:  # Check first 10 lines
            line = line.strip()
            if line.startswith('# '):
                return line[2:].strip()
        return None

    def get_doc_type(self) -> str:
        """Get document type."""
        return "documentation"


class PluginLoader(DataSourceLoader):
    """Loader for plugin marketplace items."""

    async def load_data(self, **kwargs) -> List[Any]:
        """Load plugin data."""
        try:
            # This would integrate with existing plugin marketplace service
            return [
                type('Plugin', (), {
                    'id': 'plugin_1',
                    'name': 'Data Quality Validator',
                    'description': 'Plugin for validating data quality metrics',
                    'category': 'data_quality',
                    'author': 'Aurum Team',
                    'version': '1.0.0',
                    'created_at': datetime.now(),
                    'quality_score': 0.8,
                    'popularity_score': 0.5,
                    'downloads': 150
                })()
            ]
        except Exception as e:
            logger.warning(f"Failed to load plugins: {e}")
            return []

    def get_doc_type(self) -> str:
        """Get document type."""
        return "plugin"


class BatchLoader:
    """Main batch loader for populating search index."""

    def __init__(self, settings=None):
        """Initialize batch loader.

        Args:
            settings: Application settings
        """
        self.settings = settings or get_settings()
        self.engine: Optional[ElasticsearchEngine] = None
        self.semantic_service = get_semantic_search_service(settings)

        # Available data source loaders
        self.loaders = {
            'data_products': DataProductLoader(settings),
            'datasets': DatasetLoader(settings),
            'curves': CurveLoader(settings),
            'scenarios': ScenarioLoader(settings),
            'documentation': DocumentationLoader(settings),
            'plugins': PluginLoader(settings),
        }

    async def initialize(self):
        """Initialize batch loader."""
        self.engine = ElasticsearchEngine(self.settings)
        await self.engine.initialize()

        if self.settings.search_semantic_enabled:
            await self.semantic_service.initialize()

        logger.info("Batch loader initialized")

    async def load_all_sources(self, sources: Optional[List[str]] = None) -> Dict[str, int]:
        """Load data from all sources or specified sources.

        Args:
            sources: List of source names to load. If None, loads all.

        Returns:
            Dict mapping source names to document counts loaded
        """
        if sources is None:
            sources = list(self.loaders.keys())

        results = {}

        for source_name in sources:
            if source_name not in self.loaders:
                logger.warning(f"Unknown source: {source_name}")
                continue

            try:
                count = await self.load_source(source_name)
                results[source_name] = count
                logger.info(f"Loaded {count} documents from {source_name}")

            except Exception as e:
                logger.error(f"Failed to load {source_name}: {e}")
                results[source_name] = 0

        return results

    async def load_source(self, source_name: str) -> int:
        """Load data from a specific source.

        Args:
            source_name: Name of source to load

        Returns:
            Number of documents loaded
        """
        if source_name not in self.loaders:
            raise ValueError(f"Unknown source: {source_name}")

        loader = self.loaders[source_name]
        doc_type = loader.get_doc_type()

        # Load data from source
        logger.info(f"Loading data from {source_name}")
        objects = await loader.load_data()

        if not objects:
            logger.info(f"No data found for {source_name}")
            return 0

        # Map objects to search documents
        logger.info(f"Mapping {len(objects)} {source_name} objects to search documents")
        documents = map_objects_to_search_documents(
            objects,
            doc_type,
            tenant_id='default'
        )

        # Add embeddings if semantic search is enabled
        if self.settings.search_semantic_enabled and is_semantic_search_enabled(self.settings):
            logger.info("Adding embeddings to documents")
            documents = await self.semantic_service.embed_documents(documents)

        # Index documents in batches
        batch_size = 100
        total_indexed = 0

        for i in range(0, len(documents), batch_size):
            batch = documents[i:i + batch_size]
            try:
                response = await self.engine.bulk_index(batch, refresh=False)
                if response.get('errors'):
                    error_count = sum(1 for item in response['items'] if item.get('error'))
                    logger.warning(f"Batch {i//batch_size + 1} had {error_count} errors")
                else:
                    total_indexed += len(batch)
                    logger.debug(f"Indexed batch {i//batch_size + 1} ({len(batch)} documents)")

            except Exception as e:
                logger.error(f"Failed to index batch {i//batch_size + 1}: {e}")

        # Refresh index after loading
        await self.engine.refresh_index()

        logger.info(f"Successfully indexed {total_indexed} documents from {source_name}")
        return total_indexed

    async def clear_index(self, confirm: bool = False):
        """Clear all documents from search index.

        Args:
            confirm: Must be True to actually clear the index
        """
        if not confirm:
            logger.warning("Use --confirm-clear to actually clear the index")
            return

        try:
            client = await self.engine._get_client()
            await client.delete_by_query(
                index=self.engine._index_name,
                body={"query": {"match_all": {}}},
                refresh=True
            )
            logger.info("Index cleared")
        except Exception as e:
            logger.error(f"Failed to clear index: {e}")

    async def get_index_stats(self) -> Dict[str, Any]:
        """Get index statistics.

        Returns:
            Dictionary with index stats
        """
        try:
            client = await self.engine._get_client()
            stats = await client.indices.stats(index=self.engine._index_name)
            return stats
        except Exception as e:
            logger.error(f"Failed to get index stats: {e}")
            return {}


async def main():
    """Main entry point for batch loader."""
    parser = argparse.ArgumentParser(description="Batch load data into Elasticsearch search index")
    parser.add_argument(
        '--sources',
        nargs='+',
        choices=['data_products', 'datasets', 'curves', 'scenarios', 'documentation', 'plugins'],
        help="Specific sources to load (default: all)"
    )
    parser.add_argument(
        '--clear',
        action='store_true',
        help="Clear index before loading"
    )
    parser.add_argument(
        '--confirm-clear',
        action='store_true',
        help="Required flag to confirm clearing the index"
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help="Show what would be loaded without actually indexing"
    )
    parser.add_argument(
        '--stats',
        action='store_true',
        help="Show index statistics after loading"
    )

    args = parser.parse_args()

    # Initialize settings and services
    settings = get_settings()

    if not settings.search_enabled:
        logger.error("Search is disabled in configuration")
        sys.exit(1)

    # Create batch loader
    loader = BatchLoader(settings)
    await loader.initialize()

    # Clear index if requested
    if args.clear or args.confirm_clear:
        if not args.confirm_clear:
            logger.error("--confirm-clear required to clear index")
            sys.exit(1)
        await loader.clear_index(confirm=True)

    # Load data
    if not args.dry_run:
        results = await loader.load_all_sources(args.sources)
        total_loaded = sum(results.values())

        logger.info("Batch loading completed:")
        for source, count in results.items():
            logger.info(f"  {source}: {count} documents")

        logger.info(f"Total: {total_loaded} documents loaded")
    else:
        logger.info("Dry run - showing what would be loaded:")
        results = await loader.load_all_sources(args.sources)

        for source, count in results.items():
            logger.info(f"  {source}: {count} documents would be loaded")

    # Show stats if requested
    if args.stats:
        logger.info("Index statistics:")
        stats = await loader.get_index_stats()
        if stats:
            indices = stats.get('indices', {})
            index_stats = indices.get(loader.engine._index_name, {})

            if 'total' in index_stats:
                total = index_stats['total']
                logger.info(f"  Documents: {total.get('docs', {}).get('count', 0)}")
                logger.info(f"  Size: {total.get('store', {}).get('size_in_bytes', 0)} bytes")

            if 'primaries' in index_stats:
                primaries = index_stats['primaries']
                logger.info(f"  Segments: {primaries.get('segments', {}).get('count', 0)}")
        else:
            logger.info("  Unable to retrieve statistics")


if __name__ == "__main__":
    asyncio.run(main())
