"""Data mappers for converting domain objects to search documents.

This module provides mappers for converting various domain objects
(data products, datasets, curves, scenarios, etc.) into SearchDocument
objects that can be indexed in Elasticsearch.
"""

import logging
from typing import List, Optional, Dict, Any, Union
from datetime import datetime

from .elasticsearch_engine import SearchDocument
from aurum.core import AurumSettings


logger = logging.getLogger(__name__)


class BaseMapper:
    """Base class for search document mappers."""

    def __init__(self, settings: Optional[AurumSettings] = None):
        """Initialize mapper.

        Args:
            settings: Application settings. If None, uses global settings.
        """
        self.settings = settings

    def map_to_search_document(self, source_obj: Any, **kwargs) -> SearchDocument:
        """Map source object to SearchDocument.

        Args:
            source_obj: Source domain object
            **kwargs: Additional mapping parameters

        Returns:
            SearchDocument ready for indexing
        """
        raise NotImplementedError("Subclasses must implement map_to_search_document")


class DataProductMapper(BaseMapper):
    """Mapper for data products from the data mesh catalog."""

    def map_to_search_document(self, data_product: Any, **kwargs) -> SearchDocument:
        """Map data product to SearchDocument.

        Args:
            data_product: Data product object with attributes like id, name, description, etc.
            **kwargs: Additional parameters

        Returns:
            SearchDocument for indexing
        """
        # Extract attributes based on the data product structure
        product_id = getattr(data_product, 'id', str(id(data_product)))
        name = getattr(data_product, 'name', '')
        description = getattr(data_product, 'description', '')
        domains = getattr(data_product, 'domains', [])
        tags = getattr(data_product, 'tags', [])
        quality_score = getattr(data_product, 'quality_score', None)
        owner_team = getattr(data_product, 'owner_team', '')
        created_at = getattr(data_product, 'created_at', None)
        updated_at = getattr(data_product, 'updated_at', None)

        # Build content text for search
        content_parts = []
        if name:
            content_parts.append(f"Name: {name}")
        if description:
            content_parts.append(f"Description: {description}")
        if owner_team:
            content_parts.append(f"Owner: {owner_team}")
        if domains:
            content_parts.append(f"Domains: {', '.join(domains)}")

        content_text = " ".join(content_parts)

        return SearchDocument(
            id=product_id,
            doc_type="data_product",
            tenant_id=kwargs.get('tenant_id', 'default'),
            title=name or f"Data Product {product_id}",
            name=name,
            description=description,
            content_text=content_text,
            tags=tags,
            domains=domains,
            created_at=created_at,
            updated_at=updated_at,
            quality_score=quality_score,
            popularity_score=getattr(data_product, 'popularity_score', 0.5),
            metadata={
                'owner_team': owner_team,
                'product_type': getattr(data_product, 'product_type', 'unknown'),
                'status': getattr(data_product, 'status', 'active'),
                'source_system': 'data_mesh_catalog'
            }
        )


class DatasetMapper(BaseMapper):
    """Mapper for datasets (EIA, ISO, FRED, etc.)."""

    def map_to_search_document(self, dataset: Any, **kwargs) -> SearchDocument:
        """Map dataset to SearchDocument.

        Args:
            dataset: Dataset object with metadata
            **kwargs: Additional parameters

        Returns:
            SearchDocument for indexing
        """
        dataset_id = getattr(dataset, 'id', str(id(dataset)))
        name = getattr(dataset, 'name', '')
        description = getattr(dataset, 'description', '')
        source = getattr(dataset, 'source', 'unknown')
        category = getattr(dataset, 'category', '')
        tags = getattr(dataset, 'tags', [])
        asset_class = getattr(dataset, 'asset_class', '')
        iso = getattr(dataset, 'iso', '')
        location = getattr(dataset, 'location', '')
        frequency = getattr(dataset, 'frequency', '')
        created_at = getattr(dataset, 'created_at', None)
        updated_at = getattr(dataset, 'updated_at', None)

        # Build title and content
        title_parts = []
        if name:
            title_parts.append(name)
        if category:
            title_parts.append(f"({category})")

        title = " ".join(title_parts) or f"Dataset {dataset_id}"

        content_parts = []
        if description:
            content_parts.append(description)
        if asset_class:
            content_parts.append(f"Asset Class: {asset_class}")
        if iso:
            content_parts.append(f"ISO: {iso}")
        if location:
            content_parts.append(f"Location: {location}")
        if frequency:
            content_parts.append(f"Frequency: {frequency}")

        content_text = " ".join(content_parts)

        return SearchDocument(
            id=dataset_id,
            doc_type="dataset",
            tenant_id=kwargs.get('tenant_id', 'default'),
            title=title,
            name=name,
            description=description,
            content_text=content_text,
            tags=tags + [asset_class, iso, source],
            domains=[source, asset_class],
            created_at=created_at,
            updated_at=updated_at,
            quality_score=getattr(dataset, 'quality_score', 0.7),
            popularity_score=getattr(dataset, 'popularity_score', 0.3),
            metadata={
                'source': source,
                'category': category,
                'asset_class': asset_class,
                'iso': iso,
                'location': location,
                'frequency': frequency,
                'units': getattr(dataset, 'units', ''),
                'data_points': getattr(dataset, 'data_points', 0)
            }
        )


class CurveMapper(BaseMapper):
    """Mapper for energy trading curves."""

    def map_to_search_document(self, curve: Any, **kwargs) -> SearchDocument:
        """Map curve to SearchDocument.

        Args:
            curve: Curve object with metadata
            **kwargs: Additional parameters

        Returns:
            SearchDocument for indexing
        """
        curve_id = getattr(curve, 'id', str(id(curve)))
        curve_key = getattr(curve, 'curve_key', '')
        name = getattr(curve, 'name', curve_key)
        description = getattr(curve, 'description', '')
        asset_class = getattr(curve, 'asset_class', '')
        iso = getattr(curve, 'iso', '')
        location = getattr(curve, 'location', '')
        tags = getattr(curve, 'tags', [])
        created_at = getattr(curve, 'created_at', None)
        updated_at = getattr(curve, 'updated_at', None)

        # Build title
        title_parts = []
        if name:
            title_parts.append(name)
        if asset_class:
            title_parts.append(f"({asset_class})")

        title = " ".join(title_parts) or f"Curve {curve_id}"

        # Build content
        content_parts = []
        if description:
            content_parts.append(description)
        if asset_class:
            content_parts.append(f"Asset Class: {asset_class}")
        if iso:
            content_parts.append(f"ISO: {iso}")
        if location:
            content_parts.append(f"Location: {location}")

        content_text = " ".join(content_parts)

        return SearchDocument(
            id=curve_id,
            doc_type="curve",
            tenant_id=kwargs.get('tenant_id', 'default'),
            title=title,
            name=name,
            description=description,
            content_text=content_text,
            tags=tags + [asset_class, iso, curve_key],
            domains=[asset_class, iso],
            created_at=created_at,
            updated_at=updated_at,
            quality_score=getattr(curve, 'quality_score', 0.8),
            popularity_score=getattr(curve, 'popularity_score', 0.4),
            metadata={
                'curve_key': curve_key,
                'asset_class': asset_class,
                'iso': iso,
                'location': location,
                'source_system': 'curves',
                'data_points': getattr(curve, 'data_points', 0)
            }
        )


class ScenarioMapper(BaseMapper):
    """Mapper for trading scenarios."""

    def map_to_search_document(self, scenario: Any, **kwargs) -> SearchDocument:
        """Map scenario to SearchDocument.

        Args:
            scenario: Scenario object with metadata
            **kwargs: Additional parameters

        Returns:
            SearchDocument for indexing
        """
        scenario_id = getattr(scenario, 'id', str(id(scenario)))
        name = getattr(scenario, 'name', '')
        description = getattr(scenario, 'description', '')
        status = getattr(scenario, 'status', 'unknown')
        tags = getattr(scenario, 'tags', [])
        created_at = getattr(scenario, 'created_at', None)
        updated_at = getattr(scenario, 'updated_at', None)

        # Build title
        title = name or f"Scenario {scenario_id}"

        # Build content
        content_parts = []
        if description:
            content_parts.append(description)
        if status:
            content_parts.append(f"Status: {status}")

        content_text = " ".join(content_parts)

        return SearchDocument(
            id=scenario_id,
            doc_type="scenario",
            tenant_id=kwargs.get('tenant_id', 'default'),
            title=title,
            name=name,
            description=description,
            content_text=content_text,
            tags=tags + [status],
            domains=['scenarios'],
            created_at=created_at,
            updated_at=updated_at,
            quality_score=getattr(scenario, 'quality_score', 0.6),
            popularity_score=getattr(scenario, 'popularity_score', 0.2),
            metadata={
                'status': status,
                'source_system': 'scenarios',
                'run_count': getattr(scenario, 'run_count', 0),
                'last_run': getattr(scenario, 'last_run', None)
            }
        )


class DocumentationMapper(BaseMapper):
    """Mapper for documentation files."""

    def map_to_search_document(self, doc_info: Dict[str, Any], **kwargs) -> SearchDocument:
        """Map documentation to SearchDocument.

        Args:
            doc_info: Dict with doc metadata (path, title, content, etc.)
            **kwargs: Additional parameters

        Returns:
            SearchDocument for indexing
        """
        doc_id = doc_info.get('id', doc_info.get('path', str(id(doc_info))))
        title = doc_info.get('title', '')
        content = doc_info.get('content', '')
        path = doc_info.get('path', '')
        tags = doc_info.get('tags', ['documentation'])
        created_at = doc_info.get('created_at')
        updated_at = doc_info.get('updated_at')

        # Extract headings and sections for better search
        content_snippets = []
        if title:
            content_snippets.append(f"Title: {title}")
        if content:
            # Split content into sections for better indexing
            sections = self._extract_content_sections(content)
            content_snippets.extend(sections)

        content_text = " ".join(content_snippets)

        return SearchDocument(
            id=doc_id,
            doc_type="documentation",
            tenant_id=kwargs.get('tenant_id', 'default'),
            title=title or "Documentation",
            name=title,
            description=content[:500] + "..." if len(content) > 500 else content,
            content_text=content_text,
            tags=tags,
            domains=['documentation'],
            created_at=created_at,
            updated_at=updated_at,
            quality_score=0.9,  # Documentation typically has high quality
            popularity_score=0.1,
            metadata={
                'path': path,
                'source_system': 'documentation',
                'content_length': len(content),
                'sections': len(sections) if 'sections' in locals() else 0
            }
        )

    def _extract_content_sections(self, content: str) -> List[str]:
        """Extract meaningful sections from documentation content."""
        sections = []

        # Split by common section markers
        lines = content.split('\n')
        current_section = []

        for line in lines:
            line = line.strip()
            if not line:
                continue

            # Check if this looks like a section header
            if self._is_section_header(line):
                # Save previous section
                if current_section:
                    sections.append(" ".join(current_section))
                    current_section = []

            current_section.append(line)

        # Add final section
        if current_section:
            sections.append(" ".join(current_section))

        return sections[:10]  # Limit to prevent overly large documents

    def _is_section_header(self, line: str) -> bool:
        """Check if line looks like a section header."""
        # Headers typically start with # or are in ALL CAPS or end with :
        if line.startswith('#'):
            return True
        if line.isupper() and len(line) < 100:
            return True
        if line.endswith(':') and len(line) < 100:
            return True
        return False


class PluginMapper(BaseMapper):
    """Mapper for plugin marketplace items."""

    def map_to_search_document(self, plugin: Any, **kwargs) -> SearchDocument:
        """Map plugin to SearchDocument.

        Args:
            plugin: Plugin object with metadata
            **kwargs: Additional parameters

        Returns:
            SearchDocument for indexing
        """
        plugin_id = getattr(plugin, 'id', str(id(plugin)))
        name = getattr(plugin, 'name', '')
        description = getattr(plugin, 'description', '')
        category = getattr(plugin, 'category', '')
        tags = getattr(plugin, 'tags', [])
        author = getattr(plugin, 'author', '')
        version = getattr(plugin, 'version', '')
        created_at = getattr(plugin, 'created_at', None)
        updated_at = getattr(plugin, 'updated_at', None)

        # Build title
        title = name or f"Plugin {plugin_id}"

        # Build content
        content_parts = []
        if description:
            content_parts.append(description)
        if category:
            content_parts.append(f"Category: {category}")
        if author:
            content_parts.append(f"Author: {author}")
        if version:
            content_parts.append(f"Version: {version}")

        content_text = " ".join(content_parts)

        return SearchDocument(
            id=plugin_id,
            doc_type="plugin",
            tenant_id=kwargs.get('tenant_id', 'default'),
            title=title,
            name=name,
            description=description,
            content_text=content_text,
            tags=tags + [category],
            domains=['plugins', category],
            created_at=created_at,
            updated_at=updated_at,
            quality_score=getattr(plugin, 'quality_score', 0.7),
            popularity_score=getattr(plugin, 'popularity_score', 0.3),
            metadata={
                'category': category,
                'author': author,
                'version': version,
                'source_system': 'plugin_marketplace',
                'downloads': getattr(plugin, 'downloads', 0)
            }
        )


class SearchDocumentMapper:
    """Factory for creating appropriate mappers for different data types."""

    def __init__(self, settings: Optional[AurumSettings] = None):
        """Initialize document mapper factory.

        Args:
            settings: Application settings. If None, uses global settings.
        """
        self.settings = settings
        self.mappers = {
            'data_product': DataProductMapper(settings),
            'dataset': DatasetMapper(settings),
            'curve': CurveMapper(settings),
            'scenario': ScenarioMapper(settings),
            'documentation': DocumentationMapper(settings),
            'plugin': PluginMapper(settings),
        }

    def get_mapper(self, doc_type: str) -> BaseMapper:
        """Get mapper for document type.

        Args:
            doc_type: Type of document to map

        Returns:
            Appropriate mapper instance
        """
        return self.mappers.get(doc_type, BaseMapper(self.settings))

    def map_object(self, obj: Any, doc_type: str, **kwargs) -> SearchDocument:
        """Map object to SearchDocument using appropriate mapper.

        Args:
            obj: Object to map
            doc_type: Type of document
            **kwargs: Additional mapping parameters

        Returns:
            SearchDocument for indexing
        """
        mapper = self.get_mapper(doc_type)
        return mapper.map_to_search_document(obj, **kwargs)

    def map_objects(self, objects: List[Any], doc_type: str, **kwargs) -> List[SearchDocument]:
        """Map multiple objects to SearchDocuments.

        Args:
            objects: List of objects to map
            doc_type: Type of documents
            **kwargs: Additional mapping parameters

        Returns:
            List of SearchDocuments for indexing
        """
        mapper = self.get_mapper(doc_type)
        documents = []

        for obj in objects:
            try:
                doc = mapper.map_to_search_document(obj, **kwargs)
                documents.append(doc)
            except Exception as e:
                logger.warning(f"Failed to map object {obj}: {e}")
                continue

        return documents


# Global mapper factory
_document_mapper: Optional[SearchDocumentMapper] = None


def get_document_mapper(settings: Optional[AurumSettings] = None) -> SearchDocumentMapper:
    """Get or create global document mapper factory.

    Args:
        settings: Application settings. If None, uses global settings.

    Returns:
        Document mapper factory
    """
    global _document_mapper
    if _document_mapper is None:
        _document_mapper = SearchDocumentMapper(settings)
    return _document_mapper


def map_to_search_document(obj: Any, doc_type: str, **kwargs) -> SearchDocument:
    """Map object to SearchDocument using global mapper.

    Args:
        obj: Object to map
        doc_type: Type of document
        **kwargs: Additional mapping parameters

    Returns:
        SearchDocument for indexing
    """
    mapper = get_document_mapper()
    return mapper.map_object(obj, doc_type, **kwargs)


def map_objects_to_search_documents(objects: List[Any], doc_type: str, **kwargs) -> List[SearchDocument]:
    """Map objects to SearchDocuments using global mapper.

    Args:
        objects: Objects to map
        doc_type: Type of documents
        **kwargs: Additional mapping parameters

    Returns:
        List of SearchDocuments for indexing
    """
    mapper = get_document_mapper()
    return mapper.map_objects(objects, doc_type, **kwargs)
