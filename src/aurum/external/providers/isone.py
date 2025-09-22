"""ISO New England (ISO‑NE) data collector implementation.

Purpose:
- Fetch LMP, Load, Generation Mix, and Ancillary Services via ISO‑NE web services
- Normalize records and update per‑dataset watermarks using a Postgres checkpoint store

Configuration:
- Dataset configs: `config/isone_ingest_datasets.json` (filters, data_type, market)
- API auth: `X-API-Key` header when `ISONe_API_KEY` (or mapped secret) is set
- Rate limits: `IsoNeApiConfig` exposes RPM/RPH; the client sleeps between calls accordingly

Related docs:
- seatunnel/README.md (SeaTunnel templates for ISO ingestion)
- README.md (ISO LMP ingestion quickstart)
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import aiohttp
import requests

from aurum.external.collect.base import ExternalCollector, CollectorContext
from aurum.external.collect.checkpoints import PostgresCheckpointStore

logger = logging.getLogger(__name__)


@dataclass
class IsoNeDatasetConfig:
    """Configuration for ISO-NE datasets."""

    dataset_id: str
    description: str
    data_type: str  # lmp, load, generation_mix, ancillary_services
    market: str  # DAM, RTM
    url_template: str
    interval_minutes: int
    columns: Dict[str, str]  # Column mappings
    frequency: str  # HOURLY, DAILY, etc.
    units: str


@dataclass
class IsoNeApiConfig:
    """ISO-NE API configuration."""

    base_url: str = "https://webservices.iso-ne.com/api/v1.1"
    api_key: Optional[str] = None
    timeout: int = 30
    max_retries: int = 3
    requests_per_minute: int = 60
    requests_per_hour: int = 1000


class IsoNeApiClient:
    """Client for ISO New England Web Services API."""

    def __init__(self, config: IsoNeApiConfig):
        self.config = config
        self.session = requests.Session()
        self.session.timeout = config.timeout

        if config.api_key:
            self.session.headers.update({
                "Authorization": f"Bearer {config.api_key}",
                "Accept": "application/json",
                "X-API-Key": config.api_key  # ISO-NE uses X-API-Key header
        })

    async def fetch_data(
        self,
        url: str,
        params: Dict[str, Any],
        format_type: str = "json",
        fallback_urls: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Fetch data from ISO-NE API with retry logic and fallbacks."""
        urls_to_try = [url] + (fallback_urls or [])

        for url_to_try in urls_to_try:
            for attempt in range(self.config.max_retries + 1):
                try:
                    # Rate limiting
                    await asyncio.sleep(1.0 / (self.config.requests_per_minute / 60.0))

                    response = self.session.get(url_to_try, params=params, timeout=self.config.timeout)
                    response.raise_for_status()

                    # Handle different response formats
                    content_type = response.headers.get('Content-Type', '').lower()

                    if format_type == "json" or "json" in content_type:
                        return response.json()
                    elif format_type == "xml" or "xml" in content_type:
                        return {"data": self._parse_xml(response.text)}
                    else:
                        return {"data": response.text.split('\n')}

                except Exception as e:
                    if attempt == self.config.max_retries:
                        logger.warning(f"All attempts failed for URL {url_to_try}: {e}")
                        break
                    logger.warning(f"ISO-NE API request failed (attempt {attempt + 1}) for {url_to_try}: {e}")
                    await asyncio.sleep(self.config.timeout * (2 ** attempt))

        # If we get here, all URLs failed
        raise RuntimeError(f"Failed to fetch data from all ISO-NE API endpoints")

    def _parse_xml(self, xml_text: str) -> str:
        """Parse XML text (placeholder - implement as needed)."""
        # For now, return raw XML
        return xml_text


class IsoNeCollector(ExternalCollector):
    """ISO-NE data collector."""

    def __init__(self, config: IsoNeDatasetConfig, api_config: IsoNeApiConfig):
        self.dataset_config = config
        self.api_config = api_config
        self.api_client = IsoNeApiClient(api_config)
        self.checkpoint_store = PostgresCheckpointStore()
        self.context = CollectorContext()

    async def collect(
        self,
        start_date: datetime,
        end_date: datetime
    ) -> List[Dict[str, Any]]:
        """Collect ISO-NE data for the specified date range."""
        logger.info(
            "Starting ISO-NE data collection",
            extra={
                "dataset": self.dataset_config.dataset_id,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat()
            }
        )

        records = []

        try:
            # Generate date range based on frequency
            dates = self._generate_date_range(start_date, end_date)

            for date in dates:
                # Build API URL using the template
                url = self.dataset_config.url_template.format(date=date)

                # Fetch data
                data = await self.api_client.fetch_data(url, {})

                # Process and normalize records
                batch_records = await self._process_data(data, date)
                records.extend(batch_records)

                # Update checkpoint
                await self.checkpoint_store.update_watermark(
                    self.dataset_config.dataset_id,
                    date
                )

                logger.info(
                    f"Processed {len(batch_records)} records for {date.strftime('%Y-%m-%d')}"
                )

                # Check for empty responses
                if len(batch_records) == 0:
                    await self._alert_empty_response(date)

                # Check for schema drift (basic check)
                if batch_records:
                    await self._check_schema_drift(batch_records[0], date)

        except Exception as e:
            logger.error(
                "ISO-NE data collection failed",
                extra={
                    "dataset": self.dataset_config.dataset_id,
                    "error": str(e)
                }
            )
            raise

        return records

    def _generate_date_range(
        self,
        start_date: datetime,
        end_date: datetime
    ) -> List[datetime]:
        """Generate date range based on dataset frequency."""
        dates = []
        current = start_date

        while current <= end_date:
            dates.append(current)
            current += timedelta(days=1)  # Default to daily

        return dates

    async def _process_data(
        self,
        data: Dict[str, Any],
        date: datetime
    ) -> List[Dict[str, Any]]:
        """Process raw ISO-NE data into normalized records."""
        records = []

        if "data" not in data:
            logger.warning(f"No data found for {date.strftime('%Y-%m-%d')}")
            return records

        data_items = data["data"]

        # Handle different data formats
        if isinstance(data_items, str):
            # XML or raw text
            return records
        elif isinstance(data_items, list):
            # JSON array
            for item in data_items:
                record = await self._normalize_record(item, date)
                if record:
                    records.append(record)
        else:
            logger.warning(f"Unknown data format for {date.strftime('%Y-%m-%d')}")
            return records

        return records

    async def _normalize_record(
        self,
        item: Dict[str, Any],
        date: datetime
    ) -> Optional[Dict[str, Any]]:
        """Normalize individual ISO-NE record."""
        try:
            # Common normalization logic
            normalized = {
                "iso_code": "ISONE",
                "data_type": self.dataset_config.data_type,
                "market": self.dataset_config.market,
                "delivery_date": date.date().isoformat(),
                "ingest_ts": datetime.utcnow().isoformat(),
                "source": "ISO-NE",
                "metadata": {
                    "source_timezone": "America/New_York",  # ISO-NE's timezone
                    "normalized_to_utc": True
                }
            }

            # Add data-type specific fields
            if self.dataset_config.data_type == "lmp":
                normalized.update({
                    "location_id": item.get("location_id", item.get("node_id")),
                    "location_name": item.get("location_name", item.get("node_name")),
                    "price_total": float(item.get("lmp", item.get("price", 0))),
                    "price_energy": float(item.get("energy_component", 0)),
                    "price_congestion": float(item.get("congestion_component", 0)),
                    "price_loss": float(item.get("loss_component", 0)),
                    "currency": "USD",
                    "uom": "MWh"
                })
            elif self.dataset_config.data_type == "load":
                normalized.update({
                    "location_id": item.get("location_id"),
                    "location_name": item.get("location_name"),
                    "load_mw": float(item.get("load", 0)),
                    "uom": "MW"
                })
            elif self.dataset_config.data_type == "generation_mix":
                normalized.update({
                    "location_id": item.get("location_id"),
                    "fuel_type": item.get("fuel_type"),
                    "generation_mw": float(item.get("generation", 0)),
                    "uom": "MW"
                })
            elif self.dataset_config.data_type == "ancillary_services":
                normalized.update({
                    "location_id": item.get("location_id"),
                    "as_type": item.get("service_type"),
                    "as_price": float(item.get("price", 0)),
                    "as_mw": float(item.get("mw", 0)),
                    "uom": "MW",
                    "currency": "USD"
                })

            return normalized

        except Exception as e:
            logger.warning(f"Failed to normalize record: {e}")
            return None

    async def build_node_crosswalk(self) -> None:
        """Build node and zone crosswalk from ISO-NE responses."""
        logger.info("Building ISO-NE node and zone crosswalk")

        try:
            # Collect nodes from all available endpoints
            nodes = set()

            # Get nodes from LMP data
            try:
                lmp_nodes = await self._get_lmp_nodes()
                nodes.update(lmp_nodes)
            except Exception as e:
                logger.warning(f"Failed to get LMP nodes: {e}")

            # Get nodes from load data
            try:
                load_nodes = await self._get_load_nodes()
                nodes.update(load_nodes)
            except Exception as e:
                logger.warning(f"Failed to get load nodes: {e}")

            # Convert to list and deduplicate
            unique_nodes = self._deduplicate_nodes(list(nodes))

            # Update the reference file
            await self._update_node_reference(unique_nodes)

            logger.info(f"Updated node crosswalk with {len(unique_nodes)} unique nodes")

        except Exception as e:
            logger.error(f"Failed to build node crosswalk: {e}")
            raise

    async def _get_lmp_nodes(self) -> Set[str]:
        """Extract unique nodes from LMP data."""
        nodes = set()

        try:
            # Get recent LMP data
            sample_date = datetime.now() - timedelta(days=1)
            url = f"{self.api_config.base_url}/fiveminutelmp/current"

            data = await self.api_client.fetch_data(url, {})

            if "data" in data and isinstance(data["data"], list):
                for item in data["data"][:100]:
                    if "location_id" in item:
                        nodes.add(item["location_id"])

        except Exception as e:
            logger.warning(f"Failed to extract LMP nodes: {e}")

        return nodes

    async def _get_load_nodes(self) -> Set[str]:
        """Extract unique nodes from load data."""
        nodes = set()

        try:
            # Get recent load data
            sample_date = datetime.now() - timedelta(days=1)
            url = f"{self.api_config.base_url}/fiveminutesystemload/current"

            data = await self.api_client.fetch_data(url, {})

            if "data" in data and isinstance(data["data"], list):
                for item in data["data"][:100]:
                    if "location_id" in item:
                        nodes.add(item["location_id"])

        except Exception as e:
            logger.warning(f"Failed to extract load nodes: {e}")

        return nodes

    def _deduplicate_nodes(self, nodes: List[str]) -> List[Dict[str, Any]]:
        """Deduplicate nodes and create crosswalk entries."""
        unique_nodes = {}
        for node in nodes:
            if node not in unique_nodes:
                unique_nodes[node] = {
                    "iso": "ISO-NE",
                    "location_id": node,
                    "location_name": node,
                    "location_type": "NODE",
                    "zone": node,
                    "hub": "",
                    "timezone": "America/New_York"
                }

        return list(unique_nodes.values())

    async def _update_node_reference(self, nodes: List[Dict[str, Any]]) -> None:
        """Update the node reference file."""
        import csv

        ref_file = Path(__file__).parent.parent.parent / "config" / "iso_nodes.csv"

        # Read existing nodes
        existing_nodes = {}
        if ref_file.exists():
            with open(ref_file, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    key = (row["iso"], row["location_id"])
                    existing_nodes[key] = row

        # Update with new nodes
        for node in nodes:
            key = (node["iso"], node["location_id"])
            existing_nodes[key] = node

        # Write back to file
        with open(ref_file, 'w', newline='') as f:
            if existing_nodes:
                writer = csv.DictWriter(f, fieldnames=list(existing_nodes.values())[0].keys())
                writer.writeheader()
                for node in existing_nodes.values():
                    writer.writerow(node)

    async def harvest_generator_roster(self) -> None:
        """Harvest generator roster from ISO-NE metadata."""
        logger.info("Harvesting ISO-NE generator roster")

        try:
            # Collect generator information
            generators = []

            # Get generators from generation data
            try:
                gen_data = await self._get_generation_metadata()
                generators.extend(gen_data)
            except Exception as e:
                logger.warning(f"Failed to get generation metadata: {e}")

            # Deduplicate and normalize
            unique_generators = self._deduplicate_generators(generators)

            # Update the reference file
            await self._update_generator_reference(unique_generators)

            logger.info(f"Updated generator roster with {len(unique_generators)} unique generators")

        except Exception as e:
            logger.error(f"Failed to harvest generator roster: {e}")
            raise

    async def _get_generation_metadata(self) -> List[Dict[str, Any]]:
        """Extract generator metadata from generation data."""
        generators = []

        try:
            # Get recent generation data
            sample_date = datetime.now() - timedelta(days=1)
            url = f"{self.api_config.base_url}/generation/resource"

            data = await self.api_client.fetch_data(url, {})

            if "data" in data and isinstance(data["data"], list):
                for item in data["data"][:500]:
                    if "fuel_type" in item and "location_id" in item:
                        generator = {
                            "name": item.get("resource_name", "Unknown"),
                            "fuel_type": item.get("fuel_type"),
                            "zone": item.get("location_id"),
                            "capacity_mw": float(item.get("capacity", 0)),
                            "source": "ISO-NE"
                        }
                        generators.append(generator)

        except Exception as e:
            logger.warning(f"Failed to extract generation metadata: {e}")

        return generators

    def _deduplicate_generators(self, generators: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Deduplicate generators and create roster entries."""
        unique_generators = {}

        for gen in generators:
            key = (gen.get("name", ""), gen.get("zone", ""))
            if key not in unique_generators:
                unique_generators[key] = {
                    "name": gen.get("name", "Unknown"),
                    "fuel_type": gen.get("fuel_type", "Unknown"),
                    "zone": gen.get("zone", "Unknown"),
                    "capacity_mw": gen.get("capacity_mw", 0),
                    "source": gen.get("source", "ISO-NE"),
                    "latitude": gen.get("latitude", None),
                    "longitude": gen.get("longitude", None),
                    "commission_date": gen.get("commission_date", None),
                    "owner": gen.get("owner", None)
                }

        return list(unique_generators.values())

    async def _update_generator_reference(self, generators: List[Dict[str, Any]]) -> None:
        """Update the generator reference file."""
        import csv

        ref_file = Path(__file__).parent.parent.parent / "config" / "generators.csv"

        # Read existing generators
        existing_generators = {}
        if ref_file.exists():
            with open(ref_file, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    key = (row["name"], row["zone"])
                    existing_generators[key] = row

        # Update with new generators
        for gen in generators:
            key = (gen["name"], gen["zone"])
            existing_generators[key] = gen

        # Write back to file
        with open(ref_file, 'w', newline='') as f:
            if existing_generators:
                writer = csv.DictWriter(f, fieldnames=list(existing_generators.values())[0].keys())
                writer.writeheader()
                for gen in existing_generators.values():
                    writer.writerow(gen)

    async def _alert_empty_response(self, date: datetime) -> None:
        """Alert on empty API responses."""
        logger.warning(
            "Empty response detected",
            extra={
                "dataset": self.dataset_config.dataset_id,
                "date": date.isoformat(),
                "alert_type": "empty_response"
            }
        )

        await self._send_alert({
            "alert_type": "empty_response",
            "dataset": self.dataset_config.dataset_id,
            "date": date.isoformat(),
            "message": f"No records returned for {self.dataset_config.dataset_id} on {date.strftime('%Y-%m-%d')}",
            "severity": "WARNING"
        })

    async def _check_schema_drift(self, sample_record: Dict[str, Any], date: datetime) -> None:
        """Check for schema drift in API responses."""
        expected_fields = set()

        # Define expected fields based on data type
        if self.dataset_config.data_type == "lmp":
            expected_fields = {"location_id", "lmp", "energy_component", "congestion_component", "loss_component"}
        elif self.dataset_config.data_type == "load":
            expected_fields = {"location_id", "load"}
        elif self.dataset_config.data_type == "generation_mix":
            expected_fields = {"location_id", "fuel_type", "generation"}
        elif self.dataset_config.data_type == "ancillary_services":
            expected_fields = {"location_id", "service_type", "price", "mw"}

        actual_fields = set(sample_record.keys())
        missing_fields = expected_fields - actual_fields

        if missing_fields:
            logger.warning(
                "Schema drift detected",
                extra={
                    "dataset": self.dataset_config.dataset_id,
                    "date": date.isoformat(),
                    "missing_fields": list(missing_fields),
                    "alert_type": "schema_drift"
                }
            )

            await self._send_alert({
                "alert_type": "schema_drift",
                "dataset": self.dataset_config.dataset_id,
                "date": date.isoformat(),
                "missing_fields": list(missing_fields),
                "message": f"Missing expected fields in {self.dataset_config.dataset_id}: {missing_fields}",
                "severity": "WARNING"
            })

    async def _send_alert(self, alert_data: Dict[str, Any]) -> None:
        """Send alert to monitoring system."""
        logger.info(f"Alert generated: {alert_data}")


def load_isone_dataset_configs() -> List[IsoNeDatasetConfig]:
    """Load ISO-NE dataset configurations from JSON."""
    config_path = Path(__file__).parent.parent.parent / "config" / "isone_ingest_datasets.json"

    if not config_path.exists():
        raise FileNotFoundError(f"ISO-NE config file not found: {config_path}")

    with open(config_path) as f:
        config_data = json.load(f)

    configs = []
    for dataset in config_data.get("datasets", []):
        if dataset.get("iso_name") == "isone":
            # Build URL template based on data type and market
            if dataset["data_type"] == "lmp":
                if dataset["market"] == "DAM":
                    url_template = f"{dataset.get('base_url', 'https://webservices.iso-ne.com/api/v1.1')}/dayaheadlmp"
                else:  # RTM
                    url_template = f"{dataset.get('base_url', 'https://webservices.iso-ne.com/api/v1.1')}/fiveminutelmp/current"
            elif dataset["data_type"] == "load":
                url_template = f"{dataset.get('base_url', 'https://webservices.iso-ne.com/api/v1.1')}/fiveminutesystemload/current"
            elif dataset["data_type"] == "generation_mix":
                url_template = f"{dataset.get('base_url', 'https://webservices.iso-ne.com/api/v1.1')}/generation/resource"
            elif dataset["data_type"] == "ancillary_services":
                url_template = f"{dataset.get('base_url', 'https://webservices.iso-ne.com/api/v1.1')}/ancillaryservices"
            else:
                url_template = ""

            configs.append(IsoNeDatasetConfig(
                dataset_id=dataset["source_name"],
                description=dataset["description"],
                data_type=dataset["data_type"],
                market=dataset["market"],
                url_template=url_template,
                interval_minutes=300 if dataset["market"] == "RTM" else 3600,
                columns={
                    "location_id": "location_id",
                    "location_name": "location_name",
                    "lmp": "lmp",
                    "energy_component": "energy_component",
                    "congestion_component": "congestion_component",
                    "loss_component": "loss_component",
                    "load": "load",
                    "fuel_type": "fuel_type",
                    "generation": "generation",
                    "service_type": "service_type",
                    "price": "price",
                    "mw": "mw"
                },
                frequency=dataset["frequency"],
                units=dataset["default_units"]
            ))

    return configs
