"""ISO-NE (ISO New England) data extractor implementation.

This extractor provides access to ISO-NE market data through their Web Services API.
Supports LMP (Locational Marginal Pricing), load, generation mix, and ancillary services data.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
import xml.etree.ElementTree as ET
from base64 import b64encode

import httpx

from .base import IsoBaseExtractor, IsoConfig

logger = logging.getLogger(__name__)


class IsoneExtractor(IsoBaseExtractor):
    """ISO New England data extractor.
    
    Provides access to:
    - Day-Ahead and Real-Time LMP data
    - System load data
    - Generation mix by fuel type
    - Ancillary services (reserves, regulation)
    """
    
    def __init__(self, config: IsoConfig):
        super().__init__(config)
        self.base_url = config.base_url or "https://webservices.iso-ne.com/api/v1.1"
        self.markets = {"DAM": "DA", "RTM": "RT", "DA": "DA", "RT": "RT"}
        self.namespaces = {
            'ns': 'http://www.iso-ne.com/webservices/v1.1',
            'xsi': 'http://www.w3.org/2001/XMLSchema-instance'
        }
    
    def _get_auth_headers(self) -> Dict[str, str]:
        """Get authentication headers for ISO-NE Web Services."""
        if self.config.username and self.config.password:
            # ISO-NE uses basic authentication
            credentials = f"{self.config.username}:{self.config.password}"
            encoded = b64encode(credentials.encode()).decode()
            return {
                "Authorization": f"Basic {encoded}",
                "Accept": "application/xml",
                "Content-Type": "application/xml"
            }
        return {
            "Accept": "application/xml",
            "Content-Type": "application/xml"
        }
    
    def _setup_rate_limiting(self) -> None:
        """Configure rate limiting for ISO-NE API."""
        self.config.requests_per_minute = 60
        self.config.requests_per_hour = 3600
    
    async def get_lmp_data(
        self,
        start_date: str,
        end_date: str,
        market: str = "DAM",
        nodes: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """Get LMP data from ISO-NE.
        
        Args:
            start_date: Start date (YYYY-MM-DD format)
            end_date: End date (YYYY-MM-DD format)
            market: Market type (DAM for Day-Ahead, RTM for Real-Time)
            nodes: List of node names (None = all nodes)
            
        Returns:
            List of LMP data points with price components
        """
        market_type = self.markets.get(market, market)
        
        # ISO-NE endpoints differ by market
        if market_type == "DA":
            endpoint = f"{self.base_url}/dayaheadlmp/day"
        else:
            endpoint = f"{self.base_url}/fivedayslmp/current"
        
        # Format dates for ISO-NE API
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        
        all_data = []
        current_date = start_dt
        
        while current_date <= end_dt:
            try:
                # ISO-NE API typically requires date in format: YYYYMMDD
                date_str = current_date.strftime("%Y%m%d")
                url = f"{endpoint}/{date_str}"
                
                async with self._get_client() as client:
                    response = await client.get(
                        url,
                        headers=self._get_auth_headers(),
                        timeout=30.0
                    )
                    
                    if response.status_code == 200:
                        data = self._parse_lmp_response(
                            response.text,
                            market_type,
                            nodes
                        )
                        all_data.extend(data)
                    else:
                        logger.warning(
                            f"Failed to get LMP data for {date_str}: "
                            f"Status {response.status_code}"
                        )
                
            except Exception as e:
                logger.error(f"Error getting LMP data for {current_date}: {e}")
            
            current_date += timedelta(days=1)
        
        return all_data
    
    def _parse_lmp_response(
        self,
        xml_response: str,
        market: str,
        nodes: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """Parse ISO-NE LMP XML response."""
        try:
            root = ET.fromstring(xml_response)
            data = []
            
            # Find all LMP records in the response
            for lmp_elem in root.findall(".//ns:LMP", self.namespaces):
                location = lmp_elem.find("ns:Location", self.namespaces)
                if location is None:
                    continue
                
                location_id = location.find("ns:LocationID", self.namespaces)
                location_name = location.find("ns:LocationName", self.namespaces)
                location_type = location.find("ns:LocationType", self.namespaces)
                
                if location_id is None:
                    continue
                
                node_name = location_id.text
                
                # Filter by nodes if specified
                if nodes and node_name not in nodes:
                    continue
                
                # Parse price data
                begin_date = lmp_elem.find("ns:BeginDate", self.namespaces)
                lmp_price = lmp_elem.find("ns:LMPPrice", self.namespaces)
                energy_price = lmp_elem.find("ns:EnergyPrice", self.namespaces)
                congestion_price = lmp_elem.find("ns:CongestionPrice", self.namespaces)
                loss_price = lmp_elem.find("ns:LossPrice", self.namespaces)
                
                if begin_date is None or lmp_price is None:
                    continue
                
                # Parse timestamp
                timestamp = datetime.fromisoformat(
                    begin_date.text.replace('Z', '+00:00')
                )
                
                data.append({
                    "iso": "ISONE",
                    "market": market,
                    "node": node_name,
                    "node_name": location_name.text if location_name is not None else node_name,
                    "node_type": location_type.text if location_type is not None else "Unknown",
                    "timestamp": timestamp.isoformat(),
                    "interval_start": timestamp.isoformat(),
                    "interval_end": (timestamp + timedelta(hours=1)).isoformat(),
                    "lmp": float(lmp_price.text),
                    "energy": float(energy_price.text) if energy_price is not None else None,
                    "congestion": float(congestion_price.text) if congestion_price is not None else None,
                    "loss": float(loss_price.text) if loss_price is not None else None,
                    "currency": "USD",
                    "unit": "$/MWh"
                })
            
            return data
            
        except ET.ParseError as e:
            logger.error(f"Failed to parse ISO-NE XML response: {e}")
            return []
    
    async def get_load_data(
        self,
        start_date: str,
        end_date: str,
        zones: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """Get system load data from ISO-NE.
        
        Args:
            start_date: Start date (YYYY-MM-DD format)
            end_date: End date (YYYY-MM-DD format)
            zones: List of zone names (None = system total)
            
        Returns:
            List of load data points
        """
        endpoint = f"{self.base_url}/hourlyloadforecast/day"
        
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        
        all_data = []
        current_date = start_dt
        
        while current_date <= end_dt:
            try:
                date_str = current_date.strftime("%Y%m%d")
                url = f"{endpoint}/{date_str}"
                
                async with self._get_client() as client:
                    response = await client.get(
                        url,
                        headers=self._get_auth_headers(),
                        timeout=30.0
                    )
                    
                    if response.status_code == 200:
                        data = self._parse_load_response(response.text, zones)
                        all_data.extend(data)
                    
            except Exception as e:
                logger.error(f"Error getting load data for {current_date}: {e}")
            
            current_date += timedelta(days=1)
        
        return all_data
    
    def _parse_load_response(
        self,
        xml_response: str,
        zones: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """Parse ISO-NE load XML response."""
        try:
            root = ET.fromstring(xml_response)
            data = []
            
            for load_elem in root.findall(".//ns:HourlyLoad", self.namespaces):
                begin_date = load_elem.find("ns:BeginDate", self.namespaces)
                load_mw = load_elem.find("ns:LoadMW", self.namespaces)
                
                if begin_date is None or load_mw is None:
                    continue
                
                timestamp = datetime.fromisoformat(
                    begin_date.text.replace('Z', '+00:00')
                )
                
                data.append({
                    "iso": "ISONE",
                    "zone": "ISONE",  # System total
                    "timestamp": timestamp.isoformat(),
                    "load_mw": float(load_mw.text),
                    "load_type": "actual",
                    "unit": "MW"
                })
            
            return data
            
        except ET.ParseError as e:
            logger.error(f"Failed to parse ISO-NE load response: {e}")
            return []
    
    async def get_generation_mix(
        self,
        start_date: str,
        end_date: str,
        zones: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """Get generation mix data from ISO-NE.
        
        Args:
            start_date: Start date (YYYY-MM-DD format)
            end_date: End date (YYYY-MM-DD format)
            zones: Not used for ISO-NE (system-wide only)
            
        Returns:
            List of generation mix data by fuel type
        """
        endpoint = f"{self.base_url}/genfuelmix/day"
        
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        
        all_data = []
        current_date = start_dt
        
        while current_date <= end_dt:
            try:
                date_str = current_date.strftime("%Y%m%d")
                url = f"{endpoint}/{date_str}"
                
                async with self._get_client() as client:
                    response = await client.get(
                        url,
                        headers=self._get_auth_headers(),
                        timeout=30.0
                    )
                    
                    if response.status_code == 200:
                        data = self._parse_genmix_response(response.text)
                        all_data.extend(data)
                    
            except Exception as e:
                logger.error(f"Error getting generation mix for {current_date}: {e}")
            
            current_date += timedelta(days=1)
        
        return all_data
    
    def _parse_genmix_response(self, xml_response: str) -> List[Dict[str, Any]]:
        """Parse ISO-NE generation mix XML response."""
        try:
            root = ET.fromstring(xml_response)
            data = []
            
            for mix_elem in root.findall(".//ns:GenFuelMix", self.namespaces):
                begin_date = mix_elem.find("ns:BeginDate", self.namespaces)
                fuel_category = mix_elem.find("ns:FuelCategory", self.namespaces)
                gen_mw = mix_elem.find("ns:GenMW", self.namespaces)
                
                if all([begin_date, fuel_category, gen_mw]):
                    timestamp = datetime.fromisoformat(
                        begin_date.text.replace('Z', '+00:00')
                    )
                    
                    data.append({
                        "iso": "ISONE",
                        "timestamp": timestamp.isoformat(),
                        "fuel_type": fuel_category.text,
                        "generation_mw": float(gen_mw.text),
                        "unit": "MW"
                    })
            
            return data
            
        except ET.ParseError as e:
            logger.error(f"Failed to parse ISO-NE generation mix response: {e}")
            return []
    
    async def get_ancillary_services(
        self,
        start_date: str,
        end_date: str,
        zones: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """Get ancillary services data from ISO-NE.
        
        Args:
            start_date: Start date (YYYY-MM-DD format)
            end_date: End date (YYYY-MM-DD format)
            zones: Not used for ISO-NE
            
        Returns:
            List of ancillary services data (reserves, regulation)
        """
        # Placeholder - implement based on specific ISO-NE ancillary services endpoints
        logger.info("Ancillary services data retrieval not yet implemented for ISO-NE")
        return []