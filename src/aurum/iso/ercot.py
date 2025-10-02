"""ERCOT (Electric Reliability Council of Texas) data extractor implementation.

This extractor provides access to ERCOT market data through their MIS (Market Information System) API.
Supports SPP (Settlement Point Prices), load, generation, and ancillary services data.
"""

from __future__ import annotations

import io
import logging
import zipfile
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
import csv

import httpx

from .base import IsoBaseExtractor, IsoConfig

logger = logging.getLogger(__name__)


class ErcotExtractor(IsoBaseExtractor):
    """ERCOT MIS data extractor.
    
    Provides access to:
    - Day-Ahead and Real-Time SPP (Settlement Point Prices)
    - System and zonal load data
    - Generation by fuel type
    - Ancillary services clearing prices
    """
    
    def __init__(self, config: IsoConfig):
        super().__init__(config)
        self.base_url = config.base_url or "https://www.ercot.com/api/1"
        self.mis_base_url = "https://www.ercot.com/misdownload/servlets/mirDownload"
        self.markets = {"DAM": "DAM", "RTM": "RTM", "SCED": "RTM", "DA": "DAM", "RT": "RTM"}
        
        # ERCOT report type IDs for different data
        self.report_types = {
            "dam_spp": "12331",  # Day-Ahead Settlement Point Prices
            "rtm_spp": "12301",  # Real-Time Settlement Point Prices
            "system_load": "13101",  # Actual System Load
            "wind_forecast": "13028",  # Wind Power Production
            "solar_forecast": "13483",  # Solar Power Production
        }
    
    def _get_auth_headers(self) -> Dict[str, str]:
        """Get authentication headers for ERCOT API."""
        headers = {
            "Accept": "application/json,application/zip,text/csv",
            "User-Agent": "Aurum/1.0"
        }
        
        # ERCOT MIS may require bearer token for some endpoints
        if self.config.api_key:
            headers["Authorization"] = f"Bearer {self.config.api_key}"
        
        return headers
    
    def _setup_rate_limiting(self) -> None:
        """Configure rate limiting for ERCOT API."""
        self.config.requests_per_minute = 30  # ERCOT has stricter limits
        self.config.requests_per_hour = 1000
    
    async def get_lmp_data(
        self,
        start_date: str,
        end_date: str,
        market: str = "DAM",
        nodes: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """Get SPP (Settlement Point Price) data from ERCOT.
        
        Args:
            start_date: Start date (YYYY-MM-DD format)
            end_date: End date (YYYY-MM-DD format)
            market: Market type (DAM for Day-Ahead, RTM for Real-Time)
            nodes: List of settlement point names (None = all points)
            
        Returns:
            List of SPP data points
        """
        market_type = self.markets.get(market, market)
        
        # Select appropriate report type
        if market_type == "DAM":
            report_type = self.report_types["dam_spp"]
        else:
            report_type = self.report_types["rtm_spp"]
        
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        
        all_data = []
        current_date = start_dt
        
        while current_date <= end_dt:
            try:
                data = await self._download_mis_report(
                    report_type,
                    current_date,
                    market_type,
                    nodes
                )
                all_data.extend(data)
                
            except Exception as e:
                logger.error(f"Error getting SPP data for {current_date}: {e}")
            
            current_date += timedelta(days=1)
        
        return all_data
    
    async def _download_mis_report(
        self,
        report_type: str,
        date: datetime,
        market: str,
        nodes: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """Download and parse ERCOT MIS report."""
        # ERCOT MIS URL format
        params = {
            "reportTypeId": report_type,
            "mimic_duns": "1",
            "startDate": date.strftime("%m/%d/%Y"),
            "endDate": date.strftime("%m/%d/%Y"),
        }
        
        async with self._get_client() as client:
            response = await client.get(
                self.mis_base_url,
                params=params,
                headers=self._get_auth_headers(),
                timeout=60.0  # Large files may take time
            )
            
            if response.status_code != 200:
                logger.warning(
                    f"Failed to download report {report_type} for {date}: "
                    f"Status {response.status_code}"
                )
                return []
            
            # ERCOT returns zip files containing CSV data
            if response.headers.get("content-type", "").startswith("application/zip"):
                return self._parse_zip_response(response.content, market, nodes)
            else:
                # Sometimes returns CSV directly
                return self._parse_csv_response(response.text, market, nodes)
    
    def _parse_zip_response(
        self,
        zip_content: bytes,
        market: str,
        nodes: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """Parse ERCOT zip file containing CSV data."""
        data = []
        
        try:
            with zipfile.ZipFile(io.BytesIO(zip_content)) as zf:
                for filename in zf.namelist():
                    if filename.endswith('.csv'):
                        with zf.open(filename) as csv_file:
                            csv_content = csv_file.read().decode('utf-8')
                            data.extend(
                                self._parse_csv_response(csv_content, market, nodes)
                            )
        
        except Exception as e:
            logger.error(f"Failed to parse ERCOT zip response: {e}")
        
        return data
    
    def _parse_csv_response(
        self,
        csv_content: str,
        market: str,
        nodes: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """Parse ERCOT CSV response for SPP data."""
        data = []
        
        try:
            csv_reader = csv.DictReader(io.StringIO(csv_content))
            
            for row in csv_reader:
                # ERCOT CSV column names vary by report
                # Common patterns for SPP reports
                settlement_point = (
                    row.get("Settlement Point") or
                    row.get("Settlement Point Name") or
                    row.get("SettlementPoint")
                )
                
                if not settlement_point:
                    continue
                
                # Filter by nodes if specified
                if nodes and settlement_point not in nodes:
                    continue
                
                # Parse timestamp
                delivery_date = row.get("Delivery Date") or row.get("DeliveryDate")
                delivery_hour = row.get("Delivery Hour") or row.get("DeliveryHour") or row.get("Hour Ending")
                
                if not delivery_date or not delivery_hour:
                    continue
                
                # Convert ERCOT hour ending to start time
                date_obj = datetime.strptime(delivery_date, "%m/%d/%Y")
                hour = int(delivery_hour.split(":")[0]) - 1  # Hour ending to hour beginning
                timestamp = date_obj.replace(hour=hour)
                
                # Parse price
                price = float(
                    row.get("Settlement Point Price") or
                    row.get("SPP") or
                    row.get("Price") or
                    "0"
                )
                
                data.append({
                    "iso": "ERCOT",
                    "market": market,
                    "node": settlement_point,
                    "node_name": settlement_point,
                    "node_type": row.get("Settlement Point Type", "Unknown"),
                    "timestamp": timestamp.isoformat(),
                    "interval_start": timestamp.isoformat(),
                    "interval_end": (timestamp + timedelta(hours=1)).isoformat(),
                    "lmp": price,
                    "energy": price,  # ERCOT SPP is energy-only
                    "congestion": 0.0,  # No separate congestion component
                    "loss": 0.0,  # No separate loss component
                    "currency": "USD",
                    "unit": "$/MWh"
                })
        
        except Exception as e:
            logger.error(f"Failed to parse ERCOT CSV response: {e}")
        
        return data
    
    async def get_load_data(
        self,
        start_date: str,
        end_date: str,
        zones: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """Get system load data from ERCOT.
        
        Args:
            start_date: Start date (YYYY-MM-DD format)
            end_date: End date (YYYY-MM-DD format)
            zones: List of weather zones (None = system total)
            
        Returns:
            List of load data points
        """
        report_type = self.report_types["system_load"]
        
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        
        all_data = []
        current_date = start_dt
        
        while current_date <= end_dt:
            try:
                params = {
                    "reportTypeId": report_type,
                    "startDate": current_date.strftime("%m/%d/%Y"),
                    "endDate": current_date.strftime("%m/%d/%Y"),
                }
                
                async with self._get_client() as client:
                    response = await client.get(
                        self.mis_base_url,
                        params=params,
                        headers=self._get_auth_headers(),
                        timeout=60.0
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
        csv_content: str,
        zones: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """Parse ERCOT load CSV response."""
        data = []
        
        try:
            csv_reader = csv.DictReader(io.StringIO(csv_content))
            
            for row in csv_reader:
                # Parse timestamp
                date_str = row.get("Delivery Date") or row.get("DELIVERY_DATE")
                hour_str = row.get("Hour Ending") or row.get("HOUR_ENDING")
                
                if not date_str or not hour_str:
                    continue
                
                date_obj = datetime.strptime(date_str, "%m/%d/%Y")
                hour = int(hour_str.split(":")[0]) - 1
                timestamp = date_obj.replace(hour=hour)
                
                # System-wide load
                ercot_load = float(row.get("ERCOT", "0") or row.get("Total", "0"))
                
                data.append({
                    "iso": "ERCOT",
                    "zone": "ERCOT",
                    "timestamp": timestamp.isoformat(),
                    "load_mw": ercot_load,
                    "load_type": "actual",
                    "unit": "MW"
                })
                
                # Add zonal loads if available
                if zones:
                    for zone in zones:
                        if zone in row and row[zone]:
                            data.append({
                                "iso": "ERCOT",
                                "zone": zone,
                                "timestamp": timestamp.isoformat(),
                                "load_mw": float(row[zone]),
                                "load_type": "actual",
                                "unit": "MW"
                            })
        
        except Exception as e:
            logger.error(f"Failed to parse ERCOT load response: {e}")
        
        return data
    
    async def get_generation_mix(
        self,
        start_date: str,
        end_date: str,
        zones: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """Get generation mix data from ERCOT.
        
        ERCOT provides wind and solar generation forecasts/actuals.
        
        Args:
            start_date: Start date (YYYY-MM-DD format)
            end_date: End date (YYYY-MM-DD format)
            zones: Not used for ERCOT
            
        Returns:
            List of generation data by fuel type
        """
        all_data = []
        
        # Get wind generation
        wind_data = await self._get_renewable_generation(
            self.report_types["wind_forecast"],
            start_date,
            end_date,
            "Wind"
        )
        all_data.extend(wind_data)
        
        # Get solar generation
        solar_data = await self._get_renewable_generation(
            self.report_types["solar_forecast"],
            start_date,
            end_date,
            "Solar"
        )
        all_data.extend(solar_data)
        
        return all_data
    
    async def _get_renewable_generation(
        self,
        report_type: str,
        start_date: str,
        end_date: str,
        fuel_type: str
    ) -> List[Dict[str, Any]]:
        """Get renewable generation data from ERCOT."""
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        
        all_data = []
        current_date = start_dt
        
        while current_date <= end_dt:
            try:
                params = {
                    "reportTypeId": report_type,
                    "startDate": current_date.strftime("%m/%d/%Y"),
                    "endDate": current_date.strftime("%m/%d/%Y"),
                }
                
                async with self._get_client() as client:
                    response = await client.get(
                        self.mis_base_url,
                        params=params,
                        headers=self._get_auth_headers(),
                        timeout=60.0
                    )
                    
                    if response.status_code == 200:
                        data = self._parse_generation_response(
                            response.text,
                            fuel_type
                        )
                        all_data.extend(data)
                    
            except Exception as e:
                logger.error(
                    f"Error getting {fuel_type} generation for {current_date}: {e}"
                )
            
            current_date += timedelta(days=1)
        
        return all_data
    
    def _parse_generation_response(
        self,
        csv_content: str,
        fuel_type: str
    ) -> List[Dict[str, Any]]:
        """Parse ERCOT generation CSV response."""
        data = []
        
        try:
            csv_reader = csv.DictReader(io.StringIO(csv_content))
            
            for row in csv_reader:
                # Parse timestamp
                date_str = row.get("Delivery Date") or row.get("DELIVERY_DATE")
                hour_str = row.get("Hour Ending") or row.get("HOUR_ENDING")
                
                if not date_str or not hour_str:
                    continue
                
                date_obj = datetime.strptime(date_str, "%m/%d/%Y")
                hour = int(hour_str.split(":")[0]) - 1
                timestamp = date_obj.replace(hour=hour)
                
                # Get generation value
                actual_gen = float(
                    row.get("Actual") or
                    row.get("ACTUAL") or
                    row.get("System-Wide Actual", "0")
                )
                
                data.append({
                    "iso": "ERCOT",
                    "timestamp": timestamp.isoformat(),
                    "fuel_type": fuel_type,
                    "generation_mw": actual_gen,
                    "unit": "MW"
                })
        
        except Exception as e:
            logger.error(f"Failed to parse ERCOT generation response: {e}")
        
        return data
    
    async def get_ancillary_services(
        self,
        start_date: str,
        end_date: str,
        zones: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """Get ancillary services data from ERCOT.
        
        Args:
            start_date: Start date (YYYY-MM-DD format)
            end_date: End date (YYYY-MM-DD format)
            zones: Not used for ERCOT
            
        Returns:
            List of ancillary services clearing prices
        """
        # Placeholder - implement based on specific ERCOT ancillary services reports
        logger.info("Ancillary services data retrieval not yet implemented for ERCOT")
        return []