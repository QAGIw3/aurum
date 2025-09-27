"""Data ingestion utilities and helpers."""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from libs.core import PriceObservation, CurveKey
from libs.storage import TimescaleSeriesRepo

logger = logging.getLogger(__name__)


class DataIngestionPipeline:
    """Data ingestion pipeline with repository integration."""
    
    def __init__(self, timescale_repo: TimescaleSeriesRepo):
        self.timescale_repo = timescale_repo
    
    async def ingest_price_data(
        self,
        source: str,
        raw_data: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Ingest price data from various sources."""
        
        observations = []
        errors = []
        
        for i, record in enumerate(raw_data):
            try:
                # Transform raw data to PriceObservation
                obs = self._transform_record(record, source)
                if obs:
                    observations.append(obs)
            except Exception as e:
                errors.append({
                    "record_index": i,
                    "error": str(e),
                    "record": record,
                })
                logger.warning(f"Failed to transform record {i}: {e}")
        
        # Batch insert observations
        inserted_count = 0
        if observations:
            try:
                inserted_count = await self.timescale_repo.store_observations(observations)
                logger.info(f"Inserted {inserted_count} observations from {source}")
            except Exception as e:
                logger.error(f"Failed to insert observations: {e}")
                errors.append({
                    "error": "batch_insert_failed",
                    "message": str(e),
                })
        
        return {
            "source": source,
            "total_records": len(raw_data),
            "transformed_records": len(observations),
            "inserted_records": inserted_count,
            "errors": errors,
            "processed_at": datetime.utcnow().isoformat(),
        }
    
    def _transform_record(
        self,
        record: Dict[str, Any],
        source: str,
    ) -> Optional[PriceObservation]:
        """Transform raw record to PriceObservation."""
        
        try:
            # Common transformation logic
            curve = CurveKey(
                iso=record.get("iso_code", "OTHER"),
                market=record.get("market_type", "UNKNOWN"),
                location=record.get("location", "UNKNOWN"),
                product=record.get("product"),
                block=record.get("price_block"),
            )
            
            obs = PriceObservation(
                curve=curve,
                interval_start=datetime.fromisoformat(record["interval_start"]),
                interval_end=datetime.fromisoformat(record["interval_end"]) if record.get("interval_end") else None,
                delivery_date=datetime.fromisoformat(record["delivery_date"]).date(),
                price=float(record["price"]) if record.get("price") is not None else None,
                currency=record.get("currency_code", "USD"),
                unit=record.get("unit_code", "MWh"),
            )
            
            return obs
            
        except Exception as e:
            logger.warning(f"Failed to transform record: {e}")
            return None


def create_ingestion_pipeline(timescale_repo: TimescaleSeriesRepo) -> DataIngestionPipeline:
    """Factory function to create ingestion pipeline."""
    return DataIngestionPipeline(timescale_repo)