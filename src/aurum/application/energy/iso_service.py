"""Application service for ISO market operations."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import List, Optional

from ..common.results import Result, success, failure
from ..common.unit_of_work import UnitOfWork
from ...domain.energy.models.iso import (
    IsoMarket,
    IsoMarketId,
    MarketType,
    LocationalMarginalPrice,
    SystemLoad,
    GenerationMix,
)
from ...domain.shared_kernel.repositories import Repository
from ...domain.shared_kernel.value_objects import TenantId, Location
from ...domain.shared_kernel.exceptions import DomainException


@dataclass(frozen=True)
class CreateIsoMarketCommand:
    """Command to create a new ISO market."""
    
    tenant_id: str
    iso_code: str
    iso_name: str
    timezone: str


@dataclass(frozen=True)
class AddLMPDataCommand:
    """Command to add LMP data to an ISO market."""
    
    iso_market_id: str
    node_id: str
    energy_price: Decimal
    congestion_price: Decimal
    loss_price: Decimal
    timestamp: datetime
    market_type: str  # DAM, RTM, HAM
    location_zone: Optional[str] = None
    location_node: Optional[str] = None


@dataclass(frozen=True)
class AddLoadDataCommand:
    """Command to add load data to an ISO market."""
    
    iso_market_id: str
    zone_id: str
    load_mw: Decimal
    timestamp: datetime
    forecast: bool = False


@dataclass(frozen=True)
class AddGenerationMixCommand:
    """Command to add generation mix data to an ISO market."""
    
    iso_market_id: str
    zone_id: str
    fuel_type: str
    generation_mw: Decimal
    percentage: Decimal
    timestamp: datetime


@dataclass(frozen=True)
class IsoMarketDTO:
    """Data transfer object for ISO market."""
    
    id: str
    tenant_id: str
    iso_code: str
    iso_name: str
    timezone: str
    active: bool
    created_at: datetime
    updated_at: datetime


class IsoApplicationService:
    """Application service for ISO market use cases."""
    
    def __init__(
        self,
        iso_repository: Repository[IsoMarket],
        unit_of_work: UnitOfWork,
    ):
        """Initialize the service.
        
        Args:
            iso_repository: Repository for ISO market aggregates
            unit_of_work: Unit of work for transaction management
        """
        self.iso_repository = iso_repository
        self.unit_of_work = unit_of_work
    
    async def create_iso_market(self, command: CreateIsoMarketCommand) -> Result[IsoMarketDTO]:
        """Create a new ISO market.
        
        Args:
            command: The create ISO market command
            
        Returns:
            Result containing the created ISO market DTO or error
        """
        try:
            iso_market_id = IsoMarketId.generate()
            tenant_id = TenantId.from_string(command.tenant_id)
            
            iso_market = IsoMarket(
                id=iso_market_id,
                tenant_id=tenant_id,
                iso_code=command.iso_code,
                iso_name=command.iso_name,
                timezone=command.timezone,
                active=True,
            )
            
            async with self.unit_of_work:
                await self.iso_repository.save(iso_market)
                await self.unit_of_work.commit()
            
            return success(self._to_dto(iso_market))
            
        except DomainException as e:
            return failure("DOMAIN_ERROR", str(e), e.details)
        except Exception as e:
            return failure("INTERNAL_ERROR", f"Failed to create ISO market: {str(e)}")
    
    async def add_lmp_data(self, command: AddLMPDataCommand) -> Result[IsoMarketDTO]:
        """Add LMP data to an ISO market.
        
        Args:
            command: The add LMP data command
            
        Returns:
            Result containing the updated ISO market DTO or error
        """
        try:
            iso_market_id = IsoMarketId.from_string(command.iso_market_id)
            
            async with self.unit_of_work:
                iso_market = await self.iso_repository.get_by_id(iso_market_id)
                if iso_market is None:
                    return failure("NOT_FOUND", f"ISO market {command.iso_market_id} not found")
                
                location = None
                if command.location_zone or command.location_node:
                    location = Location(
                        zone=command.location_zone,
                        node=command.location_node
                    )
                
                lmp = LocationalMarginalPrice(
                    node_id=command.node_id,
                    location=location,
                    energy_price=command.energy_price,
                    congestion_price=command.congestion_price,
                    loss_price=command.loss_price,
                    timestamp=command.timestamp,
                    market_type=MarketType(command.market_type),
                )
                
                iso_market.add_lmp_data(lmp)
                
                await self.iso_repository.save(iso_market)
                await self.unit_of_work.commit()
            
            return success(self._to_dto(iso_market))
            
        except DomainException as e:
            return failure("DOMAIN_ERROR", str(e), e.details)
        except Exception as e:
            return failure("INTERNAL_ERROR", f"Failed to add LMP data: {str(e)}")
    
    async def add_load_data(self, command: AddLoadDataCommand) -> Result[IsoMarketDTO]:
        """Add load data to an ISO market.
        
        Args:
            command: The add load data command
            
        Returns:
            Result containing the updated ISO market DTO or error
        """
        try:
            iso_market_id = IsoMarketId.from_string(command.iso_market_id)
            
            async with self.unit_of_work:
                iso_market = await self.iso_repository.get_by_id(iso_market_id)
                if iso_market is None:
                    return failure("NOT_FOUND", f"ISO market {command.iso_market_id} not found")
                
                load = SystemLoad(
                    zone_id=command.zone_id,
                    load_mw=command.load_mw,
                    timestamp=command.timestamp,
                    forecast=command.forecast,
                )
                
                iso_market.add_load_data(load)
                
                await self.iso_repository.save(iso_market)
                await self.unit_of_work.commit()
            
            return success(self._to_dto(iso_market))
            
        except DomainException as e:
            return failure("DOMAIN_ERROR", str(e), e.details)
        except Exception as e:
            return failure("INTERNAL_ERROR", f"Failed to add load data: {str(e)}")
    
    async def add_generation_mix(self, command: AddGenerationMixCommand) -> Result[IsoMarketDTO]:
        """Add generation mix data to an ISO market.
        
        Args:
            command: The add generation mix command
            
        Returns:
            Result containing the updated ISO market DTO or error
        """
        try:
            iso_market_id = IsoMarketId.from_string(command.iso_market_id)
            
            async with self.unit_of_work:
                iso_market = await self.iso_repository.get_by_id(iso_market_id)
                if iso_market is None:
                    return failure("NOT_FOUND", f"ISO market {command.iso_market_id} not found")
                
                gen_mix = GenerationMix(
                    zone_id=command.zone_id,
                    fuel_type=command.fuel_type,
                    generation_mw=command.generation_mw,
                    percentage=command.percentage,
                    timestamp=command.timestamp,
                )
                
                iso_market.add_generation_mix(gen_mix)
                
                await self.iso_repository.save(iso_market)
                await self.unit_of_work.commit()
            
            return success(self._to_dto(iso_market))
            
        except DomainException as e:
            return failure("DOMAIN_ERROR", str(e), e.details)
        except Exception as e:
            return failure("INTERNAL_ERROR", f"Failed to add generation mix: {str(e)}")
    
    async def get_iso_market(self, iso_market_id: str) -> Result[IsoMarketDTO]:
        """Get an ISO market by ID.
        
        Args:
            iso_market_id: The ISO market identifier
            
        Returns:
            Result containing the ISO market DTO or error
        """
        try:
            iso_market = await self.iso_repository.get_by_id(IsoMarketId.from_string(iso_market_id))
            if iso_market is None:
                return failure("NOT_FOUND", f"ISO market {iso_market_id} not found")
            
            return success(self._to_dto(iso_market))
            
        except Exception as e:
            return failure("INTERNAL_ERROR", f"Failed to get ISO market: {str(e)}")
    
    async def deactivate_iso_market(self, iso_market_id: str) -> Result[IsoMarketDTO]:
        """Deactivate an ISO market.
        
        Args:
            iso_market_id: The ISO market identifier
            
        Returns:
            Result containing the updated ISO market DTO or error
        """
        try:
            async with self.unit_of_work:
                iso_market = await self.iso_repository.get_by_id(IsoMarketId.from_string(iso_market_id))
                if iso_market is None:
                    return failure("NOT_FOUND", f"ISO market {iso_market_id} not found")
                
                iso_market.deactivate()
                
                await self.iso_repository.save(iso_market)
                await self.unit_of_work.commit()
            
            return success(self._to_dto(iso_market))
            
        except DomainException as e:
            return failure("DOMAIN_ERROR", str(e), e.details)
        except Exception as e:
            return failure("INTERNAL_ERROR", f"Failed to deactivate ISO market: {str(e)}")
    
    def _to_dto(self, iso_market: IsoMarket) -> IsoMarketDTO:
        """Convert ISO market aggregate to DTO.
        
        Args:
            iso_market: The ISO market aggregate
            
        Returns:
            ISO market DTO
        """
        return IsoMarketDTO(
            id=str(iso_market.id),
            tenant_id=str(iso_market.tenant_id),
            iso_code=iso_market.iso_code,
            iso_name=iso_market.iso_name,
            timezone=iso_market.timezone,
            active=iso_market.active,
            created_at=iso_market.created_at,
            updated_at=iso_market.updated_at,
        )

