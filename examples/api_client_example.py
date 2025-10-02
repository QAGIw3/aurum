#!/usr/bin/env python3
"""Example client demonstrating how to use the V2 API endpoints.

Shows:
- Using the new v2 endpoints
- Leveraging caching for performance
- Error handling
- Response structure
"""

from __future__ import annotations

import asyncio
from typing import Any, Dict, List


try:
    import httpx
except ImportError:
    print("httpx not installed. Install with: pip install httpx")
    print("This is a demo file showing API usage patterns.")
    httpx = None


class AurumAPIClient:
    """Client for Aurum V2 API."""
    
    def __init__(self, base_url: str = "http://localhost:8095"):
        """Initialize client.
        
        Args:
            base_url: Base URL for API
        """
        self.base_url = base_url.rstrip("/")
        if httpx:
            self.client = httpx.AsyncClient(base_url=self.base_url, timeout=30.0)
        else:
            self.client = None
    
    async def get_curves(
        self,
        iso: str = None,
        market: str = None,
        limit: int = 100,
        use_cache: bool = True
    ) -> Dict[str, Any]:
        """Get curves from V2 API.
        
        Args:
            iso: ISO identifier
            market: Market type
            limit: Maximum results
            use_cache: Whether to use server-side caching
            
        Returns:
            API response with curves data
        """
        params = {"limit": limit, "use_cache": use_cache}
        if iso:
            params["iso"] = iso
        if market:
            params["market"] = market
        
        response = await self.client.get("/v2/curves", params=params)
        response.raise_for_status()
        return response.json()
    
    async def export_curves(self, iso: str = None, market: str = None):
        """Export curves as streaming response.
        
        Args:
            iso: ISO identifier
            market: Market type
            
        Yields:
            Individual curve records
        """
        params = {}
        if iso:
            params["iso"] = iso
        if market:
            params["market"] = market
        
        async with self.client.stream("GET", "/v2/curves/export", params=params) as response:
            response.raise_for_status()
            async for line in response.aiter_lines():
                if line.strip() and line not in ["[", "]", ","]:
                    import json
                    yield json.loads(line.rstrip(","))
    
    async def create_scenario(
        self,
        name: str,
        description: str = None,
        assumptions: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """Create a new scenario.
        
        Args:
            name: Scenario name
            description: Scenario description
            assumptions: Scenario assumptions
            
        Returns:
            Created scenario data
        """
        payload = {"name": name}
        if description:
            payload["description"] = description
        if assumptions:
            payload["assumptions"] = assumptions
        
        response = await self.client.post("/v2/scenarios", json=payload)
        response.raise_for_status()
        return response.json()
    
    async def get_scenario(self, scenario_id: str, use_cache: bool = True) -> Dict[str, Any]:
        """Get scenario by ID.
        
        Args:
            scenario_id: Scenario UUID
            use_cache: Whether to use server-side caching
            
        Returns:
            Scenario data
        """
        params = {"use_cache": use_cache}
        response = await self.client.get(f"/v2/scenarios/{scenario_id}", params=params)
        response.raise_for_status()
        return response.json()
    
    async def get_dimensions(
        self,
        dataset: str,
        dimension: str,
        use_cache: bool = True
    ) -> Dict[str, Any]:
        """Get dimension values.
        
        Args:
            dataset: Dataset name
            dimension: Dimension name
            use_cache: Whether to use server-side caching
            
        Returns:
            Dimension values
        """
        params = {"use_cache": use_cache}
        response = await self.client.get(
            f"/v2/metadata/dimensions/{dataset}/{dimension}",
            params=params
        )
        response.raise_for_status()
        return response.json()
    
    async def search_metadata(self, query: str, limit: int = 100) -> Dict[str, Any]:
        """Search metadata.
        
        Args:
            query: Search query
            limit: Maximum results
            
        Returns:
            Search results
        """
        params = {"q": query, "limit": limit}
        response = await self.client.get("/v2/metadata/search", params=params)
        response.raise_for_status()
        return response.json()
    
    async def get_iso_lmp(
        self,
        iso: str,
        market_type: str = None,
        limit: int = 100
    ) -> Dict[str, Any]:
        """Get ISO LMP data.
        
        Args:
            iso: ISO identifier
            market_type: Market type (DA, RT)
            limit: Maximum results
            
        Returns:
            LMP data
        """
        params = {"iso": iso, "limit": limit}
        if market_type:
            params["market_type"] = market_type
        
        response = await self.client.get("/v2/iso/lmp", params=params)
        response.raise_for_status()
        return response.json()
    
    async def close(self):
        """Close client connection."""
        if self.client:
            await self.client.aclose()


async def demo_basic_usage():
    """Demonstrate basic API usage."""
    print("=" * 60)
    print("Demo: Basic V2 API Usage")
    print("=" * 60)
    
    if not httpx:
        print("\n📝 Example code (httpx not installed):\n")
        print("client = AurumAPIClient('http://localhost:8095')")
        print()
        print("# Get curves with caching")
        print("result = await client.get_curves(iso='PJM', market='DA', use_cache=True)")
        print("print(f'Source: {result[\"metadata\"][\"source\"]}')  # 'cache' or 'database'")
        print()
        print("# Search metadata")
        print("results = await client.search_metadata('power', limit=50)")
        print()
        print("# Create scenario")
        print("scenario = await client.create_scenario('My Scenario', description='Test')")
        print()
        return
    
    client = AurumAPIClient("http://localhost:8095")
    
    try:
        print("\n1. Querying curves (with cache)...")
        curves = await client.get_curves(iso="PJM", market="DA", limit=10, use_cache=True)
        print(f"   - Source: {curves.get('metadata', {}).get('source', 'unknown')}")
        print(f"   - Count: {curves.get('metadata', {}).get('count', 0)}")
        
        print("\n2. Getting dimensions...")
        dims = await client.get_dimensions("curves", "iso", use_cache=True)
        print(f"   - Found {len(dims.get('data', []))} ISOs")
        print(f"   - Source: {dims.get('metadata', {}).get('source', 'unknown')}")
        
        print("\n3. Searching metadata...")
        search = await client.search_metadata("electricity", limit=20)
        print(f"   - Found {len(search.get('data', []))} results")
        
        print("\n✅ All API calls successful!")
        
    except Exception as e:
        print(f"\n⚠️  API calls skipped (server not running): {type(e).__name__}")
        print("   Start the API server to run live demos")
    finally:
        await client.close()


async def demo_caching_behavior():
    """Demonstrate caching behavior."""
    print("\n" + "=" * 60)
    print("Demo: Caching Behavior")
    print("=" * 60)
    
    print("\n📝 Caching Features:\n")
    print("1. First query - hits database:")
    print("   GET /v2/curves?iso=PJM&market=DA&use_cache=true")
    print("   Response: {\"metadata\": {\"source\": \"database\"}}")
    print()
    print("2. Second query (same params) - hits cache:")
    print("   GET /v2/curves?iso=PJM&market=DA&use_cache=true")
    print("   Response: {\"metadata\": {\"source\": \"cache\"}}")
    print()
    print("3. Disable caching for specific query:")
    print("   GET /v2/curves?iso=PJM&market=DA&use_cache=false")
    print("   Response: {\"metadata\": {\"source\": \"database\"}}")
    print()
    print("4. Invalidate cache after data update:")
    print("   POST /v2/curves/cache/invalidate?iso=PJM&market=DA")
    print()
    print("Benefits:")
    print("  - 5-10x faster for cache hits")
    print("  - Reduces database load")
    print("  - Per-query cache control")
    print("  - Automatic cache key generation")
    print()


def main_sync():
    """Main entry point."""
    print("\n")
    print("╔" + "=" * 58 + "╗")
    print("║" + " " * 15 + "V2 API Client Example" + " " * 21 + "║")
    print("╚" + "=" * 58 + "╝")
    print()
    
    asyncio.run(demo_basic_usage())
    asyncio.run(demo_caching_behavior())
    
    print("=" * 60)
    print("Summary:")
    print("  ✓ V2 API provides modern, consistent endpoints")
    print("  ✓ Optional caching improves performance")
    print("  ✓ Standard response format across all endpoints")
    print("  ✓ Comprehensive error handling")
    print("  ✓ Service layer handles business logic")
    print("=" * 60)
    print()


if __name__ == "__main__":
    main_sync()

