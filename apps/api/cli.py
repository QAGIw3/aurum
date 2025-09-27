#!/usr/bin/env python3
"""CLI administrative tool for Aurum API management."""

import asyncio
import click
from typing import Optional

from libs.common.config import get_settings, AurumSettings
from libs.storage import TimescaleSeriesRepo, PostgresMetaRepo
from libs.storage.timescale_ops import TimescalePerformanceOps
from libs.common.cache import CacheManager


@click.group()
@click.option('--env', default='development', help='Environment name')
@click.pass_context
def cli(ctx, env):
    """Aurum API administration CLI."""
    # Set environment context
    import os
    os.environ["AURUM_ENVIRONMENT"] = env
    
    ctx.ensure_object(dict)
    ctx.obj['settings'] = get_settings()


@cli.group()
def timescale():
    """TimescaleDB operations."""
    pass


@timescale.command()
@click.pass_context
async def optimize(ctx):
    """Run all TimescaleDB optimizations."""
    settings: AurumSettings = ctx.obj['settings']
    
    click.echo("🔧 Running TimescaleDB optimizations...")
    
    try:
        repo = TimescaleSeriesRepo(settings.database)
        ops = TimescalePerformanceOps(repo)
        
        results = await ops.optimize_all()
        
        click.echo("✅ Optimization results:")
        for category, result in results.items():
            click.echo(f"  {category}: {result}")
        
        await repo.close()
        
    except Exception as e:
        click.echo(f"❌ Optimization failed: {e}")


@timescale.command()
@click.pass_context
async def stats(ctx):
    """Get TimescaleDB statistics."""
    settings: AurumSettings = ctx.obj['settings']
    
    try:
        repo = TimescaleSeriesRepo(settings.database)
        ops = TimescalePerformanceOps(repo)
        
        stats = await ops.get_hypertable_stats()
        
        click.echo("📊 TimescaleDB Statistics:")
        for key, value in stats.items():
            click.echo(f"  {key}: {value}")
        
        await repo.close()
        
    except Exception as e:
        click.echo(f"❌ Stats failed: {e}")


@cli.group() 
def cache():
    """Cache operations."""
    pass


@cache.command()
@click.pass_context
async def stats(ctx):
    """Get cache statistics."""
    settings: AurumSettings = ctx.obj['settings']
    
    try:
        cache_manager = CacheManager(settings.redis, settings.cache)
        
        stats = await cache_manager.get_cache_stats()
        
        click.echo("📊 Cache Statistics:")
        for key, value in stats.items():
            if isinstance(value, dict):
                click.echo(f"  {key}:")
                for sub_key, sub_value in value.items():
                    click.echo(f"    {sub_key}: {sub_value}")
            else:
                click.echo(f"  {key}: {value}")
        
        await cache_manager.close()
        
    except Exception as e:
        click.echo(f"❌ Cache stats failed: {e}")


@cache.command()
@click.argument('pattern', required=False)
@click.pass_context
async def clear(ctx, pattern: Optional[str]):
    """Clear cache entries by pattern."""
    settings: AurumSettings = ctx.obj['settings']
    
    try:
        cache_manager = CacheManager(settings.redis, settings.cache)
        
        if pattern:
            count = await cache_manager.invalidate_pattern(pattern)
            click.echo(f"✅ Cleared {count} cache entries matching pattern: {pattern}")
        else:
            # Clear all aurum keys
            count = await cache_manager.invalidate_pattern("")
            click.echo(f"✅ Cleared {count} cache entries")
        
        await cache_manager.close()
        
    except Exception as e:
        click.echo(f"❌ Cache clear failed: {e}")


@cli.group()
def db():
    """Database operations."""
    pass


@db.command()
@click.pass_context
async def health(ctx):
    """Check database health."""
    settings: AurumSettings = ctx.obj['settings']
    
    click.echo("🔍 Checking database health...")
    
    # Check TimescaleDB
    try:
        timescale = TimescaleSeriesRepo(settings.database)
        _, count = await timescale.list_curves(limit=1)
        click.echo(f"✅ TimescaleDB: Connected (curves: {count})")
        await timescale.close()
    except Exception as e:
        click.echo(f"❌ TimescaleDB: {e}")
    
    # Check PostgreSQL
    try:
        postgres = PostgresMetaRepo(settings.database)
        _, count = await postgres.list_scenarios(limit=1)
        click.echo(f"✅ PostgreSQL: Connected (scenarios: {count})")
        await postgres.close()
    except Exception as e:
        click.echo(f"❌ PostgreSQL: {e}")


@cli.command()
@click.pass_context
def config(ctx):
    """Show current configuration."""
    settings: AurumSettings = ctx.obj['settings']
    
    click.echo("⚙️  Current Configuration:")
    click.echo(f"  Environment: {settings.environment}")
    click.echo(f"  Debug: {settings.debug}")
    click.echo(f"  API Title: {settings.api.title}")
    click.echo(f"  API Version: {settings.api.version}")
    click.echo(f"  Database Host: {settings.database.timescale_host}")
    click.echo(f"  Redis Host: {settings.redis.host}")
    click.echo(f"  Cache TTL (High): {settings.cache.high_frequency_ttl}s")
    click.echo(f"  Features:")
    click.echo(f"    V2 Only: {settings.enable_v2_only}")
    click.echo(f"    Timescale CAGGs: {settings.enable_timescale_caggs}")
    click.echo(f"    Iceberg Time Travel: {settings.enable_iceberg_time_travel}")


def run_async_command(func):
    """Wrapper to run async click commands."""
    def wrapper(*args, **kwargs):
        return asyncio.run(func(*args, **kwargs))
    return wrapper


# Apply async wrapper to async commands
optimize.callback = run_async_command(optimize.callback)
stats.callback = run_async_command(stats.callback)
cache.commands['stats'].callback = run_async_command(cache.commands['stats'].callback)
clear.callback = run_async_command(clear.callback)
health.callback = run_async_command(health.callback)


if __name__ == '__main__':
    cli()