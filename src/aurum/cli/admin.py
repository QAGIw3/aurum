"""Administrative CLI tasks for the Aurum platform."""

from __future__ import annotations

import asyncio
from typing import Optional

import click

from aurum.core.settings import AurumSettings, get_settings
from aurum.libs.storage import PostgresMetaRepo, TimescaleSeriesRepo
from aurum.libs.storage.timescale_ops import TimescalePerformanceOps
from aurum.api.cache.cache import CacheManager


def _configure_environment(env: str) -> AurumSettings:
    """Configure settings for the given environment name."""

    import os

    os.environ.setdefault("AURUM_ENVIRONMENT", env)
    return get_settings()


def _run_async(func):  # type: ignore[no-untyped-def]
    """Decorator to run async click commands."""

    def wrapper(*args, **kwargs):  # type: ignore[no-untyped-def]
        return asyncio.run(func(*args, **kwargs))

    return wrapper


@click.group()
@click.option("--env", default="development", help="Environment name")
@click.pass_context
def main(ctx: click.Context, env: str) -> None:
    """Aurum administrative CLI."""

    ctx.ensure_object(dict)
    ctx.obj["settings"] = _configure_environment(env)


@main.group()
def timescale() -> None:
    """TimescaleDB operations."""


@timescale.command()
@click.pass_context
@_run_async
async def optimize(ctx: click.Context) -> None:
    """Run TimescaleDB optimizations."""

    settings: AurumSettings = ctx.obj["settings"]
    repo = TimescaleSeriesRepo(settings.database)
    ops = TimescalePerformanceOps(repo)
    results = await ops.optimize_all()
    click.echo("TimescaleDB optimizations complete:")
    for category, result in results.items():
        click.echo(f"  {category}: {result}")
    await repo.close()


@timescale.command()
@click.pass_context
@_run_async
async def stats(ctx: click.Context) -> None:
    """Print TimescaleDB statistics."""

    settings: AurumSettings = ctx.obj["settings"]
    repo = TimescaleSeriesRepo(settings.database)
    ops = TimescalePerformanceOps(repo)
    stats = await ops.get_hypertable_stats()
    click.echo("TimescaleDB statistics:")
    for key, value in stats.items():
        click.echo(f"  {key}: {value}")
    await repo.close()


@main.group()
def cache() -> None:
    """Cache operations."""


@cache.command()
@click.option("--pattern", help="Optional pattern to match keys")
@click.pass_context
@_run_async
async def clear(ctx: click.Context, pattern: Optional[str]) -> None:
    """Clear cache entries."""

    settings: AurumSettings = ctx.obj["settings"]
    cache_manager = CacheManager(settings=settings)
    count = await cache_manager.invalidate_pattern(pattern or "")
    click.echo(f"Cleared {count} cache entries")
    await cache_manager.close()


@cache.command()
@click.pass_context
@_run_async
async def stats(ctx: click.Context) -> None:
    """Display cache statistics."""

    settings: AurumSettings = ctx.obj["settings"]
    cache_manager = CacheManager(settings=settings)
    statistics = await cache_manager.get_cache_stats()
    for key, value in statistics.items():
        click.echo(f"{key}: {value}")
    await cache_manager.close()


@main.group()
def db() -> None:
    """Database health checks."""


@db.command()
@click.pass_context
@_run_async
async def health(ctx: click.Context) -> None:
    """Check database health."""

    settings: AurumSettings = ctx.obj["settings"]

    # TimescaleDB
    try:
        timescale_repo = TimescaleSeriesRepo(settings.database)
        _, count = await timescale_repo.list_curves(limit=1)
        click.echo(f"TimescaleDB connected (curves: {count})")
        await timescale_repo.close()
    except Exception as exc:  # pragma: no cover - diagnostics path
        click.echo(f"TimescaleDB error: {exc}")

    # PostgreSQL
    try:
        pg_repo = PostgresMetaRepo(settings.database)
        _, count = await pg_repo.list_scenarios(limit=1)
        click.echo(f"PostgreSQL connected (scenarios: {count})")
        await pg_repo.close()
    except Exception as exc:  # pragma: no cover - diagnostics path
        click.echo(f"PostgreSQL error: {exc}")


@main.command()
@click.pass_context
def config(ctx: click.Context) -> None:
    """Show effective configuration values."""

    settings: AurumSettings = ctx.obj["settings"]
    click.echo("Configuration summary:")
    click.echo(f"  Environment: {settings.environment}")
    click.echo(f"  Debug: {settings.debug}")
    click.echo(f"  API Title: {settings.api.api_title}")
    click.echo(f"  API Version: {settings.api.version}")
    click.echo(f"  Database Host: {settings.database.timescale_host}")
    click.echo(f"  Redis Host: {settings.redis.host}")
    click.echo(f"  Cache TTL (high frequency): {settings.cache.high_frequency_ttl}s")


if __name__ == "__main__":  # pragma: no cover - manual invocation
    main()

