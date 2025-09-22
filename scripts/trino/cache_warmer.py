#!/usr/bin/env python3
"""Trino cache warmer for materialized views and common query patterns.

This script warms up the Trino query cache by executing common analytical queries
against materialized views to ensure fast response times for dashboard and reporting workloads.
"""

import argparse
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any

try:
    import trino
except ImportError:
    trino = None

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Common analytical queries for cache warming
CACHE_WARMING_QUERIES = [
    # Curve price summary queries
    {
        'name': 'curve_price_daily_summary',
        'query': '''
            SELECT
                asof_date,
                iso_code,
                market_code,
                product_code,
                avg_price,
                daily_price_change_pct,
                volatility_ratio
            FROM iceberg.market.curve_price_daily_summary
            WHERE asof_date >= CURRENT_DATE - INTERVAL '7' DAY
            ORDER BY asof_date DESC, avg_price DESC
            LIMIT 1000
        ''',
        'description': 'Daily curve price summary for recent week'
    },

    # External data provider summary
    {
        'name': 'external_data_summary',
        'query': '''
            SELECT
                provider,
                dataset_code,
                asof_date,
                avg_value,
                completeness_ratio,
                quality_score
            FROM iceberg.market.external_data_summary
            WHERE asof_date >= CURRENT_DATE - INTERVAL '3' DAY
            ORDER BY provider, asof_date DESC
            LIMIT 500
        ''',
        'description': 'External data summary for recent days'
    },

    # LMP volatility analysis
    {
        'name': 'lmp_volatility_analysis',
        'query': '''
            SELECT
                iso_code,
                location_id,
                hour_bucket,
                avg_price,
                hourly_volatility,
                volatility_class,
                congestion_severity
            FROM iceberg.market.lmp_volatility_analysis
            WHERE hour_bucket >= CURRENT_DATE - INTERVAL '1' DAY
            ORDER BY hourly_volatility DESC
            LIMIT 200
        ''',
        'description': 'LMP volatility analysis for recent day'
    },

    # Load forecast accuracy
    {
        'name': 'load_forecast_accuracy',
        'query': '''
            SELECT
                iso_code,
                area,
                forecast_date,
                mean_absolute_error_pct,
                forecast_accuracy
            FROM iceberg.market.load_forecast_accuracy
            WHERE forecast_date >= CURRENT_DATE - INTERVAL '7' DAY
            ORDER BY mean_absolute_error_pct ASC
            LIMIT 100
        ''',
        'description': 'Load forecast accuracy for recent week'
    },

    # Price correlation matrix
    {
        'name': 'price_correlation_matrix',
        'query': '''
            SELECT
                iso_a,
                market_a,
                product_a,
                iso_b,
                market_b,
                product_b,
                price_correlation,
                correlation_strength
            FROM iceberg.market.price_correlation_matrix
            WHERE price_correlation > 0.5
            ORDER BY ABS(price_correlation) DESC
            LIMIT 50
        ''',
        'description': 'High correlation price pairs'
    },

    # Market dashboard summary
    {
        'name': 'market_dashboard_summary',
        'query': '''
            WITH latest_prices AS (
                SELECT
                    iso_code,
                    market_code,
                    product_code,
                    asof_date,
                    avg_price,
                    daily_price_change_pct,
                    volatility_ratio,
                    ROW_NUMBER() OVER (
                        PARTITION BY iso_code, market_code, product_code
                        ORDER BY asof_date DESC
                    ) as rn
                FROM iceberg.market.curve_price_daily_summary
                WHERE asof_date >= CURRENT_DATE - INTERVAL '30' DAY
            ),
            market_stats AS (
                SELECT
                    iso_code,
                    COUNT(DISTINCT market_code) as market_count,
                    COUNT(DISTINCT product_code) as product_count,
                    AVG(avg_price) as avg_market_price,
                    STDDEV_POP(avg_price) as price_volatility
                FROM latest_prices
                WHERE rn = 1
                GROUP BY iso_code
            )
            SELECT
                iso_code,
                market_count,
                product_count,
                avg_market_price,
                price_volatility
            FROM market_stats
            ORDER BY avg_market_price DESC
        ''',
        'description': 'Market overview for dashboard'
    },

    # Real-time metrics from Timescale
    {
        'name': 'realtime_system_metrics',
        'query': '''
            SELECT
                time_bucket('5 minutes', ts) as bucket,
                metric,
                AVG(value) as avg_value,
                MAX(value) as max_value,
                COUNT(*) as sample_count
            FROM timescale.public.ops_metrics
            WHERE ts >= NOW() - INTERVAL '1' HOUR
            GROUP BY 1, 2
            ORDER BY 1 DESC, 3 DESC
            LIMIT 100
        ''',
        'description': 'System metrics for the past hour'
    },

    # ISO LMP latest prices
    {
        'name': 'iso_lmp_latest',
        'query': '''
            SELECT
                iso_code,
                location_id,
                market,
                price_total,
                interval_start,
                interval_end
            FROM timescale.public.iso_lmp_latest
            ORDER BY price_total DESC
            LIMIT 50
        ''',
        'description': 'Latest LMP prices from Timescale'
    }
]


class TrinoCacheWarmer:
    """Trino cache warmer for analytical queries."""

    def __init__(self, host: str, port: int = 8080, user: str = 'cache_warmer'):
        if trino is None:
            raise ImportError("trino package is required for cache warming")

        self.host = host
        self.port = port
        self.user = user
        self.connection = None

    def connect(self) -> None:
        """Establish connection to Trino."""
        try:
            self.connection = trino.dbapi.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                catalog='iceberg',
                schema='market'
            )
            logger.info(f"Connected to Trino at {self.host}:{self.port}")
        except Exception as e:
            logger.error(f"Failed to connect to Trino: {e}")
            raise

    def disconnect(self) -> None:
        """Close Trino connection."""
        if self.connection:
            self.connection.close()
            logger.info("Disconnected from Trino")

    def execute_query(self, query: str, description: str) -> Dict[str, Any]:
        """Execute a single query and return performance metrics."""
        if not self.connection:
            raise RuntimeError("Not connected to Trino")

        start_time = time.time()
        rows_affected = 0

        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            rows_affected = len(results)

            execution_time = time.time() - start_time

            # Get query statistics from cursor
            stats = getattr(cursor, 'stats', {})

            logger.info(f"✅ {description}: {rows_affected} rows in {execution_time:.2f}s")

            return {
                'query_name': description,
                'execution_time': execution_time,
                'rows_affected': rows_affected,
                'query_stats': stats
            }

        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"❌ {description}: Failed after {execution_time:.2f}s - {e}")
            return {
                'query_name': description,
                'execution_time': execution_time,
                'rows_affected': 0,
                'error': str(e)
            }

    def warm_cache(self, query_list: Optional[List[Dict]] = None) -> List[Dict]:
        """Execute cache warming queries."""
        queries = query_list or CACHE_WARMING_QUERIES
        results = []

        logger.info(f"Starting cache warming with {len(queries)} queries")

        for query_info in queries:
            result = self.execute_query(
                query_info['query'],
                query_info['name']
            )
            results.append(result)

            # Brief pause between queries to avoid overwhelming the system
            time.sleep(0.5)

        return results

    def refresh_materialized_views(self) -> List[Dict]:
        """Refresh all materialized views."""
        refresh_queries = [
            {
                'name': 'refresh_curve_price_daily_summary',
                'query': 'REFRESH MATERIALIZED VIEW iceberg.market.curve_price_daily_summary',
                'description': 'Refresh curve price daily summary'
            },
            {
                'name': 'refresh_external_data_summary',
                'query': 'REFRESH MATERIALIZED VIEW iceberg.market.external_data_summary',
                'description': 'Refresh external data summary'
            },
            {
                'name': 'refresh_lmp_volatility_analysis',
                'query': 'REFRESH MATERIALIZED VIEW iceberg.market.lmp_volatility_analysis',
                'description': 'Refresh LMP volatility analysis'
            },
            {
                'name': 'refresh_load_forecast_accuracy',
                'query': 'REFRESH MATERIALIZED VIEW iceberg.market.load_forecast_accuracy',
                'description': 'Refresh load forecast accuracy'
            },
            {
                'name': 'refresh_price_correlation_matrix',
                'query': 'REFRESH MATERIALIZED VIEW iceberg.market.price_correlation_matrix',
                'description': 'Refresh price correlation matrix'
            }
        ]

        return self.warm_cache(refresh_queries)

    def get_cache_warming_summary(self, results: List[Dict]) -> Dict[str, Any]:
        """Generate a summary of cache warming results."""
        successful_queries = [r for r in results if 'error' not in r]
        failed_queries = [r for r in results if 'error' in r]

        total_time = sum(r['execution_time'] for r in results)
        total_rows = sum(r['rows_affected'] for r in successful_queries)

        return {
            'total_queries': len(results),
            'successful_queries': len(successful_queries),
            'failed_queries': len(failed_queries),
            'total_execution_time': total_time,
            'total_rows_processed': total_rows,
            'average_query_time': total_time / len(results) if results else 0,
            'queries_per_second': len(results) / total_time if total_time > 0 else 0,
            'results': results
        }


def main():
    """Main entry point for cache warming script."""
    parser = argparse.ArgumentParser(description='Trino cache warmer for analytical queries')
    parser.add_argument('--host', default='localhost', help='Trino host')
    parser.add_argument('--port', type=int, default=8080, help='Trino port')
    parser.add_argument('--user', default='cache_warmer', help='Trino username')
    parser.add_argument('--mode', choices=['warm', 'refresh', 'both'],
                       default='warm', help='Cache warming mode')
    parser.add_argument('--queries', nargs='*', help='Specific queries to run')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose logging')

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Filter queries if specific ones are requested
    queries_to_run = CACHE_WARMING_QUERIES
    if args.queries:
        queries_to_run = [q for q in CACHE_WARMING_QUERIES if q['name'] in args.queries]

    try:
        # Initialize cache warmer
        warmer = TrinoCacheWarmer(args.host, args.port, args.user)
        warmer.connect()

        all_results = []

        # Execute cache warming
        if args.mode in ['warm', 'both']:
            logger.info("Starting cache warming queries...")
            warm_results = warmer.warm_cache(queries_to_run)
            all_results.extend(warm_results)

        # Refresh materialized views
        if args.mode in ['refresh', 'both']:
            logger.info("Refreshing materialized views...")
            refresh_results = warmer.refresh_materialized_views()
            all_results.extend(refresh_results)

        # Generate summary
        summary = warmer.get_cache_warming_summary(all_results)

        logger.info("=== Cache Warming Summary ===")
        logger.info(f"Total queries executed: {summary['total_queries']}")
        logger.info(f"Successful queries: {summary['successful_queries']}")
        logger.info(f"Failed queries: {summary['failed_queries']}")
        logger.info(f"Total execution time: {summary['total_execution_time']:.2f}s")
        logger.info(f"Total rows processed: {summary['total_rows_processed']}")
        logger.info(f"Average query time: {summary['average_query_time']:.2f}s")
        logger.info(f"Queries per second: {summary['queries_per_second']:.2f}")

        if summary['failed_queries'] > 0:
            logger.warning("Some queries failed during cache warming")
            for result in summary['results']:
                if 'error' in result:
                    logger.warning(f"  - {result['query_name']}: {result['error']}")
            return 1
        else:
            logger.info("✅ Cache warming completed successfully")
            return 0

    except Exception as e:
        logger.error(f"Cache warming failed: {e}")
        return 1
    finally:
        if 'warmer' in locals():
            warmer.disconnect()


if __name__ == '__main__':
    exit(main())
