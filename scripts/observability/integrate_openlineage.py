#!/usr/bin/env python3
"""Integration script for OpenLineage event emission in Aurum components.

This script demonstrates how to integrate OpenLineage event emission into:
- dbt model execution
- Seatunnel job execution
- Trino query execution
- Custom pipeline steps

Usage:
    python integrate_openlineage.py --component dbt --model-name curve_observation
    python integrate_openlineage.py --component seatunnel --job-name isone_comprehensive
    python integrate_openlineage.py --component trino --query-id 20241201_123456
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Any

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'src'))

try:
    from aurum.observability.openlineage_emitter import (
        OpenLineageEmitter,
        emit_dbt_start,
        emit_dbt_complete,
        emit_dbt_fail,
        emit_seatunnel_start,
        emit_seatunnel_complete,
        emit_trino_start,
        emit_trino_complete
    )
except ImportError as e:
    print(f"Failed to import OpenLineage emitter: {e}")
    sys.exit(1)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
OPENLINEAGE_URL = os.getenv('OPENLINEAGE_URL', 'http://localhost:5000')
AURUM_NAMESPACE = os.getenv('AURUM_NAMESPACE', 'aurum')


class DBTIntegration:
    """OpenLineage integration for dbt models."""

    @staticmethod
    def on_model_start(model_name: str, run_id: Optional[str] = None,
                      inputs: Optional[List[str]] = None) -> Optional[str]:
        """Called when a dbt model starts execution."""
        logger.info(f"Starting dbt model: {model_name}")

        try:
            success = emit_dbt_start(model_name, run_id, inputs)
            if success:
                logger.info(f"✅ Emitted START event for dbt model: {model_name}")
                return run_id or f"dbt_{model_name}_{int(datetime.now(timezone.utc).timestamp())}"
            else:
                logger.warning(f"❌ Failed to emit START event for dbt model: {model_name}")
        except Exception as e:
            logger.error(f"Error emitting dbt START event: {e}")

        return run_id

    @staticmethod
    def on_model_complete(model_name: str, run_id: Optional[str] = None,
                         inputs: Optional[List[str]] = None,
                         outputs: Optional[List[str]] = None,
                         execution_time: Optional[float] = None) -> bool:
        """Called when a dbt model completes successfully."""
        logger.info(f"Completed dbt model: {model_name}")

        try:
            success = emit_dbt_complete(model_name, run_id, inputs, outputs, execution_time)
            if success:
                logger.info(f"✅ Emitted COMPLETE event for dbt model: {model_name}")
                return True
            else:
                logger.warning(f"❌ Failed to emit COMPLETE event for dbt model: {model_name}")
                return False
        except Exception as e:
            logger.error(f"Error emitting dbt COMPLETE event: {e}")
            return False

    @staticmethod
    def on_model_fail(model_name: str, run_id: Optional[str] = None,
                     error_message: str = "") -> bool:
        """Called when a dbt model fails."""
        logger.error(f"Failed dbt model: {model_name} - {error_message}")

        try:
            success = emit_dbt_fail(model_name, run_id, error_message)
            if success:
                logger.info(f"✅ Emitted FAIL event for dbt model: {model_name}")
                return True
            else:
                logger.warning(f"❌ Failed to emit FAIL event for dbt model: {model_name}")
                return False
        except Exception as e:
            logger.error(f"Error emitting dbt FAIL event: {e}")
            return False


class SeatunnelIntegration:
    """OpenLineage integration for Seatunnel jobs."""

    @staticmethod
    def on_job_start(job_name: str, run_id: Optional[str] = None,
                    inputs: Optional[List[Dict[str, str]]] = None) -> Optional[str]:
        """Called when a Seatunnel job starts."""
        logger.info(f"Starting Seatunnel job: {job_name}")

        try:
            success = emit_seatunnel_start(job_name, inputs)
            if success:
                logger.info(f"✅ Emitted START event for Seatunnel job: {job_name}")
                return run_id or f"seatunnel_{job_name}_{int(datetime.now(timezone.utc).timestamp())}"
            else:
                logger.warning(f"❌ Failed to emit START event for Seatunnel job: {job_name}")
        except Exception as e:
            logger.error(f"Error emitting Seatunnel START event: {e}")

        return run_id

    @staticmethod
    def on_job_complete(job_name: str, run_id: Optional[str] = None,
                       inputs: Optional[List[Dict[str, str]]] = None,
                       outputs: Optional[List[Dict[str, str]]] = None,
                       execution_time: Optional[float] = None) -> bool:
        """Called when a Seatunnel job completes successfully."""
        logger.info(f"Completed Seatunnel job: {job_name}")

        try:
            success = emit_seatunnel_complete(job_name, inputs, outputs, execution_time)
            if success:
                logger.info(f"✅ Emitted COMPLETE event for Seatunnel job: {job_name}")
                return True
            else:
                logger.warning(f"❌ Failed to emit COMPLETE event for Seatunnel job: {job_name}")
                return False
        except Exception as e:
            logger.error(f"Error emitting Seatunnel COMPLETE event: {e}")
            return False


class TrinoIntegration:
    """OpenLineage integration for Trino queries."""

    @staticmethod
    def on_query_start(query_id: str, query_text: Optional[str] = None,
                      run_id: Optional[str] = None) -> Optional[str]:
        """Called when a Trino query starts."""
        logger.info(f"Starting Trino query: {query_id}")

        try:
            success = emit_trino_start(query_id, query_text)
            if success:
                logger.info(f"✅ Emitted START event for Trino query: {query_id}")
                return run_id or f"trino_{query_id}_{int(datetime.now(timezone.utc).timestamp())}"
            else:
                logger.warning(f"❌ Failed to emit START event for Trino query: {query_id}")
        except Exception as e:
            logger.error(f"Error emitting Trino START event: {e}")

        return run_id

    @staticmethod
    def on_query_complete(query_id: str, run_id: Optional[str] = None,
                         inputs: Optional[List[str]] = None,
                         outputs: Optional[List[str]] = None,
                         execution_time: Optional[float] = None) -> bool:
        """Called when a Trino query completes successfully."""
        logger.info(f"Completed Trino query: {query_id}")

        try:
            success = emit_trino_complete(query_id, inputs, outputs, execution_time)
            if success:
                logger.info(f"✅ Emitted COMPLETE event for Trino query: {query_id}")
                return True
            else:
                logger.warning(f"❌ Failed to emit COMPLETE event for Trino query: {query_id}")
                return False
        except Exception as e:
            logger.error(f"Error emitting Trino COMPLETE event: {e}")
            return False


def demonstrate_integration():
    """Demonstrate OpenLineage integration with sample data."""

    logger.info("=== OpenLineage Integration Demonstration ===")

    # Initialize emitter
    emitter = OpenLineageEmitter(OPENLINEAGE_URL, AURUM_NAMESPACE)

    # Example 1: dbt model execution
    logger.info("\n1. dbt Model Integration")
    dbt_run_id = DBTIntegration.on_model_start(
        "curve_observation",
        inputs=["curve_landing", "curve_quarantine"]
    )

    # Simulate model execution
    import time
    time.sleep(0.1)

    DBTIntegration.on_model_complete(
        "curve_observation",
        dbt_run_id,
        inputs=["curve_landing", "curve_quarantine"],
        outputs=["curve_observation"],
        execution_time=5.2
    )

    # Example 2: Seatunnel job execution
    logger.info("\n2. Seatunnel Job Integration")
    seatunnel_run_id = SeatunnelIntegration.on_job_start(
        "isone_comprehensive",
        inputs=[
            {"type": "kafka", "name": "aurum.iso.lmp.v1"},
            {"type": "kafka", "name": "aurum.iso.load.v1"}
        ]
    )

    time.sleep(0.1)

    SeatunnelIntegration.on_job_complete(
        "isone_comprehensive",
        seatunnel_run_id,
        inputs=[
            {"type": "kafka", "name": "aurum.iso.lmp.v1"},
            {"type": "kafka", "name": "aurum.iso.load.v1"}
        ],
        outputs=[
            {"type": "iceberg", "name": "iceberg.external.timeseries_observation"},
            {"type": "timescale", "name": "timescale.public.iso_lmp_timeseries"}
        ],
        execution_time=45.8
    )

    # Example 3: Trino query execution
    logger.info("\n3. Trino Query Integration")
    trino_query_id = "20241201_143022_12345_abcde"
    trino_run_id = TrinoIntegration.on_query_start(
        trino_query_id,
        "SELECT * FROM iceberg.market.curve_observation WHERE asof_date >= '2024-12-01'"
    )

    time.sleep(0.1)

    TrinoIntegration.on_query_complete(
        trino_query_id,
        trino_run_id,
        inputs=["iceberg.market.curve_observation"],
        outputs=["results"],
        execution_time=2.3
    )

    # Example 4: Custom pipeline step
    logger.info("\n4. Custom Pipeline Step")
    custom_run_id = emitter.emit_custom_event(
        "pipeline",
        "daily_curve_processing",
        "START",
        inputs=[
            {"type": "kafka", "name": "aurum.curve.observation.v1"}
        ],
        outputs=[
            {"type": "iceberg", "name": "iceberg.market.curve_observation"}
        ],
        custom_facets={
            "pipelineStep": {
                "stepType": "DATA_PROCESSING",
                "pipelineName": "daily_curve_processing",
                "version": "1.0.0"
            }
        }
    )

    time.sleep(0.1)

    emitter.emit_custom_event(
        "pipeline",
        "daily_curve_processing",
        "COMPLETE",
        run_id=custom_run_id,
        inputs=[
            {"type": "kafka", "name": "aurum.curve.observation.v1"}
        ],
        outputs=[
            {"type": "iceberg", "name": "iceberg.market.curve_observation"}
        ],
        execution_time=120.5,
        custom_facets={
            "pipelineStep": {
                "stepType": "DATA_PROCESSING",
                "pipelineName": "daily_curve_processing",
                "version": "1.0.0"
            }
        }
    )

    logger.info("✅ OpenLineage integration demonstration completed")


def main():
    """Main entry point for OpenLineage integration demonstration."""

    parser = argparse.ArgumentParser(description='Demonstrate OpenLineage integration')
    parser.add_argument('--component', choices=['dbt', 'seatunnel', 'trino', 'demo'],
                       default='demo', help='Component to demonstrate')
    parser.add_argument('--model-name', help='dbt model name')
    parser.add_argument('--job-name', help='Seatunnel job name')
    parser.add_argument('--query-id', help='Trino query ID')
    parser.add_argument('--openlineage-url', default=OPENLINEAGE_URL,
                       help='OpenLineage server URL')
    parser.add_argument('--namespace', default=AURUM_NAMESPACE,
                       help='Aurum namespace')

    args = parser.parse_args()

    # Update global configuration
    global OPENLINEAGE_URL, AURUM_NAMESPACE
    OPENLINEAGE_URL = args.openlineage_url
    AURUM_NAMESPACE = args.namespace

    if args.component == 'demo':
        demonstrate_integration()
    elif args.component == 'dbt':
        if not args.model_name:
            print("❌ --model-name is required for dbt component")
            sys.exit(1)

        # Demonstrate dbt integration
        logger.info(f"dbt Model: {args.model_name}")

        run_id = DBTIntegration.on_model_start(args.model_name)
        print(f"Run ID: {run_id}")

        # Simulate completion
        import time
        time.sleep(1)
        DBTIntegration.on_model_complete(args.model_name, run_id, execution_time=5.0)

    elif args.component == 'seatunnel':
        if not args.job_name:
            print("❌ --job-name is required for seatunnel component")
            sys.exit(1)

        # Demonstrate Seatunnel integration
        logger.info(f"Seatunnel Job: {args.job_name}")

        run_id = SeatunnelIntegration.on_job_start(args.job_name)
        print(f"Run ID: {run_id}")

        # Simulate completion
        import time
        time.sleep(1)
        SeatunnelIntegration.on_job_complete(args.job_name, run_id, execution_time=30.0)

    elif args.component == 'trino':
        if not args.query_id:
            print("❌ --query-id is required for trino component")
            sys.exit(1)

        # Demonstrate Trino integration
        logger.info(f"Trino Query: {args.query_id}")

        run_id = TrinoIntegration.on_query_start(args.query_id)
        print(f"Run ID: {run_id}")

        # Simulate completion
        import time
        time.sleep(1)
        TrinoIntegration.on_query_complete(args.query_id, run_id, execution_time=2.0)


if __name__ == '__main__':
    main()
