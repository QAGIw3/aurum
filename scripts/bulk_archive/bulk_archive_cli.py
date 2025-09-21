#!/usr/bin/env python3
"""CLI for bulk archive ingestion operations with checksum verification."""

import argparse
import asyncio
import json
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional

from aurum.bulk_archive import BulkArchiveManager, ArchiveConfig
from aurum.bulk_archive.processor import BulkDataProcessor, ProcessingConfig
from aurum.bulk_archive.monitor import BulkArchiveMonitor
from aurum.cost_profiler import CostProfiler, ProfilerConfig
from aurum.logging import create_logger, LogLevel


class BulkArchiveCLI:
    """CLI for bulk archive operations."""

    def __init__(self):
        """Initialize CLI."""
        self.logger = create_logger(
            source_name="bulk_archive_cli",
            kafka_bootstrap_servers=None,
            kafka_topic="aurum.bulk_archive.cli",
            dataset="bulk_archive_cli"
        )

    async def run_download(
        self,
        source_name: str,
        urls: list,
        output_dir: Path,
        config_overrides: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """Download bulk archives.

        Args:
            source_name: Name of the data source
            urls: List of URLs to download
            output_dir: Output directory
            config_overrides: Configuration overrides

        Returns:
            Download results
        """
        self.logger.log(
            LogLevel.INFO,
            f"Starting bulk archive download for {source_name}",
            "bulk_archive_download_started",
            source_name=source_name,
            url_count=len(urls),
            output_dir=str(output_dir)
        )

        # Create archive configuration
        config = ArchiveConfig(
            source_name=source_name,
            download_dir=output_dir,
            temp_dir=output_dir / "temp",
            max_concurrent_downloads=2,
            download_timeout_seconds=600,
            retry_attempts=3,
            enable_checksum_verification=True,
            enable_size_verification=True,
            **(config_overrides or {})
        )

        results = {
            "operation": "download",
            "source_name": source_name,
            "total_urls": len(urls),
            "successful_downloads": 0,
            "failed_downloads": 0,
            "results": []
        }

        async with BulkArchiveManager(config) as manager:
            for url in urls:
                try:
                    self.logger.log(
                        LogLevel.INFO,
                        f"Downloading {url}",
                        "bulk_archive_download_url_started",
                        source_name=source_name,
                        url=url
                    )

                    download_result = await manager.download_archive(url)

                    if download_result.success:
                        results["successful_downloads"] += 1

                        # Verify download
                        verification = await manager.verify_file(
                            download_result.file_path,
                            download_result.checksum
                        )

                        results["results"].append({
                            "url": url,
                            "download": download_result.__dict__,
                            "verification": verification.__dict__
                        })

                        self.logger.log(
                            LogLevel.INFO,
                            f"Successfully downloaded and verified {download_result.file_path}",
                            "bulk_archive_download_completed",
                            source_name=source_name,
                            file_path=str(download_result.file_path),
                            file_size=download_result.file_size
                        )
                    else:
                        results["failed_downloads"] += 1
                        results["results"].append({
                            "url": url,
                            "download": download_result.__dict__,
                            "verification": None
                        })

                        self.logger.log(
                            LogLevel.ERROR,
                            f"Failed to download {url}: {download_result.error_message}",
                            "bulk_archive_download_failed",
                            source_name=source_name,
                            url=url,
                            error=download_result.error_message
                        )

                except Exception as e:
                    results["failed_downloads"] += 1
                    results["results"].append({
                        "url": url,
                        "error": str(e)
                    })

                    self.logger.log(
                        LogLevel.ERROR,
                        f"Error processing {url}: {e}",
                        "bulk_archive_download_error",
                        source_name=source_name,
                        url=url,
                        error=str(e)
                    )

        results["success_rate"] = results["successful_downloads"] / len(urls) if urls else 0

        self.logger.log(
            LogLevel.INFO,
            f"Bulk archive download completed for {source_name}",
            "bulk_archive_download_completed",
            source_name=source_name,
            successful_downloads=results["successful_downloads"],
            failed_downloads=results["failed_downloads"],
            success_rate=results["success_rate"]
        )

        return results

    async def run_process(
        self,
        source_name: str,
        input_dir: Path,
        output_dir: Path,
        config_overrides: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """Process bulk archive files.

        Args:
            source_name: Name of the data source
            input_dir: Input directory containing bulk files
            output_dir: Output directory for processed data
            config_overrides: Configuration overrides

        Returns:
            Processing results
        """
        self.logger.log(
            LogLevel.INFO,
            f"Starting bulk data processing for {source_name}",
            "bulk_data_processing_started",
            source_name=source_name,
            input_dir=str(input_dir),
            output_dir=str(output_dir)
        )

        # Create processing configuration
        config = ProcessingConfig(
            source_name=source_name,
            data_format="csv",
            chunk_size=5000,
            max_workers=4,
            enable_validation=True,
            quality_threshold=0.95,
            output_format="parquet",
            **(config_overrides or {})
        )

        processor = BulkDataProcessor(config)
        result = await processor.process_directory(input_dir, output_dir)

        # Generate processing report
        report = processor.generate_processing_report(result, input_dir, output_dir)

        self.logger.log(
            LogLevel.INFO,
            f"Bulk data processing completed for {source_name}",
            "bulk_data_processing_completed",
            source_name=source_name,
            files_processed=result.files_processed,
            records_processed=result.records_processed,
            success=result.success
        )

        return report

    async def run_full_pipeline(
        self,
        source_name: str,
        urls: list,
        output_dir: Path,
        config_overrides: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """Run complete bulk archive pipeline (download + process).

        Args:
            source_name: Name of the data source
            urls: List of URLs to download
            output_dir: Output directory
            config_overrides: Configuration overrides

        Returns:
            Pipeline results
        """
        self.logger.log(
            LogLevel.INFO,
            f"Starting full bulk archive pipeline for {source_name}",
            "bulk_archive_pipeline_started",
            source_name=source_name,
            url_count=len(urls),
            output_dir=str(output_dir)
        )

        # Create directories
        download_dir = output_dir / "downloads"
        processing_dir = output_dir / "processed"
        temp_dir = output_dir / "temp"

        download_dir.mkdir(parents=True, exist_ok=True)
        processing_dir.mkdir(parents=True, exist_ok=True)
        temp_dir.mkdir(parents=True, exist_ok=True)

        results = {
            "operation": "full_pipeline",
            "source_name": source_name,
            "pipeline_stages": ["download", "verify", "process"],
            "download_results": None,
            "processing_results": None,
            "overall_success": False
        }

        try:
            # Stage 1: Download
            download_results = await self.run_download(
                source_name, urls, download_dir, config_overrides
            )
            results["download_results"] = download_results

            if download_results["successful_downloads"] == 0:
                raise Exception("No archives were successfully downloaded")

            # Stage 2: Process
            processing_results = await self.run_process(
                source_name, download_dir, processing_dir, config_overrides
            )
            results["processing_results"] = processing_results

            # Overall success
            results["overall_success"] = (
                download_results["success_rate"] > 0.8 and
                processing_results["processing_summary"]["success"]
            )

            self.logger.log(
                LogLevel.INFO,
                f"Bulk archive pipeline completed for {source_name}",
                "bulk_archive_pipeline_completed",
                source_name=source_name,
                overall_success=results["overall_success"],
                download_success_rate=download_results["success_rate"],
                processing_success=processing_results["processing_summary"]["success"]
            )

        except Exception as e:
            results["error"] = str(e)
            self.logger.log(
                LogLevel.ERROR,
                f"Bulk archive pipeline failed for {source_name}: {e}",
                "bulk_archive_pipeline_failed",
                source_name=source_name,
                error=str(e)
            )

        return results

    async def run_monitoring(
        self,
        source_name: str,
        operation_type: str,
        duration_hours: int = 1
    ) -> Dict[str, Any]:
        """Run monitoring for bulk archive operations.

        Args:
            source_name: Name of the data source
            operation_type: Type of operation to monitor
            duration_hours: Duration to monitor

        Returns:
            Monitoring results
        """
        self.logger.log(
            LogLevel.INFO,
            f"Starting monitoring for {source_name} {operation_type} operations",
            "bulk_archive_monitoring_started",
            source_name=source_name,
            operation_type=operation_type,
            duration_hours=duration_hours
        )

        monitor = BulkArchiveMonitor(
            metrics_collection_interval=10.0,
            enable_resource_monitoring=True
        )

        await monitor.start_monitoring()

        try:
            # Simulate monitoring for the specified duration
            await asyncio.sleep(duration_hours * 3600)

            # Generate monitoring report
            report_file = Path(f"bulk_archive_monitoring_report_{source_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
            await monitor.generate_report(report_file)

            # Get summary
            summary = monitor.get_metrics_summary(duration_hours)

            self.logger.log(
                LogLevel.INFO,
                f"Monitoring completed for {source_name}",
                "bulk_archive_monitoring_completed",
                source_name=source_name,
                report_file=str(report_file),
                active_operations=summary.get("archive_operations", {}).get("total", 0)
            )

            return {
                "source_name": source_name,
                "operation_type": operation_type,
                "duration_hours": duration_hours,
                "report_file": str(report_file),
                "summary": summary
            }

        finally:
            await monitor.stop_monitoring()


def main():
    """Main CLI function."""
    parser = argparse.ArgumentParser(
        description="Bulk archive ingestion CLI with checksum verification",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Download EIA bulk archives
  python bulk_archive_cli.py download eia --urls "https://api.eia.gov/bulk/EBA.zip" --output-dir ./data/eia

  # Process downloaded bulk data
  python bulk_archive_cli.py process eia --input-dir ./data/eia/downloads --output-dir ./data/eia/processed

  # Run full pipeline (download + process)
  python bulk_archive_cli.py pipeline eia --urls "https://api.eia.gov/bulk/EBA.zip" --output-dir ./data/eia

  # Monitor bulk operations
  python bulk_archive_cli.py monitor eia --operation-type download --duration-hours 2

  # Download with custom configuration
  python bulk_archive_cli.py download eia --urls "https://api.eia.gov/bulk/EBA.zip" --max-concurrent 1 --batch-size 500

  # Process with validation
  python bulk_archive_cli.py process eia --input-dir ./data --output-dir ./output --quality-threshold 0.98
        """
    )

    subparsers = parser.add_subparsers(dest='command', help='Available commands')

    # Download command
    download_parser = subparsers.add_parser('download', help='Download bulk archives')
    download_parser.add_argument('source_name', help='Data source name')
    download_parser.add_argument('--urls', nargs='+', required=True, help='URLs to download')
    download_parser.add_argument('--output-dir', type=Path, required=True, help='Output directory')
    download_parser.add_argument('--max-concurrent', type=int, default=2, help='Max concurrent downloads')
    download_parser.add_argument('--batch-size', type=int, default=1000, help='Batch size for processing')
    download_parser.add_argument('--no-checksum-verification', action='store_true', help='Disable checksum verification')
    download_parser.add_argument('--config-file', help='Configuration file path')

    # Process command
    process_parser = subparsers.add_parser('process', help='Process bulk data files')
    process_parser.add_argument('source_name', help='Data source name')
    process_parser.add_argument('--input-dir', type=Path, required=True, help='Input directory')
    process_parser.add_argument('--output-dir', type=Path, required=True, help='Output directory')
    process_parser.add_argument('--chunk-size', type=int, default=5000, help='Processing chunk size')
    process_parser.add_argument('--quality-threshold', type=float, default=0.95, help='Data quality threshold')
    process_parser.add_argument('--no-validation', action='store_true', help='Disable data validation')
    process_parser.add_argument('--config-file', help='Configuration file path')

    # Pipeline command
    pipeline_parser = subparsers.add_parser('pipeline', help='Run full pipeline (download + process)')
    pipeline_parser.add_argument('source_name', help='Data source name')
    pipeline_parser.add_argument('--urls', nargs='+', required=True, help='URLs to download')
    pipeline_parser.add_argument('--output-dir', type=Path, required=True, help='Output directory')
    pipeline_parser.add_argument('--max-concurrent', type=int, default=2, help='Max concurrent downloads')
    pipeline_parser.add_argument('--batch-size', type=int, default=1000, help='Batch size')
    pipeline_parser.add_argument('--quality-threshold', type=float, default=0.95, help='Data quality threshold')

    # Monitor command
    monitor_parser = subparsers.add_parser('monitor', help='Monitor bulk operations')
    monitor_parser.add_argument('source_name', help='Data source name')
    monitor_parser.add_argument('--operation-type', choices=['download', 'process', 'all'], default='all', help='Operation type to monitor')
    monitor_parser.add_argument('--duration-hours', type=int, default=1, help='Monitoring duration in hours')
    monitor_parser.add_argument('--output-file', help='Output file for report')

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 1

    # Initialize CLI
    cli = BulkArchiveCLI()

    async def run_async():
        try:
            if args.command == 'download':
                results = await cli.run_download(
                    args.source_name,
                    args.urls,
                    args.output_dir,
                    {
                        'max_concurrent_downloads': args.max_concurrent,
                        'batch_size': args.batch_size,
                        'enable_checksum_verification': not args.no_checksum_verification
                    }
                )

                print(json.dumps(results, indent=2, default=str))
                return 0 if results.get('success_rate', 0) > 0.8 else 1

            elif args.command == 'process':
                results = await cli.run_process(
                    args.source_name,
                    args.input_dir,
                    args.output_dir,
                    {
                        'chunk_size': args.chunk_size,
                        'enable_validation': not args.no_validation,
                        'quality_threshold': args.quality_threshold
                    }
                )

                print(json.dumps(results, indent=2, default=str))
                return 0 if results.get('processing_summary', {}).get('success', False) else 1

            elif args.command == 'pipeline':
                results = await cli.run_full_pipeline(
                    args.source_name,
                    args.urls,
                    args.output_dir,
                    {
                        'max_concurrent_downloads': args.max_concurrent,
                        'batch_size': args.batch_size,
                        'quality_threshold': args.quality_threshold
                    }
                )

                print(json.dumps(results, indent=2, default=str))
                success = results.get('overall_success', False)
                return 0 if success else 1

            elif args.command == 'monitor':
                results = await cli.run_monitoring(
                    args.source_name,
                    args.operation_type,
                    args.duration_hours
                )

                print(json.dumps(results, indent=2, default=str))
                return 0

        except KeyboardInterrupt:
            print("\nOperation interrupted by user")
            return 130
        except Exception as e:
            print(f"Operation failed: {e}")
            return 1

    return asyncio.run(run_async())


if __name__ == "__main__":
    sys.exit(main())
