"""Bulk data processor for handling extracted archive files."""

from __future__ import annotations

import asyncio
import csv
import json
import pandas as pd
from pathlib import Path
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Any, Callable, Union
import tempfile

from ..logging import create_logger, LogLevel


class ProcessingError(Exception):
    """Exception raised during bulk data processing."""
    pass


@dataclass
class ProcessingConfig:
    """Configuration for bulk data processing."""

    source_name: str
    data_format: str = "csv"  # csv, json, parquet, xml
    delimiter: str = ","
    encoding: str = "utf-8"
    chunk_size: int = 10000
    max_workers: int = 4
    enable_parallel_processing: bool = True

    # File processing options
    skip_header: bool = True
    header_row: Optional[int] = 0
    column_mapping: Dict[str, str] = field(default_factory=dict)

    # Data validation options
    enable_validation: bool = True
    validation_rules: Dict[str, Any] = field(default_factory=dict)
    quality_threshold: float = 0.95

    # Output options
    output_format: str = "parquet"
    compression: Optional[str] = "snappy"
    partition_by: List[str] = field(default_factory=list)

    # Processing callbacks
    pre_processing_hook: Optional[Callable] = None
    post_processing_hook: Optional[Callable] = None
    error_handler: Optional[Callable] = None


@dataclass
class ProcessingResult:
    """Result of bulk data processing operation."""

    success: bool = False
    files_processed: int = 0
    records_processed: int = 0
    records_valid: int = 0
    records_invalid: int = 0
    processing_time_seconds: float = 0.0
    output_files: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


class BulkDataProcessor:
    """Processor for bulk data files extracted from archives."""

    def __init__(self, config: ProcessingConfig):
        """Initialize bulk data processor.

        Args:
            config: Processing configuration
        """
        self.config = config
        self.logger = create_logger(
            source_name="bulk_data_processor",
            kafka_bootstrap_servers=None,
            kafka_topic="aurum.bulk_archive.processing",
            dataset="bulk_data_processing"
        )

    async def process_directory(
        self,
        input_dir: Path,
        output_dir: Path
    ) -> ProcessingResult:
        """Process all data files in a directory.

        Args:
            input_dir: Directory containing input files
            output_dir: Directory for output files

        Returns:
            Processing result
        """
        start_time = datetime.now()

        self.logger.log(
            LogLevel.INFO,
            f"Starting bulk data processing for {input_dir}",
            "bulk_processing_started",
            input_dir=str(input_dir),
            output_dir=str(output_dir),
            data_format=self.config.data_format
        )

        try:
            # Pre-processing hook
            if self.config.pre_processing_hook:
                await self.config.pre_processing_hook(input_dir, output_dir)

            # Find files to process
            files_to_process = self._find_files_to_process(input_dir)

            if not files_to_process:
                raise ProcessingError(f"No {self.config.data_format} files found in {input_dir}")

            # Process files
            result = await self._process_files(files_to_process, output_dir)

            # Post-processing hook
            if self.config.post_processing_hook:
                await self.config.post_processing_hook(result, input_dir, output_dir)

            processing_time = (datetime.now() - start_time).total_seconds()

            result.processing_time_seconds = processing_time

            self.logger.log(
                LogLevel.INFO,
                f"Bulk data processing completed for {input_dir}",
                "bulk_processing_completed",
                input_dir=str(input_dir),
                files_processed=result.files_processed,
                records_processed=result.records_processed,
                processing_time_seconds=processing_time
            )

            return result

        except Exception as e:
            processing_time = (datetime.now() - start_time).total_seconds()

            error_result = ProcessingResult(
                success=False,
                processing_time_seconds=processing_time,
                errors=[str(e)]
            )

            self.logger.log(
                LogLevel.ERROR,
                f"Bulk data processing failed for {input_dir}: {e}",
                "bulk_processing_failed",
                input_dir=str(input_dir),
                error=str(e),
                processing_time_seconds=processing_time
            )

            return error_result

    def _find_files_to_process(self, input_dir: Path) -> List[Path]:
        """Find files to process based on configuration.

        Args:
            input_dir: Input directory

        Returns:
            List of files to process
        """
        files_to_process = []

        # File extensions by format
        extensions = {
            "csv": [".csv", ".txt"],
            "json": [".json", ".jsonl"],
            "parquet": [".parquet"],
            "xml": [".xml"]
        }

        if self.config.data_format in extensions:
            for ext in extensions[self.config.data_format]:
                files_to_process.extend(input_dir.rglob(f"*{ext}"))
                files_to_process.extend(input_dir.rglob(f"*{ext.upper()}"))

        # Also check for files with common names
        common_names = ["data", "records", "values", "measurements"]
        for name in common_names:
            for ext in [".csv", ".txt", ".json"]:
                files_to_process.extend(input_dir.rglob(f"{name}{ext}"))
                files_to_process.extend(input_dir.rglob(f"{name}{ext.upper()}"))

        # Remove duplicates and non-files
        files_to_process = list(set(files_to_process))
        files_to_process = [f for f in files_to_process if f.is_file()]

        self.logger.log(
            LogLevel.INFO,
            f"Found {len(files_to_process)} files to process",
            "files_found_for_processing",
            input_dir=str(input_dir),
            file_count=len(files_to_process),
            data_format=self.config.data_format
        )

        return files_to_process

    async def _process_files(
        self,
        files: List[Path],
        output_dir: Path
    ) -> ProcessingResult:
        """Process multiple files.

        Args:
            files: Files to process
            output_dir: Output directory

        Returns:
            Processing result
        """
        result = ProcessingResult()

        # Process files based on format
        if self.config.data_format == "csv":
            result = await self._process_csv_files(files, output_dir)
        elif self.config.data_format == "json":
            result = await self._process_json_files(files, output_dir)
        elif self.config.data_format == "parquet":
            result = await self._process_parquet_files(files, output_dir)
        else:
            raise ProcessingError(f"Unsupported data format: {self.config.data_format}")

        return result

    async def _process_csv_files(
        self,
        files: List[Path],
        output_dir: Path
    ) -> ProcessingResult:
        """Process CSV files.

        Args:
            files: CSV files to process
            output_dir: Output directory

        Returns:
            Processing result
        """
        result = ProcessingResult()

        for file_path in files:
            try:
                self.logger.log(
                    LogLevel.INFO,
                    f"Processing CSV file: {file_path}",
                    "csv_processing_started",
                    file_path=str(file_path)
                )

                # Read CSV file in chunks
                chunk_iter = pd.read_csv(
                    file_path,
                    delimiter=self.config.delimiter,
                    encoding=self.config.encoding,
                    chunksize=self.config.chunk_size,
                    header=0 if self.config.skip_header else None
                )

                chunk_number = 0
                for chunk in chunk_iter:
                    chunk_number += 1

                    # Apply column mapping
                    if self.config.column_mapping:
                        chunk = chunk.rename(columns=self.config.column_mapping)

                    # Validate data
                    if self.config.enable_validation:
                        validation_result = await self._validate_chunk(chunk)
                        result.records_valid += validation_result["valid"]
                        result.records_invalid += validation_result["invalid"]

                    # Write chunk to output
                    output_file = output_dir / f"{file_path.stem}_chunk_{chunk_number:04d}.parquet"
                    chunk.to_parquet(output_file, compression=self.config.compression)

                    result.output_files.append(str(output_file))
                    result.records_processed += len(chunk)

                result.files_processed += 1

                self.logger.log(
                    LogLevel.INFO,
                    f"Completed processing CSV file: {file_path}",
                    "csv_processing_completed",
                    file_path=str(file_path),
                    records_processed=result.records_processed,
                    output_files=len(result.output_files)
                )

            except Exception as e:
                error_msg = f"Failed to process CSV file {file_path}: {e}"
                result.errors.append(error_msg)

                self.logger.log(
                    LogLevel.ERROR,
                    error_msg,
                    "csv_processing_failed",
                    file_path=str(file_path),
                    error=str(e)
                )

        result.success = len(result.errors) == 0
        return result

    async def _process_json_files(
        self,
        files: List[Path],
        output_dir: Path
    ) -> ProcessingResult:
        """Process JSON files.

        Args:
            files: JSON files to process
            output_dir: Output directory

        Returns:
            Processing result
        """
        result = ProcessingResult()

        for file_path in files:
            try:
                self.logger.log(
                    LogLevel.INFO,
                    f"Processing JSON file: {file_path}",
                    "json_processing_started",
                    file_path=str(file_path)
                )

                # Read JSON file
                with open(file_path, 'r', encoding=self.config.encoding) as f:
                    if file_path.suffix == '.jsonl':
                        # JSON Lines format
                        lines = f.readlines()
                        data = [json.loads(line.strip()) for line in lines if line.strip()]
                        df = pd.DataFrame(data)
                    else:
                        # Regular JSON
                        data = json.load(f)
                        if isinstance(data, list):
                            df = pd.DataFrame(data)
                        else:
                            df = pd.json_normalize(data)

                # Apply column mapping
                if self.config.column_mapping:
                    df = df.rename(columns=self.config.column_mapping)

                # Validate data
                if self.config.enable_validation:
                    validation_result = await self._validate_chunk(df)
                    result.records_valid += validation_result["valid"]
                    result.records_invalid += validation_result["invalid"]

                # Write to output
                output_file = output_dir / f"{file_path.stem}.parquet"
                df.to_parquet(output_file, compression=self.config.compression)

                result.output_files.append(str(output_file))
                result.records_processed += len(df)
                result.files_processed += 1

                self.logger.log(
                    LogLevel.INFO,
                    f"Completed processing JSON file: {file_path}",
                    "json_processing_completed",
                    file_path=str(file_path),
                    records_processed=len(df)
                )

            except Exception as e:
                error_msg = f"Failed to process JSON file {file_path}: {e}"
                result.errors.append(error_msg)

                self.logger.log(
                    LogLevel.ERROR,
                    error_msg,
                    "json_processing_failed",
                    file_path=str(file_path),
                    error=str(e)
                )

        result.success = len(result.errors) == 0
        return result

    async def _process_parquet_files(
        self,
        files: List[Path],
        output_dir: Path
    ) -> ProcessingResult:
        """Process Parquet files.

        Args:
            files: Parquet files to process
            output_dir: Output directory

        Returns:
            Processing result
        """
        result = ProcessingResult()

        for file_path in files:
            try:
                self.logger.log(
                    LogLevel.INFO,
                    f"Processing Parquet file: {file_path}",
                    "parquet_processing_started",
                    file_path=str(file_path)
                )

                # Read Parquet file
                df = pd.read_parquet(file_path)

                # Apply column mapping
                if self.config.column_mapping:
                    df = df.rename(columns=self.config.column_mapping)

                # Validate data
                if self.config.enable_validation:
                    validation_result = await self._validate_chunk(df)
                    result.records_valid += validation_result["valid"]
                    result.records_invalid += validation_result["invalid"]

                # Write to output (repartition if needed)
                output_file = output_dir / f"{file_path.stem}_processed.parquet"
                df.to_parquet(output_file, compression=self.config.compression)

                result.output_files.append(str(output_file))
                result.records_processed += len(df)
                result.files_processed += 1

                self.logger.log(
                    LogLevel.INFO,
                    f"Completed processing Parquet file: {file_path}",
                    "parquet_processing_completed",
                    file_path=str(file_path),
                    records_processed=len(df)
                )

            except Exception as e:
                error_msg = f"Failed to process Parquet file {file_path}: {e}"
                result.errors.append(error_msg)

                self.logger.log(
                    LogLevel.ERROR,
                    error_msg,
                    "parquet_processing_failed",
                    file_path=str(file_path),
                    error=str(e)
                )

        result.success = len(result.errors) == 0
        return result

    async def _validate_chunk(self, chunk: pd.DataFrame) -> Dict[str, int]:
        """Validate a data chunk.

        Args:
            chunk: Data chunk to validate

        Returns:
            Validation results with valid/invalid counts
        """
        validation_result = {
            "valid": len(chunk),
            "invalid": 0
        }

        if not self.config.enable_validation:
            return validation_result

        # Apply validation rules
        for rule_name, rule_config in self.config.validation_rules.items():
            try:
                if rule_name == "not_empty":
                    # Check for non-empty values in specified columns
                    columns = rule_config.get("columns", [])
                    for col in columns:
                        if col in chunk.columns:
                            empty_count = chunk[col].isna().sum()
                            validation_result["invalid"] += empty_count

                elif rule_name == "range":
                    # Check value ranges
                    column = rule_config.get("column")
                    min_val = rule_config.get("min")
                    max_val = rule_config.get("max")

                    if column and column in chunk.columns:
                        if chunk[column].dtype in ['int64', 'float64']:
                            out_of_range = ((chunk[column] < min_val) | (chunk[column] > max_val)).sum()
                            validation_result["invalid"] += out_of_range

                elif rule_name == "pattern":
                    # Check regex patterns
                    column = rule_config.get("column")
                    pattern = rule_config.get("pattern")

                    if column and column in chunk.columns and pattern:
                        import re
                        invalid_matches = ~chunk[column].astype(str).str.contains(pattern, na=False)
                        validation_result["invalid"] += invalid_matches.sum()

            except Exception as e:
                self.logger.log(
                    LogLevel.WARNING,
                    f"Validation rule {rule_name} failed: {e}",
                    "validation_rule_failed",
                    rule_name=rule_name,
                    error=str(e)
                )

        validation_result["valid"] = max(0, validation_result["valid"] - validation_result["invalid"])

        return validation_result

    async def cleanup_temp_files(self, temp_dir: Path, older_than_hours: int = 24) -> int:
        """Clean up temporary processing files.

        Args:
            temp_dir: Temporary directory to clean
            older_than_hours: Remove files older than this many hours

        Returns:
            Number of files cleaned up
        """
        from datetime import timedelta
        import time

        cutoff_time = time.time() - (older_than_hours * 3600)
        cleaned_count = 0

        for file_path in temp_dir.rglob('*'):
            if file_path.is_file() and file_path.stat().st_mtime < cutoff_time:
                try:
                    file_path.unlink()
                    cleaned_count += 1
                except Exception as e:
                    self.logger.log(
                        LogLevel.WARNING,
                        f"Failed to clean up temp file {file_path}: {e}",
                        "temp_file_cleanup_failed",
                        file_path=str(file_path),
                        error=str(e)
                    )

        if cleaned_count > 0:
            self.logger.log(
                LogLevel.INFO,
                f"Cleaned up {cleaned_count} temporary processing files",
                "temp_files_cleanup_completed",
                temp_dir=str(temp_dir),
                cleaned_count=cleaned_count,
                older_than_hours=older_than_hours
            )

        return cleaned_count

    def generate_processing_report(
        self,
        result: ProcessingResult,
        input_dir: Path,
        output_dir: Path
    ) -> Dict[str, Any]:
        """Generate a processing report.

        Args:
            result: Processing result
            input_dir: Input directory
            output_dir: Output directory

        Returns:
            Processing report
        """
        report = {
            "processing_summary": {
                "source_name": self.config.source_name,
                "input_directory": str(input_dir),
                "output_directory": str(output_dir),
                "data_format": self.config.data_format,
                "success": result.success,
                "files_processed": result.files_processed,
                "records_processed": result.records_processed,
                "processing_time_seconds": result.processing_time_seconds,
                "average_records_per_second": (
                    result.records_processed / result.processing_time_seconds
                    if result.processing_time_seconds > 0 else 0
                )
            },
            "data_quality": {
                "records_valid": result.records_valid,
                "records_invalid": result.records_invalid,
                "quality_score": (
                    result.records_valid / max(result.records_processed, 1)
                ),
                "quality_threshold_met": (
                    result.records_valid / max(result.records_processed, 1)
                ) >= self.config.quality_threshold
            },
            "output": {
                "output_files": result.output_files,
                "total_output_files": len(result.output_files),
                "output_format": self.config.output_format
            },
            "errors": result.errors if result.errors else None,
            "metadata": result.metadata
        }

        return report
