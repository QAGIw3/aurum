"""Bulk archive manager for downloading and processing bulk data files."""

from __future__ import annotations

import asyncio
import hashlib
import json
import zipfile
import tempfile
import shutil
from pathlib import Path
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Callable
from enum import Enum
import aiohttp
import aiofiles
from urllib.parse import urlparse

from ..logging import StructuredLogger, LogLevel, create_logger


class ArchiveStatus(str, Enum):
    """Status of bulk archive operations."""

    PENDING = "pending"
    DOWNLOADING = "downloading"
    DOWNLOADED = "downloaded"
    VERIFYING = "verifying"
    VERIFIED = "verified"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class DownloadResult:
    """Result of a download operation."""

    success: bool
    file_path: Optional[Path] = None
    file_size: int = 0
    download_time_seconds: float = 0.0
    checksum: Optional[str] = None
    error_message: Optional[str] = None
    http_status: int = 200
    content_type: Optional[str] = None


@dataclass
class VerificationResult:
    """Result of a verification operation."""

    success: bool
    checksum_match: bool = False
    expected_checksum: Optional[str] = None
    actual_checksum: Optional[str] = None
    file_size_match: bool = False
    expected_size: Optional[int] = None
    actual_size: int = 0
    error_message: Optional[str] = None


@dataclass
class ArchiveConfig:
    """Configuration for bulk archive operations."""

    source_name: str
    url: str
    download_dir: Path
    temp_dir: Path
    max_concurrent_downloads: int = 3
    download_timeout_seconds: int = 300
    retry_attempts: int = 3
    retry_delay_seconds: int = 5
    chunk_size: int = 8192
    enable_checksum_verification: bool = True
    enable_size_verification: bool = True
    expected_checksums: Dict[str, str] = field(default_factory=dict)
    expected_file_sizes: Dict[str, int] = field(default_factory=dict)
    user_agent: str = "Aurum-Bulk-Archive-Manager/1.0"

    # Processing configuration
    extract_archives: bool = True
    delete_archives_after_processing: bool = False
    preserve_directory_structure: bool = True

    # Monitoring configuration
    enable_progress_reporting: bool = True
    progress_report_interval_seconds: int = 30

    # Rate limiting
    requests_per_second: float = 1.0
    burst_limit: int = 5


class BulkArchiveManager:
    """Manager for bulk archive download and processing operations."""

    def __init__(self, config: ArchiveConfig):
        """Initialize bulk archive manager.

        Args:
            config: Archive configuration
        """
        self.config = config
        self.logger = create_logger(
            source_name="bulk_archive_manager",
            kafka_bootstrap_servers=None,
            kafka_topic="aurum.bulk_archive.operations",
            dataset="bulk_archive_operations"
        )

        # Create directories
        self.config.download_dir.mkdir(parents=True, exist_ok=True)
        self.config.temp_dir.mkdir(parents=True, exist_ok=True)

        # State tracking
        self.active_downloads: Dict[str, DownloadResult] = {}
        self.download_history: List[DownloadResult] = []
        self.processing_queue: asyncio.Queue = asyncio.Queue()

        # Session management
        self._http_session: Optional[aiohttp.ClientSession] = None

        self.logger.log(
            LogLevel.INFO,
            "Initialized bulk archive manager",
            "bulk_archive_manager_initialized",
            source_name=config.source_name,
            download_dir=str(config.download_dir),
            max_concurrent_downloads=config.max_concurrent_downloads
        )

    async def __aenter__(self):
        """Async context manager entry."""
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.stop()

    async def start(self) -> None:
        """Start the bulk archive manager."""
        self.logger.log(
            LogLevel.INFO,
            "Starting bulk archive manager",
            "bulk_archive_manager_starting",
            source_name=self.config.source_name
        )

        # Create HTTP session with optimized settings
        timeout = aiohttp.ClientTimeout(total=self.config.download_timeout_seconds)
        connector = aiohttp.TCPConnector(
            limit=self.config.max_concurrent_downloads,
            limit_per_host=self.config.max_concurrent_downloads,
            ttl_dns_cache=300,
            use_dns_cache=True
        )

        self._http_session = aiohttp.ClientSession(
            timeout=timeout,
            connector=connector,
            headers={"User-Agent": self.config.user_agent}
        )

    async def stop(self) -> None:
        """Stop the bulk archive manager."""
        self.logger.log(
            LogLevel.INFO,
            "Stopping bulk archive manager",
            "bulk_archive_manager_stopping",
            source_name=self.config.source_name
        )

        # Cancel any active downloads
        for download_id, result in self.active_downloads.items():
            if result.success is None:  # Still in progress
                result.success = False
                result.error_message = "Cancelled during shutdown"

        # Close HTTP session
        if self._http_session:
            await self._http_session.close()

    async def download_archive(
        self,
        url: str,
        filename: Optional[str] = None,
        checksum: Optional[str] = None,
        expected_size: Optional[int] = None
    ) -> DownloadResult:
        """Download a bulk archive file.

        Args:
            url: URL to download from
            filename: Optional filename for the download
            checksum: Optional expected checksum for verification
            expected_size: Optional expected file size

        Returns:
            Download result
        """
        download_id = f"{self.config.source_name}_{int(datetime.now().timestamp())}"

        # Determine filename
        if not filename:
            parsed_url = urlparse(url)
            filename = Path(parsed_url.path).name or f"archive_{download_id}.zip"

        file_path = self.config.download_dir / filename

        # Check if file already exists
        if file_path.exists():
            self.logger.log(
                LogLevel.INFO,
                f"File already exists: {file_path}",
                "archive_file_exists",
                source_name=self.config.source_name,
                file_path=str(file_path)
            )

            # Verify existing file
            verification = await self.verify_file(file_path, checksum, expected_size)
            if verification.success:
                return DownloadResult(
                    success=True,
                    file_path=file_path,
                    file_size=file_path.stat().st_size,
                    download_time_seconds=0.0,
                    checksum=verification.actual_checksum
                )

        # Create download result
        result = DownloadResult(
            success=None,  # In progress
            file_path=file_path,
            checksum=checksum
        )

        self.active_downloads[download_id] = result

        try:
            start_time = datetime.now()

            # Download with retry logic
            for attempt in range(self.config.retry_attempts):
                try:
                    self.logger.log(
                        LogLevel.INFO,
                        f"Downloading {url} (attempt {attempt + 1}/{self.config.retry_attempts})",
                        "archive_download_started",
                        source_name=self.config.source_name,
                        url=url,
                        attempt=attempt + 1,
                        max_attempts=self.config.retry_attempts
                    )

                    await self._download_with_progress(url, file_path)

                    # Verify download
                    verification = await self.verify_file(file_path, checksum, expected_size)

                    if verification.success:
                        result.success = True
                        result.file_size = file_path.stat().st_size
                        result.download_time_seconds = (datetime.now() - start_time).total_seconds()
                        result.checksum = verification.actual_checksum

                        self.logger.log(
                            LogLevel.INFO,
                            f"Successfully downloaded {file_path}",
                            "archive_download_completed",
                            source_name=self.config.source_name,
                            file_path=str(file_path),
                            file_size=result.file_size,
                            download_time_seconds=result.download_time_seconds
                        )

                        self.download_history.append(result)
                        return result
                    else:
                        result.error_message = verification.error_message
                        raise Exception(f"Verification failed: {verification.error_message}")

                except Exception as e:
                    result.error_message = str(e)

                    if attempt < self.config.retry_attempts - 1:
                        delay = self.config.retry_delay_seconds * (2 ** attempt)  # Exponential backoff
                        self.logger.log(
                            LogLevel.WARNING,
                            f"Download attempt {attempt + 1} failed, retrying in {delay}s: {e}",
                            "archive_download_retry",
                            source_name=self.config.source_name,
                            attempt=attempt + 1,
                            delay_seconds=delay,
                            error=str(e)
                        )
                        await asyncio.sleep(delay)
                    else:
                        raise

        except Exception as e:
            result.success = False
            result.error_message = str(e)
            result.download_time_seconds = (datetime.now() - start_time).total_seconds()

            self.logger.log(
                LogLevel.ERROR,
                f"Failed to download {url}: {e}",
                "archive_download_failed",
                source_name=self.config.source_name,
                url=url,
                error=str(e)
            )

        finally:
            if download_id in self.active_downloads:
                del self.active_downloads[download_id]

        return result

    async def _download_with_progress(self, url: str, file_path: Path) -> None:
        """Download file with progress reporting.

        Args:
            url: URL to download from
            file_path: Path to save the file
        """
        async with self._http_session.get(url) as response:
            if response.status != 200:
                raise Exception(f"HTTP {response.status}: {response.reason}")

            result = self.active_downloads.get(
                next((k for k, v in self.active_downloads.items() if v.file_path == file_path), None)
            )

            if not result:
                raise Exception("Download result not found")

            result.http_status = response.status
            result.content_type = response.headers.get('content-type', '')

            # Get file size for progress tracking
            content_length = response.headers.get('content-length')
            if content_length:
                total_size = int(content_length)
            else:
                total_size = 0

            # Download with progress
            downloaded = 0
            async with aiofiles.open(file_path, 'wb') as f:
                async for chunk in response.content.iter_chunked(self.config.chunk_size):
                    await f.write(chunk)
                    downloaded += len(chunk)

                    # Progress reporting
                    if (self.config.enable_progress_reporting and
                        total_size > 0 and
                        downloaded % (total_size // 20) == 0):  # Every 5%
                        progress = (downloaded / total_size) * 100
                        self.logger.log(
                            LogLevel.DEBUG,
                            f"Download progress: {progress".1f"}% ({downloaded}/{total_size} bytes)",
                            "archive_download_progress",
                            source_name=self.config.source_name,
                            progress_percent=progress,
                            downloaded_bytes=downloaded,
                            total_bytes=total_size
                        )

    async def verify_file(
        self,
        file_path: Path,
        expected_checksum: Optional[str] = None,
        expected_size: Optional[int] = None
    ) -> VerificationResult:
        """Verify downloaded file integrity.

        Args:
            file_path: Path to file to verify
            expected_checksum: Expected checksum
            expected_size: Expected file size

        Returns:
            Verification result
        """
        if not file_path.exists():
            return VerificationResult(
                success=False,
                error_message="File does not exist"
            )

        actual_size = file_path.stat().st_size

        # Size verification
        size_match = True
        if expected_size is not None:
            size_match = actual_size == expected_size

        # Checksum verification
        checksum_match = True
        actual_checksum = None

        if expected_checksum and self.config.enable_checksum_verification:
            try:
                actual_checksum = await self._calculate_checksum(file_path)
                checksum_match = actual_checksum == expected_checksum
            except Exception as e:
                return VerificationResult(
                    success=False,
                    checksum_match=False,
                    expected_checksum=expected_checksum,
                    actual_checksum=actual_checksum,
                    file_size_match=size_match,
                    expected_size=expected_size,
                    actual_size=actual_size,
                    error_message=f"Checksum calculation failed: {e}"
                )

        success = size_match and checksum_match

        return VerificationResult(
            success=success,
            checksum_match=checksum_match,
            expected_checksum=expected_checksum,
            actual_checksum=actual_checksum,
            file_size_match=size_match,
            expected_size=expected_size,
            actual_size=actual_size,
            error_message=None if success else "Verification failed"
        )

    async def _calculate_checksum(self, file_path: Path) -> str:
        """Calculate SHA-256 checksum of a file.

        Args:
            file_path: Path to file

        Returns:
            SHA-256 checksum as hex string
        """
        hash_sha256 = hashlib.sha256()

        async with aiofiles.open(file_path, 'rb') as f:
            while True:
                chunk = await f.read(self.config.chunk_size)
                if not chunk:
                    break
                hash_sha256.update(chunk)

        return hash_sha256.hexdigest()

    async def extract_archive(self, archive_path: Path, extract_to: Optional[Path] = None) -> Path:
        """Extract archive file.

        Args:
            archive_path: Path to archive file
            extract_to: Directory to extract to (default: temp directory)

        Returns:
            Path to extracted directory
        """
        if not extract_to:
            extract_to = self.config.temp_dir / archive_path.stem

        extract_to.mkdir(parents=True, exist_ok=True)

        self.logger.log(
            LogLevel.INFO,
            f"Extracting archive {archive_path} to {extract_to}",
            "archive_extraction_started",
            source_name=self.config.source_name,
            archive_path=str(archive_path),
            extract_to=str(extract_to)
        )

        try:
            with zipfile.ZipFile(archive_path, 'r') as zip_ref:
                # Extract all files
                zip_ref.extractall(extract_to)

                self.logger.log(
                    LogLevel.INFO,
                    f"Successfully extracted {len(zip_ref.namelist())} files from {archive_path}",
                    "archive_extraction_completed",
                    source_name=self.config.source_name,
                    archive_path=str(archive_path),
                    extracted_files=len(zip_ref.namelist()),
                    extract_to=str(extract_to)
                )

            return extract_to

        except Exception as e:
            self.logger.log(
                LogLevel.ERROR,
                f"Failed to extract archive {archive_path}: {e}",
                "archive_extraction_failed",
                source_name=self.config.source_name,
                archive_path=str(archive_path),
                error=str(e)
            )
            raise

    async def cleanup_temp_files(self, older_than_hours: int = 24) -> int:
        """Clean up temporary files older than specified hours.

        Args:
            older_than_hours: Remove files older than this many hours

        Returns:
            Number of files cleaned up
        """
        cutoff_time = datetime.now() - timedelta(hours=older_than_hours)
        cleaned_count = 0

        # Clean temp directory
        for file_path in self.config.temp_dir.rglob('*'):
            if file_path.is_file() and file_path.stat().st_mtime < cutoff_time.timestamp():
                file_path.unlink()
                cleaned_count += 1

        # Clean download directory (archives marked for deletion)
        for file_path in self.config.download_dir.rglob('*.zip'):
            if (self.config.delete_archives_after_processing and
                file_path.is_file() and
                file_path.stat().st_mtime < cutoff_time.timestamp()):
                file_path.unlink()
                cleaned_count += 1

        if cleaned_count > 0:
            self.logger.log(
                LogLevel.INFO,
                f"Cleaned up {cleaned_count} temporary/archive files",
                "temp_files_cleanup",
                source_name=self.config.source_name,
                cleaned_count=cleaned_count,
                older_than_hours=older_than_hours
            )

        return cleaned_count

    def get_download_history(self, limit: Optional[int] = None) -> List[DownloadResult]:
        """Get download history.

        Args:
            limit: Maximum number of results to return

        Returns:
            List of download results
        """
        history = self.download_history.copy()

        if limit:
            history = history[-limit:]

        return history

    def get_active_downloads(self) -> Dict[str, DownloadResult]:
        """Get currently active downloads.

        Returns:
            Dictionary of active downloads
        """
        return self.active_downloads.copy()

    async def process_bulk_datasets(self, datasets_config: Dict[str, Any]) -> Dict[str, Any]:
        """Process bulk datasets from configuration.

        Args:
            datasets_config: Configuration for datasets to process

        Returns:
            Processing results
        """
        results = {
            "total_datasets": len(datasets_config),
            "successful_downloads": 0,
            "successful_verifications": 0,
            "successful_processing": 0,
            "total_errors": 0,
            "datasets": {}
        }

        for dataset_name, dataset_config in datasets_config.items():
            try:
                self.logger.log(
                    LogLevel.INFO,
                    f"Processing bulk dataset: {dataset_name}",
                    "bulk_dataset_processing_started",
                    source_name=self.config.source_name,
                    dataset_name=dataset_name
                )

                # Download dataset
                url = dataset_config["url"]
                checksum = dataset_config.get("checksum")
                expected_size = dataset_config.get("expected_size")

                download_result = await self.download_archive(
                    url=url,
                    checksum=checksum,
                    expected_size=expected_size
                )

                results["datasets"][dataset_name] = {
                    "download": download_result.__dict__,
                    "processing": None,
                    "verification": None
                }

                if download_result.success:
                    results["successful_downloads"] += 1

                    # Verify download
                    verification = await self.verify_file(
                        download_result.file_path,
                        checksum,
                        expected_size
                    )

                    results["datasets"][dataset_name]["verification"] = verification.__dict__

                    if verification.success:
                        results["successful_verifications"] += 1

                        # Extract and process
                        extracted_path = await self.extract_archive(download_result.file_path)
                        processing_result = await self._process_extracted_data(
                            extracted_path, dataset_config
                        )

                        results["datasets"][dataset_name]["processing"] = processing_result

                        if processing_result.get("success", False):
                            results["successful_processing"] += 1

                else:
                    results["total_errors"] += 1

            except Exception as e:
                self.logger.log(
                    LogLevel.ERROR,
                    f"Failed to process dataset {dataset_name}: {e}",
                    "bulk_dataset_processing_failed",
                    source_name=self.config.source_name,
                    dataset_name=dataset_name,
                    error=str(e)
                )

                results["total_errors"] += 1
                results["datasets"][dataset_name] = {
                    "error": str(e)
                }

        return results

    async def _process_extracted_data(self, extracted_path: Path, dataset_config: Dict[str, Any]) -> Dict[str, Any]:
        """Process extracted bulk data.

        Args:
            extracted_path: Path to extracted data
            dataset_config: Dataset configuration

        Returns:
            Processing results
        """
        # This would integrate with SeaTunnel or other processing pipelines
        # For now, return basic processing result

        files_processed = list(extracted_path.rglob('*'))
        total_size = sum(f.stat().st_size for f in files_processed if f.is_file())

        return {
            "success": True,
            "files_processed": len(files_processed),
            "total_size_bytes": total_size,
            "extracted_path": str(extracted_path),
            "processing_time_seconds": 1.0  # Placeholder
        }
