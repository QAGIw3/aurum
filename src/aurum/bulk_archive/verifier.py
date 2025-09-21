"""File verification utilities for bulk archive integrity checking."""

from __future__ import annotations

import hashlib
import asyncio
from pathlib import Path
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass
from datetime import datetime
import aiofiles

from ..logging import create_logger, LogLevel


class VerificationError(Exception):
    """Base exception for verification errors."""
    pass


class ChecksumMismatchError(VerificationError):
    """Exception raised when checksum verification fails."""
    pass


class FileNotFoundError(VerificationError):
    """Exception raised when file is not found for verification."""
    pass


@dataclass
class FileVerificationResult:
    """Result of file verification operation."""

    file_path: Path
    checksum_verified: bool = False
    size_verified: bool = False
    checksum_expected: Optional[str] = None
    checksum_actual: Optional[str] = None
    size_expected: Optional[int] = None
    size_actual: int = 0
    verification_time_seconds: float = 0.0
    error_message: Optional[str] = None
    verified_at: datetime = field(default_factory=datetime.now)


class ChecksumVerifier:
    """Verifier for file checksums and integrity."""

    def __init__(self, algorithm: str = "sha256"):
        """Initialize checksum verifier.

        Args:
            algorithm: Hash algorithm to use (sha256, sha1, md5)
        """
        self.algorithm = algorithm
        self.logger = create_logger(
            source_name="checksum_verifier",
            kafka_bootstrap_servers=None,
            kafka_topic="aurum.bulk_archive.verification",
            dataset="file_verification"
        )

    async def verify_file(
        self,
        file_path: Path,
        expected_checksum: Optional[str] = None,
        expected_size: Optional[int] = None
    ) -> FileVerificationResult:
        """Verify a single file's integrity.

        Args:
            file_path: Path to file to verify
            expected_checksum: Expected checksum
            expected_size: Expected file size

        Returns:
            Verification result
        """
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        start_time = datetime.now()

        try:
            # Calculate actual checksum
            actual_checksum = await self._calculate_checksum(file_path)
            actual_size = file_path.stat().st_size

            # Verify checksum
            checksum_verified = True
            if expected_checksum:
                checksum_verified = actual_checksum == expected_checksum

            # Verify size
            size_verified = True
            if expected_size:
                size_verified = actual_size == expected_size

            verification_time = (datetime.now() - start_time).total_seconds()

            result = FileVerificationResult(
                file_path=file_path,
                checksum_verified=checksum_verified,
                size_verified=size_verified,
                checksum_expected=expected_checksum,
                checksum_actual=actual_checksum,
                size_expected=expected_size,
                size_actual=actual_size,
                verification_time_seconds=verification_time
            )

            # Log verification result
            if checksum_verified and size_verified:
                self.logger.log(
                    LogLevel.INFO,
                    f"File verification passed: {file_path}",
                    "file_verification_passed",
                    file_path=str(file_path),
                    checksum=actual_checksum,
                    size_bytes=actual_size,
                    verification_time_seconds=verification_time
                )
            else:
                error_msg = "Verification failed"
                if not checksum_verified:
                    error_msg += f" (checksum mismatch: expected={expected_checksum}, actual={actual_checksum})"
                if not size_verified:
                    error_msg += f" (size mismatch: expected={expected_size}, actual={actual_size})"

                result.error_message = error_msg

                self.logger.log(
                    LogLevel.WARNING,
                    f"File verification failed: {file_path}",
                    "file_verification_failed",
                    file_path=str(file_path),
                    checksum_verified=checksum_verified,
                    size_verified=size_verified,
                    error_message=error_msg
                )

            return result

        except Exception as e:
            verification_time = (datetime.now() - start_time).total_seconds()

            self.logger.log(
                LogLevel.ERROR,
                f"File verification error for {file_path}: {e}",
                "file_verification_error",
                file_path=str(file_path),
                error=str(e),
                verification_time_seconds=verification_time
            )

            raise VerificationError(f"Verification failed for {file_path}: {e}") from e

    async def verify_files_batch(
        self,
        files: List[Path],
        expected_checksums: Dict[str, str],
        expected_sizes: Optional[Dict[str, int]] = None
    ) -> Dict[str, FileVerificationResult]:
        """Verify multiple files in batch.

        Args:
            files: List of file paths to verify
            expected_checksums: Dictionary mapping file paths to expected checksums
            expected_sizes: Optional dictionary mapping file paths to expected sizes

        Returns:
            Dictionary mapping file paths to verification results
        """
        results = {}

        self.logger.log(
            LogLevel.INFO,
            f"Starting batch verification of {len(files)} files",
            "batch_verification_started",
            file_count=len(files)
        )

        # Create verification tasks
        tasks = []
        for file_path in files:
            checksum = expected_checksums.get(str(file_path))
            size = expected_sizes.get(str(file_path)) if expected_sizes else None

            task = self.verify_file(file_path, checksum, size)
            tasks.append(task)

        # Execute verification tasks concurrently
        completed_tasks = await asyncio.gather(*tasks, return_exceptions=True)

        # Process results
        for file_path, result in zip(files, completed_tasks):
            if isinstance(result, Exception):
                # Handle verification error
                results[str(file_path)] = FileVerificationResult(
                    file_path=file_path,
                    error_message=str(result)
                )
            else:
                results[str(file_path)] = result

        # Log summary
        successful = len([r for r in results.values() if r.checksum_verified and r.size_verified])
        failed = len(results) - successful

        self.logger.log(
            LogLevel.INFO,
            f"Batch verification completed: {successful}/{len(files)} successful, {failed} failed",
            "batch_verification_completed",
            total_files=len(files),
            successful_files=successful,
            failed_files=failed
        )

        return results

    async def _calculate_checksum(self, file_path: Path) -> str:
        """Calculate checksum of a file using specified algorithm.

        Args:
            file_path: Path to file

        Returns:
            Checksum as hexadecimal string
        """
        hash_func = getattr(hashlib, self.algorithm)()

        async with aiofiles.open(file_path, 'rb') as f:
            # Read file in chunks to handle large files
            while True:
                chunk = await f.read(8192)
                if not chunk:
                    break
                hash_func.update(chunk)

        return hash_func.hexdigest()

    def verify_checksum_string(self, checksum: str) -> bool:
        """Verify that a checksum string is valid for the algorithm.

        Args:
            checksum: Checksum string to validate

        Returns:
            True if valid, False otherwise
        """
        try:
            # Check length (SHA-256 = 64 chars, SHA-1 = 40 chars, MD5 = 32 chars)
            expected_length = {
                'sha256': 64,
                'sha1': 40,
                'md5': 32
            }.get(self.algorithm, 64)

            if len(checksum) != expected_length:
                return False

            # Check if all characters are valid hex digits
            return all(c in '0123456789abcdefABCDEF' for c in checksum)

        except Exception:
            return False

    def calculate_checksum_from_bytes(self, data: bytes) -> str:
        """Calculate checksum from bytes data.

        Args:
            data: Bytes to calculate checksum for

        Returns:
            Checksum as hexadecimal string
        """
        hash_func = getattr(hashlib, self.algorithm)()
        hash_func.update(data)
        return hash_func.hexdigest()

    def calculate_checksum_from_string(self, data: str, encoding: str = 'utf-8') -> str:
        """Calculate checksum from string data.

        Args:
            data: String to calculate checksum for
            encoding: String encoding

        Returns:
            Checksum as hexadecimal string
        """
        return self.calculate_checksum_from_bytes(data.encode(encoding))


class BulkVerificationManager:
    """Manager for bulk file verification operations."""

    def __init__(self, verifier: Optional[ChecksumVerifier] = None):
        """Initialize bulk verification manager.

        Args:
            verifier: Checksum verifier instance
        """
        self.verifier = verifier or ChecksumVerifier()
        self.logger = create_logger(
            source_name="bulk_verification_manager",
            kafka_bootstrap_servers=None,
            kafka_topic="aurum.bulk_archive.verification",
            dataset="bulk_verification"
        )

    async def verify_download_integrity(
        self,
        download_path: Path,
        manifest_path: Optional[Path] = None
    ) -> Dict[str, Any]:
        """Verify integrity of downloaded files.

        Args:
            download_path: Path to downloaded files
            manifest_path: Optional path to manifest file with expected checksums

        Returns:
            Verification results
        """
        self.logger.log(
            LogLevel.INFO,
            f"Starting bulk verification for {download_path}",
            "bulk_verification_started",
            download_path=str(download_path)
        )

        # Find all files to verify
        if download_path.is_file():
            files_to_verify = [download_path]
        else:
            files_to_verify = list(download_path.rglob('*'))
            files_to_verify = [f for f in files_to_verify if f.is_file()]

        # Load manifest if provided
        expected_checksums = {}
        expected_sizes = {}

        if manifest_path and manifest_path.exists():
            manifest_data = await self._load_manifest(manifest_path)
            expected_checksums = manifest_data.get('checksums', {})
            expected_sizes = manifest_data.get('sizes', {})

        # Perform verification
        verification_results = await self.verifier.verify_files_batch(
            files_to_verify,
            expected_checksums,
            expected_sizes
        )

        # Generate summary
        total_files = len(verification_results)
        successful_files = len([
            r for r in verification_results.values()
            if r.checksum_verified and r.size_verified
        ])
        failed_files = total_files - successful_files

        summary = {
            "verification_path": str(download_path),
            "total_files": total_files,
            "successful_files": successful_files,
            "failed_files": failed_files,
            "success_rate": successful_files / total_files if total_files > 0 else 0,
            "results": {
                str(file_path): result.__dict__
                for file_path, result in verification_results.items()
            }
        }

        self.logger.log(
            LogLevel.INFO,
            f"Bulk verification completed: {successful_files}/{total_files} successful",
            "bulk_verification_completed",
            total_files=total_files,
            successful_files=successful_files,
            failed_files=failed_files,
            success_rate=summary["success_rate"]
        )

        return summary

    async def _load_manifest(self, manifest_path: Path) -> Dict[str, Any]:
        """Load verification manifest file.

        Args:
            manifest_path: Path to manifest file

        Returns:
            Manifest data
        """
        try:
            async with aiofiles.open(manifest_path, 'r') as f:
                content = await f.read()
                return json.loads(content)
        except Exception as e:
            self.logger.log(
                LogLevel.WARNING,
                f"Failed to load manifest {manifest_path}: {e}",
                "manifest_load_failed",
                manifest_path=str(manifest_path),
                error=str(e)
            )
            return {}

    async def generate_manifest(
        self,
        directory_path: Path,
        output_path: Path,
        include_hidden: bool = False
    ) -> Dict[str, Any]:
        """Generate verification manifest for a directory.

        Args:
            directory_path: Directory to generate manifest for
            output_path: Path to save manifest file
            include_hidden: Whether to include hidden files

        Returns:
            Manifest data
        """
        self.logger.log(
            LogLevel.INFO,
            f"Generating manifest for {directory_path}",
            "manifest_generation_started",
            directory_path=str(directory_path)
        )

        # Find all files
        if directory_path.is_file():
            files = [directory_path]
        else:
            pattern = '**/*' if include_hidden else '*'
            files = list(directory_path.glob(pattern))
            files = [f for f in files if f.is_file()]

        # Calculate checksums and sizes
        manifest_data = {
            "generated_at": datetime.now().isoformat(),
            "directory_path": str(directory_path),
            "algorithm": self.verifier.algorithm,
            "total_files": len(files),
            "checksums": {},
            "sizes": {}
        }

        for file_path in files:
            try:
                # Calculate checksum
                checksum = await self.verifier._calculate_checksum(file_path)
                size = file_path.stat().st_size

                # Store relative path as key
                rel_path = str(file_path.relative_to(directory_path))
                manifest_data["checksums"][rel_path] = checksum
                manifest_data["sizes"][rel_path] = size

            except Exception as e:
                self.logger.log(
                    LogLevel.WARNING,
                    f"Failed to process file {file_path}: {e}",
                    "manifest_file_processing_failed",
                    file_path=str(file_path),
                    error=str(e)
                )

        # Save manifest file
        try:
            async with aiofiles.open(output_path, 'w') as f:
                await f.write(json.dumps(manifest_data, indent=2))

            self.logger.log(
                LogLevel.INFO,
                f"Manifest generated and saved to {output_path}",
                "manifest_generation_completed",
                manifest_path=str(output_path),
                total_files=manifest_data["total_files"]
            )

        except Exception as e:
            self.logger.log(
                LogLevel.ERROR,
                f"Failed to save manifest to {output_path}: {e}",
                "manifest_save_failed",
                output_path=str(output_path),
                error=str(e)
            )
            raise

        return manifest_data

    async def compare_manifests(
        self,
        manifest1_path: Path,
        manifest2_path: Path
    ) -> Dict[str, Any]:
        """Compare two manifest files.

        Args:
            manifest1_path: Path to first manifest
            manifest2_path: Path to second manifest

        Returns:
            Comparison results
        """
        self.logger.log(
            LogLevel.INFO,
            f"Comparing manifests: {manifest1_path} vs {manifest2_path}",
            "manifest_comparison_started",
            manifest1_path=str(manifest1_path),
            manifest2_path=str(manifest2_path)
        )

        # Load manifests
        manifest1 = await self._load_manifest(manifest1_path)
        manifest2 = await self._load_manifest(manifest2_path)

        # Compare
        comparison = {
            "manifest1_path": str(manifest1_path),
            "manifest2_path": str(manifest2_path),
            "differences": {
                "added_files": [],
                "removed_files": [],
                "modified_files": [],
                "checksum_changes": []
            },
            "summary": {
                "files_in_manifest1": len(manifest1.get("checksums", {})),
                "files_in_manifest2": len(manifest2.get("checksums", {})),
                "files_added": 0,
                "files_removed": 0,
                "files_modified": 0
            }
        }

        # Find differences
        checksums1 = manifest1.get("checksums", {})
        checksums2 = manifest2.get("checksums", {})

        all_files = set(checksums1.keys()) | set(checksums2.keys())

        for file_path in all_files:
            in_manifest1 = file_path in checksums1
            in_manifest2 = file_path in checksums2

            if in_manifest1 and not in_manifest2:
                comparison["differences"]["removed_files"].append(file_path)
                comparison["summary"]["files_removed"] += 1

            elif in_manifest2 and not in_manifest1:
                comparison["differences"]["added_files"].append(file_path)
                comparison["summary"]["files_added"] += 1

            elif in_manifest1 and in_manifest2:
                checksum1 = checksums1[file_path]
                checksum2 = checksums2[file_path]

                if checksum1 != checksum2:
                    comparison["differences"]["modified_files"].append(file_path)
                    comparison["differences"]["checksum_changes"].append({
                        "file_path": file_path,
                        "old_checksum": checksum1,
                        "new_checksum": checksum2
                    })
                    comparison["summary"]["files_modified"] += 1

        self.logger.log(
            LogLevel.INFO,
            f"Manifest comparison completed",
            "manifest_comparison_completed",
            files_added=comparison["summary"]["files_added"],
            files_removed=comparison["summary"]["files_removed"],
            files_modified=comparison["summary"]["files_modified"]
        )

        return comparison
