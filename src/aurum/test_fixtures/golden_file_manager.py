"""Golden file management for test validation."""

from __future__ import annotations

import json
import hashlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Any, Union
from datetime import datetime
from enum import Enum


class GoldenFileStatus(str, Enum):
    """Status of golden file comparison."""

    MATCH = "MATCH"           # Files match exactly
    DIFFERENT = "DIFFERENT"   # Files are different
    MISSING = "MISSING"       # Golden file is missing
    EXTRA = "EXTRA"          # Golden file exists but shouldn't
    ERROR = "ERROR"          # Error during comparison


@dataclass
class GoldenFile:
    """Represents a golden file for test validation."""

    name: str
    path: Path
    checksum: str
    record_count: int
    schema: Dict[str, Any]
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())

    def calculate_checksum(self) -> str:
        """Calculate checksum of the golden file.

        Returns:
            SHA256 checksum as hex string
        """
        if not self.path.exists():
            return ""

        with open(self.path, 'r') as f:
            data = json.load(f)

        data_str = json.dumps(data, sort_keys=True, default=str)
        return hashlib.sha256(data_str.encode()).hexdigest()

    def update_checksum(self) -> None:
        """Update the stored checksum."""
        self.checksum = self.calculate_checksum()

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary.

        Returns:
            Dictionary representation
        """
        return {
            "name": self.name,
            "path": str(self.path),
            "checksum": self.checksum,
            "record_count": self.record_count,
            "schema": self.schema,
            "metadata": self.metadata,
            "created_at": self.created_at
        }


class GoldenFileManager:
    """Manage golden files for test validation."""

    def __init__(self, base_dir: Path):
        """Initialize golden file manager.

        Args:
            base_dir: Base directory for golden files
        """
        self.base_dir = base_dir
        self.golden_files: Dict[str, GoldenFile] = {}
        self._load_existing_files()

    def _load_existing_files(self) -> None:
        """Load existing golden files from disk."""
        if not self.base_dir.exists():
            return

        for golden_file_path in self.base_dir.rglob("*.json"):
            if golden_file_path.name.startswith("expected_output"):
                self._load_golden_file(golden_file_path)

    def _load_golden_file(self, file_path: Path) -> None:
        """Load a single golden file.

        Args:
            file_path: Path to golden file
        """
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)

            # Extract metadata if it exists
            metadata = {}
            if isinstance(data, list) and len(data) > 0:
                record_count = len(data)
                # Calculate checksum
                data_str = json.dumps(data, sort_keys=True, default=str)
                checksum = hashlib.sha256(data_str.encode()).hexdigest()

                golden_file = GoldenFile(
                    name=file_path.stem,
                    path=file_path,
                    checksum=checksum,
                    record_count=record_count,
                    schema={},
                    metadata=metadata
                )

                self.golden_files[golden_file.name] = golden_file

        except Exception as e:
            print(f"Error loading golden file {file_path}: {e}")

    def register_golden_file(
        self,
        name: str,
        data: List[Dict[str, Any]],
        schema: Dict[str, Any],
        metadata: Optional[Dict[str, Any]] = None
    ) -> GoldenFile:
        """Register a new golden file.

        Args:
            name: Name of the golden file
            data: Expected output data
            schema: Data schema
            metadata: Additional metadata

        Returns:
            Golden file instance
        """
        if metadata is None:
            metadata = {}

        # Calculate checksum
        data_str = json.dumps(data, sort_keys=True, default=str)
        checksum = hashlib.sha256(data_str.encode()).hexdigest()

        # Create golden file instance
        golden_file = GoldenFile(
            name=name,
            path=self.base_dir / f"{name}.json",
            checksum=checksum,
            record_count=len(data),
            schema=schema,
            metadata=metadata
        )

        # Save to disk
        self.base_dir.mkdir(parents=True, exist_ok=True)
        with open(golden_file.path, 'w') as f:
            json.dump(data, f, indent=2, default=str)

        self.golden_files[name] = golden_file
        return golden_file

    def get_golden_file(self, name: str) -> Optional[GoldenFile]:
        """Get a golden file by name.

        Args:
            name: Name of the golden file

        Returns:
            Golden file instance or None if not found
        """
        return self.golden_files.get(name)

    def compare_with_actual(
        self,
        golden_name: str,
        actual_data: List[Dict[str, Any]]
    ) -> GoldenFileStatus:
        """Compare golden file with actual data.

        Args:
            golden_name: Name of the golden file
            actual_data: Actual output data to compare

        Returns:
            Comparison status
        """
        golden_file = self.get_golden_file(golden_name)
        if not golden_file:
            return GoldenFileStatus.MISSING

        # Calculate checksum of actual data
        actual_str = json.dumps(actual_data, sort_keys=True, default=str)
        actual_checksum = hashlib.sha256(actual_str.encode()).hexdigest()

        if actual_checksum == golden_file.checksum:
            return GoldenFileStatus.MATCH
        else:
            return GoldenFileStatus.DIFFERENT

    def update_golden_file(
        self,
        name: str,
        new_data: List[Dict[str, Any]],
        schema: Optional[Dict[str, Any]] = None
    ) -> GoldenFile:
        """Update an existing golden file.

        Args:
            name: Name of the golden file
            new_data: New expected data
            schema: Updated schema

        Returns:
            Updated golden file instance
        """
        if name not in self.golden_files:
            raise ValueError(f"Golden file {name} not found")

        golden_file = self.golden_files[name]

        # Update data
        data_str = json.dumps(new_data, sort_keys=True, default=str)
        golden_file.checksum = hashlib.sha256(data_str.encode()).hexdigest()
        golden_file.record_count = len(new_data)

        if schema:
            golden_file.schema = schema

        golden_file.created_at = datetime.now().isoformat()

        # Save to disk
        with open(golden_file.path, 'w') as f:
            json.dump(new_data, f, indent=2, default=str)

        return golden_file

    def list_golden_files(self) -> List[GoldenFile]:
        """List all golden files.

        Returns:
            List of all golden files
        """
        return list(self.golden_files.values())

    def get_statistics(self) -> Dict[str, Any]:
        """Get statistics about golden files.

        Returns:
            Statistics dictionary
        """
        total_files = len(self.golden_files)
        total_records = sum(gf.record_count for gf in self.golden_files.values())

        # Group by data source
        by_source = {}
        for gf in self.golden_files.values():
            source = gf.metadata.get("data_source", "unknown")
            if source not in by_source:
                by_source[source] = 0
            by_source[source] += 1

        return {
            "total_files": total_files,
            "total_records": total_records,
            "by_data_source": by_source,
            "base_directory": str(self.base_dir)
        }

    def cleanup_orphaned_files(self) -> List[Path]:
        """Clean up golden files that are no longer referenced.

        Returns:
            List of cleaned up file paths
        """
        cleaned_files = []

        if not self.base_dir.exists():
            return cleaned_files

        for golden_file_path in self.base_dir.rglob("*.json"):
            if golden_file_path.name.startswith("expected_output"):
                # Check if this file is registered
                file_registered = False
                for gf in self.golden_files.values():
                    if gf.path == golden_file_path:
                        file_registered = True
                        break

                if not file_registered:
                    # Remove orphaned file
                    golden_file_path.unlink()
                    cleaned_files.append(golden_file_path)

        return cleaned_files
