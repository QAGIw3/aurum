from __future__ import annotations

"""HTTP download helpers with resumable support and checksum validation."""

from dataclasses import dataclass, field
from hashlib import new as hashlib_new
from pathlib import Path
from typing import Dict, Optional

import logging
import os
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


@dataclass
class HttpAsset:
    url: str
    destination: Path
    checksum: Optional[str] = None
    checksum_algorithm: str = "sha256"
    headers: Dict[str, str] = field(default_factory=dict)


@dataclass
class AssetDownloadResult:
    asset: HttpAsset
    path: Path
    bytes_downloaded: int
    checksum: Optional[str]
    from_cache: bool


class AssetFetcher:
    """Download assets from Drought.gov with resumable support."""

    def __init__(
        self,
        download_dir: Path,
        session: Optional[requests.Session] = None,
        chunk_size: int = 16 * 1024 * 1024,
        max_retries: int = 3,
    ) -> None:
        self.download_dir = download_dir
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.session = session or self._build_session(max_retries)
        self.chunk_size = chunk_size

    def _build_session(self, max_retries: int) -> requests.Session:
        retry = Retry(
            total=max_retries,
            connect=max_retries,
            read=max_retries,
            backoff_factor=0.5,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET", "HEAD"),
        )
        adapter = HTTPAdapter(max_retries=retry)
        session = requests.Session()
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def fetch(self, asset: HttpAsset) -> AssetDownloadResult:
        destination = asset.destination
        destination.parent.mkdir(parents=True, exist_ok=True)
        try:
            head = self.session.head(asset.url, headers=asset.headers, timeout=30, allow_redirects=True)
            head.raise_for_status()
            remote_size = int(head.headers.get("Content-Length", 0))
            resume_supported = head.headers.get("Accept-Ranges", "").lower() == "bytes"
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code in {401, 403}:
                raise
            logger.debug('HEAD failed for asset; falling back to GET', extra={'url': asset.url, 'status': getattr(exc.response, 'status_code', None)})
            head = None
            remote_size = 0
            resume_supported = False
        except requests.RequestException:
            logger.exception('HEAD request failed, continuing without resume support', extra={'url': asset.url})
            head = None
            remote_size = 0
            resume_supported = False

        if destination.exists() and remote_size and destination.stat().st_size == remote_size:
            checksum = self._maybe_checksum(destination, asset)
            logger.debug("Reusing cached download", extra={"url": asset.url, "path": str(destination)})
            return AssetDownloadResult(asset=asset, path=destination, bytes_downloaded=0, checksum=checksum, from_cache=True)

        temp_path = destination.with_suffix(destination.suffix + ".part")
        downloaded_bytes = 0
        mode = "ab" if resume_supported and temp_path.exists() else "wb"
        existing = temp_path.stat().st_size if temp_path.exists() else 0

        request_headers = dict(asset.headers)
        if resume_supported and existing:
            request_headers["Range"] = f"bytes={existing}-"

        with self.session.get(asset.url, stream=True, headers=request_headers, timeout=120) as response:
            if resume_supported and response.status_code == 206:
                downloaded_bytes = existing
            response.raise_for_status()
            hasher = self._build_hasher(asset) if asset.checksum else None
            with open(temp_path, mode) as handle:
                for chunk in response.iter_content(chunk_size=self.chunk_size):
                    if not chunk:
                        continue
                    handle.write(chunk)
                    downloaded_bytes += len(chunk)
                    if hasher:
                        hasher.update(chunk)

        os.replace(temp_path, destination)
        checksum = self._finalize_checksum(destination, asset, hasher if asset.checksum else None)
        return AssetDownloadResult(
            asset=asset,
            path=destination,
            bytes_downloaded=downloaded_bytes,
            checksum=checksum,
            from_cache=False,
        )

    def _build_hasher(self, asset: HttpAsset):
        return hashlib_new(asset.checksum_algorithm)

    def _finalize_checksum(self, destination: Path, asset: HttpAsset, hasher) -> Optional[str]:
        if not asset.checksum:
            return None
        digest = hasher.hexdigest() if hasher else self._maybe_checksum(destination, asset)
        if digest.lower() != asset.checksum.lower():
            raise ChecksumMismatchError(asset.url, expected=asset.checksum, actual=digest)
        return digest

    def _maybe_checksum(self, destination: Path, asset: HttpAsset) -> Optional[str]:
        if not asset.checksum:
            return None
        hasher = self._build_hasher(asset)
        with open(destination, "rb") as handle:
            for chunk in iter(lambda: handle.read(self.chunk_size), b""):
                hasher.update(chunk)
        digest = hasher.hexdigest()
        if digest.lower() != asset.checksum.lower():
            raise ChecksumMismatchError(asset.url, expected=asset.checksum, actual=digest)
        return digest


class ChecksumMismatchError(RuntimeError):
    def __init__(self, url: str, expected: str, actual: str) -> None:
        super().__init__(f"Checksum mismatch for {url}: expected {expected} but got {actual}")
        self.url = url
        self.expected = expected
        self.actual = actual


__all__ = ["HttpAsset", "AssetDownloadResult", "AssetFetcher", "ChecksumMismatchError"]
