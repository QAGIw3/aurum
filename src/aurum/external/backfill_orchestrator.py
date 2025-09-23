"""Backfill orchestrator for historical data ingestion.

This module provides a robust backfill mechanism with watermarks, idempotent chunks,
and comprehensive error handling for historical ISO data ingestion.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set, Tuple, Callable, Awaitable
from enum import Enum
from pathlib import Path

from ..data_ingestion.watermark_store import WatermarkStore
from ..observability.metrics import get_metrics_client

logger = logging.getLogger(__name__)


class BackfillStatus(Enum):
    """Status of a backfill operation."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    PARTIALLY_COMPLETED = "partially_completed"
    CANCELLED = "cancelled"


class ChunkStatus(Enum):
    """Status of an individual chunk."""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class BackfillJob:
    """Configuration for a backfill job."""

    job_id: str
    iso_code: str
    data_types: List[str]
    start_date: datetime
    end_date: datetime
    priority: int = 1  # 1=highest, 10=lowest
    max_concurrent_chunks: int = 3
    chunk_size_days: int = 7
    retry_failed_chunks: bool = True
    max_retries_per_chunk: int = 3

    # Status tracking
    status: BackfillStatus = BackfillStatus.PENDING
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    completed_at: Optional[datetime] = None

    # Progress tracking
    total_chunks: int = 0
    completed_chunks: int = 0
    failed_chunks: int = 0
    skipped_chunks: int = 0

    # Chunk details
    chunks: Dict[str, 'BackfillChunk'] = field(default_factory=dict)

    # Configuration
    config: Dict[str, Any] = field(default_factory=dict)


@dataclass
class BackfillChunk:
    """Individual chunk of work in a backfill job."""

    chunk_id: str
    job_id: str
    iso_code: str
    data_type: str
    start_date: datetime
    end_date: datetime
    status: ChunkStatus = ChunkStatus.PENDING
    retry_count: int = 0

    # Processing metadata
    records_processed: int = 0
    bytes_processed: int = 0
    processing_time_seconds: float = 0.0

    # Error tracking
    last_error: Optional[str] = None
    error_count: int = 0

    # Watermark information
    watermark_before: Optional[str] = None
    watermark_after: Optional[str] = None

    # Timestamps
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None


class BackfillOrchestrator:
    """Main orchestrator for backfill operations."""

    def __init__(self):
        self.active_jobs: Dict[str, BackfillJob] = {}
        self.job_queue: asyncio.PriorityQueue = asyncio.PriorityQueue()
        self.chunk_tasks: Dict[str, asyncio.Task] = {}
        self.watermark_store = WatermarkStore()
        self.metrics = get_metrics_client()

        # Concurrency control
        self.max_concurrent_jobs = int(os.getenv("AURUM_BACKFILL_MAX_CONCURRENT_JOBS", "5"))
        self._job_semaphore = asyncio.Semaphore(self.max_concurrent_jobs)

        # Start background processing
        self._processing_task: Optional[asyncio.Task] = None
        self._is_running = False
        self._is_paused = False
        self._pause_event = asyncio.Event()
        self._pause_event.set()  # Start unpaused

    async def start(self):
        """Start the backfill orchestrator."""
        if self._is_running:
            return

        self._is_running = True
        self._processing_task = asyncio.create_task(self._process_job_queue())
        logger.info("Backfill orchestrator started")

    async def stop(self):
        """Stop the backfill orchestrator."""
        self._is_running = False

        if self._processing_task:
            self._processing_task.cancel()
            try:
                await self._processing_task
            except asyncio.CancelledError:
                pass

        # Cancel all active chunk tasks
        for task in self.chunk_tasks.values():
            if not task.done():
                task.cancel()

        logger.info("Backfill orchestrator stopped")

    async def pause(self):
        """Pause the backfill orchestrator."""
        if not self._is_running:
            return

        self._is_paused = True
        self._pause_event.clear()

        # Wait for all active chunk tasks to complete
        active_tasks = [task for task in self.chunk_tasks.values() if not task.done()]
        if active_tasks:
            logger.info(f"Waiting for {len(active_tasks)} active chunks to complete before pausing")
            await asyncio.gather(*active_tasks, return_exceptions=True)

        logger.info("Backfill orchestrator paused")

    async def resume(self):
        """Resume the backfill orchestrator."""
        if not self._is_running:
            return

        self._is_paused = False
        self._pause_event.set()
        logger.info("Backfill orchestrator resumed")

    async def is_paused(self) -> bool:
        """Check if orchestrator is paused."""
        return self._is_paused

    async def create_backfill_job(
        self,
        iso_code: str,
        data_types: List[str],
        start_date: datetime,
        end_date: datetime,
        **kwargs
    ) -> str:
        """Create a new backfill job."""
        job_id = f"{iso_code}_backfill_{int(datetime.now(timezone.utc).timestamp())}"

        job = BackfillJob(
            job_id=job_id,
            iso_code=iso_code,
            data_types=data_types,
            start_date=start_date,
            end_date=end_date,
            **kwargs
        )

        # Create chunks
        await self._create_chunks_for_job(job)

        self.active_jobs[job_id] = job
        await self.job_queue.put((job.priority, job_id, job))

        logger.info(f"Created backfill job {job_id} for {iso_code} with {job.total_chunks} chunks")

        return job_id

    async def _create_chunks_for_job(self, job: BackfillJob):
        """Create chunks for a backfill job."""
        chunks = []

        for data_type in job.data_types:
            # Generate date chunks
            current_date = job.start_date
            chunk_index = 0

            while current_date < job.end_date:
                chunk_end = min(current_date + timedelta(days=job.chunk_size_days), job.end_date)

                chunk_id = f"{job.job_id}_{data_type}_{chunk_index}"
                chunk = BackfillChunk(
                    chunk_id=chunk_id,
                    job_id=job.job_id,
                    iso_code=job.iso_code,
                    data_type=data_type,
                    start_date=current_date,
                    end_date=chunk_end
                )

                chunks.append(chunk)
                job.chunks[chunk_id] = chunk
                chunk_index += 1
                current_date = chunk_end

        job.total_chunks = len(chunks)

    async def get_job_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get the status of a backfill job."""
        if job_id not in self.active_jobs:
            return None

        job = self.active_jobs[job_id]
        return self._job_to_dict(job)

    def _job_to_dict(self, job: BackfillJob) -> Dict[str, Any]:
        """Convert job to dictionary."""
        return {
            "job_id": job.job_id,
            "iso_code": job.iso_code,
            "data_types": job.data_types,
            "start_date": job.start_date.isoformat(),
            "end_date": job.end_date.isoformat(),
            "status": job.status.value,
            "priority": job.priority,
            "created_at": job.created_at.isoformat(),
            "updated_at": job.updated_at.isoformat(),
            "completed_at": job.completed_at.isoformat() if job.completed_at else None,
            "total_chunks": job.total_chunks,
            "completed_chunks": job.completed_chunks,
            "failed_chunks": job.failed_chunks,
            "skipped_chunks": job.skipped_chunks,
            "progress_percent": (job.completed_chunks / job.total_chunks * 100) if job.total_chunks > 0 else 0,
            "chunks": {chunk_id: self._chunk_to_dict(chunk) for chunk_id, chunk in job.chunks.items()}
        }

    def _chunk_to_dict(self, chunk: BackfillChunk) -> Dict[str, Any]:
        """Convert chunk to dictionary."""
        return {
            "chunk_id": chunk.chunk_id,
            "status": chunk.status.value,
            "data_type": chunk.data_type,
            "start_date": chunk.start_date.isoformat(),
            "end_date": chunk.end_date.isoformat(),
            "records_processed": chunk.records_processed,
            "bytes_processed": chunk.bytes_processed,
            "processing_time_seconds": chunk.processing_time_seconds,
            "retry_count": chunk.retry_count,
            "last_error": chunk.last_error,
            "created_at": chunk.created_at.isoformat(),
            "started_at": chunk.started_at.isoformat() if chunk.started_at else None,
            "completed_at": chunk.completed_at.isoformat() if chunk.completed_at else None
        }

    async def _process_job_queue(self):
        """Background task to process the job queue."""
        while self._is_running:
            try:
                # Wait for pause to be lifted
                await self._pause_event.wait()

                # Wait for a job from the queue
                priority, job_id, job = await self.job_queue.get()

                # Start processing the job
                task = asyncio.create_task(self._process_job(job))
                self.chunk_tasks[job_id] = task

                # Don't wait for the job to complete, just start it

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error processing job queue: {e}")

    async def _process_job(self, job: BackfillJob):
        """Process a single backfill job."""
        try:
            job.status = BackfillStatus.RUNNING
            job.started_at = datetime.now(timezone.utc)

            logger.info(f"Starting backfill job {job.job_id} for {job.iso_code}")

            # Process chunks concurrently
            chunk_tasks = []
            semaphore = asyncio.Semaphore(job.max_concurrent_chunks)

            # Wait for pause to be lifted before starting chunks
            await self._pause_event.wait()

            for chunk in job.chunks.values():
                task = asyncio.create_task(self._process_chunk_with_semaphore(chunk, semaphore))
                chunk_tasks.append(task)

            # Wait for all chunks to complete
            results = await asyncio.gather(*chunk_tasks, return_exceptions=True)

            # Update job status based on results
            completed_chunks = sum(1 for r in results if isinstance(r, dict) and r.get('status') == 'completed')
            failed_chunks = sum(1 for r in results if isinstance(r, dict) and r.get('status') == 'failed')

            job.completed_chunks = completed_chunks
            job.failed_chunks = failed_chunks

            if failed_chunks == 0:
                job.status = BackfillStatus.COMPLETED
            elif completed_chunks > 0:
                job.status = BackfillStatus.PARTIALLY_COMPLETED
            else:
                job.status = BackfillStatus.FAILED

            job.completed_at = datetime.now(timezone.utc)

            # Emit metrics
            self.metrics.increment_counter("backfill.jobs_completed")
            self.metrics.gauge("backfill.chunks_completed", completed_chunks)
            self.metrics.gauge("backfill.chunks_failed", failed_chunks)

            logger.info(f"Completed backfill job {job.job_id}: {completed_chunks}/{job.total_chunks} chunks successful")

        except Exception as e:
            job.status = BackfillStatus.FAILED
            job.completed_at = datetime.now(timezone.utc)
            logger.error(f"Backfill job {job.job_id} failed: {e}")
            self.metrics.increment_counter("backfill.jobs_failed")

    async def _process_chunk_with_semaphore(self, chunk: BackfillChunk, semaphore: asyncio.Semaphore):
        """Process a chunk with concurrency control."""
        async with semaphore:
            return await self._process_chunk(chunk)

    async def _process_chunk(self, chunk: BackfillChunk) -> Dict[str, Any]:
        """Process a single chunk."""
        chunk.status = ChunkStatus.PROCESSING
        chunk.started_at = datetime.now(timezone.utc)
        start_time = time.time()

        try:
            # Check if we should skip this chunk
            if await self._should_skip_chunk(chunk):
                chunk.status = ChunkStatus.SKIPPED
                return {"status": "skipped", "reason": "Already processed"}

            # Get watermark before processing
            chunk.watermark_before = await self._get_watermark(chunk)

            # Process the chunk
            result = await self._execute_chunk(chunk)

            # Update chunk with results
            chunk.records_processed = result.get("records_processed", 0)
            chunk.bytes_processed = result.get("bytes_processed", 0)
            chunk.status = ChunkStatus.COMPLETED
            chunk.completed_at = datetime.now(timezone.utc)
            chunk.processing_time_seconds = time.time() - start_time

            # Update watermark after processing
            chunk.watermark_after = await self._update_watermark(chunk)

            logger.info(f"Completed chunk {chunk.chunk_id}: {chunk.records_processed} records")

            return {"status": "completed", "records_processed": chunk.records_processed}

        except Exception as e:
            chunk.status = ChunkStatus.FAILED
            chunk.last_error = str(e)
            chunk.error_count += 1
            chunk.processing_time_seconds = time.time() - start_time

            logger.error(f"Chunk {chunk.chunk_id} failed: {e}")

            # Check if we should retry
            if chunk.retry_count < chunk.job.max_retries_per_chunk and chunk.job.retry_failed_chunks:
                chunk.retry_count += 1
                chunk.status = ChunkStatus.PENDING  # Will be retried

                # Re-queue for retry
                await self._retry_chunk(chunk)

                return {"status": "retrying", "retry_count": chunk.retry_count}
            else:
                return {"status": "failed", "error": str(e)}

    async def _should_skip_chunk(self, chunk: BackfillChunk) -> bool:
        """Check if a chunk should be skipped (already processed)."""
        # Create unique key for this chunk
        chunk_key = f"backfill_{chunk.chunk_id}"

        # Check if chunk was already processed successfully
        existing_watermark = await self.watermark_store.get_watermark(
            source=f"{chunk.iso_code}.{chunk.data_type}",
            table=chunk_key
        )

        if existing_watermark:
            logger.info(f"Chunk {chunk.chunk_id} already processed, skipping")
            return True

        # Check if chunk is currently being processed (prevent duplicate processing)
        processing_key = f"processing_{chunk.chunk_id}"
        processing_watermark = await self.watermark_store.get_watermark(
            source=f"{chunk.iso_code}.{chunk.data_type}",
            table=processing_key
        )

        if processing_watermark:
            # Check if it's been processing for too long (>1 hour)
            try:
                processing_time = datetime.now(timezone.utc) - datetime.fromisoformat(processing_watermark)
                if processing_time.total_seconds() > 3600:  # 1 hour
                    logger.warning(f"Chunk {chunk.chunk_id} appears stuck, will retry")
                    await self.watermark_store.set_watermark(
                        source=f"{chunk.iso_code}.{chunk.data_type}",
                        table=processing_key,
                        value=None  # Clear stuck processing flag
                    )
                    return False
                else:
                    logger.info(f"Chunk {chunk.chunk_id} is currently being processed")
                    return True
            except (ValueError, TypeError):
                # Invalid timestamp, clear the processing flag
                await self.watermark_store.set_watermark(
                    source=f"{chunk.iso_code}.{chunk.data_type}",
                    table=processing_key,
                    value=None
                )

        # Mark chunk as being processed
        await self.watermark_store.set_watermark(
            source=f"{chunk.iso_code}.{chunk.data_type}",
            table=processing_key,
            value=datetime.now(timezone.utc).isoformat()
        )

        return False

    async def _get_watermark(self, chunk: BackfillChunk) -> Optional[str]:
        """Get watermark before processing."""
        return await self.watermark_store.get_watermark(
            f"{chunk.iso_code}.{chunk.data_type}",
            chunk.start_date,
            chunk.end_date
        )

    async def _update_watermark(self, chunk: BackfillChunk) -> Optional[str]:
        """Update watermark after processing and mark chunk as completed."""
        # Mark chunk as completed
        chunk_key = f"backfill_{chunk.chunk_id}"
        await self.watermark_store.set_watermark(
            source=f"{chunk.iso_code}.{chunk.data_type}",
            table=chunk_key,
            value=datetime.now(timezone.utc).isoformat(),
            metadata={
                "records_processed": chunk.records_processed,
                "bytes_processed": chunk.bytes_processed,
                "processing_time_seconds": chunk.processing_time_seconds,
                "status": "completed"
            }
        )

        # Clear processing flag
        processing_key = f"processing_{chunk.chunk_id}"
        await self.watermark_store.set_watermark(
            source=f"{chunk.iso_code}.{chunk.data_type}",
            table=processing_key,
            value=None
        )

        return chunk_key

    async def _execute_chunk(self, chunk: BackfillChunk) -> Dict[str, Any]:
        """Execute the actual chunk processing."""
        # This would integrate with the ISO adapters and pipeline
        # For now, return mock results
        logger.info(f"Processing chunk {chunk.chunk_id} for {chunk.data_type}")

        # Simulate processing time
        await asyncio.sleep(0.1)

        # Mock result
        return {
            "records_processed": 1000,
            "bytes_processed": 50000,
            "status": "success"
        }

    async def _retry_chunk(self, chunk: BackfillChunk):
        """Retry a failed chunk."""
        logger.info(f"Retrying chunk {chunk.chunk_id} (attempt {chunk.retry_count + 1})")

        # Create a new task for the retry
        task = asyncio.create_task(self._process_chunk(chunk))
        # Note: This simplified version doesn't re-add to queue
        # In a full implementation, we'd need more sophisticated retry logic

    def get_orchestrator_status(self) -> Dict[str, Any]:
        """Get the overall status of the orchestrator."""
        return {
            "is_running": self._is_running,
            "active_jobs": len(self.active_jobs),
            "queued_jobs": self.job_queue.qsize(),
            "active_chunk_tasks": len([t for t in self.chunk_tasks.values() if not t.done()]),
            "total_chunks_ever": sum(job.total_chunks for job in self.active_jobs.values()),
            "completed_chunks_ever": sum(job.completed_chunks for job in self.active_jobs.values()),
            "failed_chunks_ever": sum(job.failed_chunks for job in self.active_jobs.values())
        }


# Convenience functions for common backfill operations
async def backfill_caiso_historical(
    start_date: datetime,
    end_date: datetime,
    data_types: List[str] = None
) -> str:
    """Create a backfill job for CAISO historical data."""
    if data_types is None:
        data_types = ['lmp', 'load', 'asm']

    orchestrator = BackfillOrchestrator()
    return await orchestrator.create_backfill_job(
        iso_code="CAISO",
        data_types=data_types,
        start_date=start_date,
        end_date=end_date,
        chunk_size_days=7,
        priority=1
    )


async def backfill_miso_historical(
    start_date: datetime,
    end_date: datetime,
    data_types: List[str] = None
) -> str:
    """Create a backfill job for MISO historical data."""
    if data_types is None:
        data_types = ['lmp', 'load', 'generation_mix']

    orchestrator = BackfillOrchestrator()
    return await orchestrator.create_backfill_job(
        iso_code="MISO",
        data_types=data_types,
        start_date=start_date,
        end_date=end_date,
        chunk_size_days=5,
        priority=2
    )


async def backfill_pjm_historical(
    start_date: datetime,
    end_date: datetime,
    data_types: List[str] = None
) -> str:
    """Create a backfill job for PJM historical data."""
    if data_types is None:
        data_types = ['lmp', 'load', 'generation']

    orchestrator = BackfillOrchestrator()
    return await orchestrator.create_backfill_job(
        iso_code="PJM",
        data_types=data_types,
        start_date=start_date,
        end_date=end_date,
        chunk_size_days=7,
        priority=1
    )
