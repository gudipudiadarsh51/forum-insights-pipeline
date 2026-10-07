"""
Buffers records in memory and periodically flushes them to Google Cloud
Storage as gzip-compressed newline-delimited JSON files.

Used for two things in this service, distinguished by which top-level
prefix they write under:
  - Bronze landing (config.gcs_bronze_prefix): raw records straight
    from the source APIs.
  - Dead letter queue (config.gcs_dead_letter_prefix): records that
    failed validation/shaping during extraction (see app/dead_letter.py).

Objects are written under:
    gs://<bucket>/<prefix>/<entity>/dt=YYYY-MM-DD/hour=HH/<uuid>.jsonl.gz

Partitioning by date/hour keeps downstream consumers' job small (only
recent partitions need scanning) and makes a historical backfill for a
specific window cheap to re-run.

Reliability: a failed upload puts the batch BACK at the front of the
buffer instead of discarding it, so a transient GCS/network error costs
a delay, not data. A batch that keeps failing past a retry cap is
logged loudly rather than silently dropped - the person, not the
process, is expected to notice and remediate a batch that won't upload
after several retries.

Shutdown timing: on Cloud Run (worker pools or services), SIGTERM is
followed by a FIXED 10-second grace period before SIGKILL - there is no
way to extend it. stop() therefore does one quick, single-attempt,
short-timeout flush rather than the full multi-attempt retry loop used
during normal operation - a deadline is exactly the situation where
"try three more times with backoff" is the wrong move, since it can
burn the entire shutdown budget on retries that likely won't succeed
anyway, starving every other writer that also needs to flush before
SIGKILL arrives.
"""
from __future__ import annotations

import gzip
import json
import logging
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from google.cloud import storage

from .config import config

logger = logging.getLogger(__name__)

_MAX_UPLOAD_ATTEMPTS = 4
_RETRY_BACKOFF_SECONDS = 2.0
_UPLOAD_TIMEOUT_SECONDS = 30          # per-call timeout during normal operation
_SHUTDOWN_THREAD_JOIN_TIMEOUT = 2.0    # default; callers with a tighter deadline should pass their own
_SHUTDOWN_UPLOAD_TIMEOUT_SECONDS = 5   # short - a single best-effort attempt, not a wait-it-out call


class GCSBatchWriter:
    def __init__(self, entity: str, top_level_prefix: Optional[str] = None, client: Optional[storage.Client] = None) -> None:
        """
        entity: logical record type/path, e.g. "hackernews/items" -
        becomes part of the GCS object path.
        top_level_prefix: overrides config.gcs_bronze_prefix - used to
        route dead-letter records to a separate top-level path.
        """
        self.entity = entity
        self._prefix = top_level_prefix or config.gcs_bronze_prefix
        self._client = client or storage.Client(project=config.gcp_project_id or None)
        self._bucket = self._client.bucket(config.gcs_bucket)

        self._buffer: List[Dict[str, Any]] = []
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        self._thread = threading.Thread(
            target=self._flush_loop, name=f"gcs-flush-{self.entity}", daemon=True
        )
        self._thread.start()

    def stop(
        self,
        thread_join_timeout: float = _SHUTDOWN_THREAD_JOIN_TIMEOUT,
        upload_timeout: float = _SHUTDOWN_UPLOAD_TIMEOUT_SECONDS,
    ) -> None:
        """
        Both timeouts are explicit parameters, not hardcoded, specifically
        because a caller operating under a shared shutdown deadline (see
        app/ingest.py) needs to control the TOTAL time this call can take -
        thread_join_timeout plus upload_timeout - not just one piece of it.
        A fixed upload_timeout here would let N writers each independently
        burn their own full allotment regardless of how little time is
        actually left, which defeats the point of a shared deadline.
        """
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=thread_join_timeout)
        # One quick, bounded, single-attempt flush - see module docstring
        # on why this isn't the full retry loop used during normal operation.
        self.flush(max_attempts=1, upload_timeout=upload_timeout)

    def add(self, row: Dict[str, Any]) -> None:
        with self._lock:
            self._buffer.append(row)
            should_flush = len(self._buffer) >= config.gcs_batch_size
        if should_flush:
            self.flush()

    def flush(self, max_attempts: Optional[int] = None, upload_timeout: Optional[float] = None) -> None:
        """
        max_attempts / upload_timeout: override the retry count and
        per-call timeout. Left as None for normal periodic flushing
        (full retry loop, generous timeout); stop() passes tight values
        since it's operating under a hard deadline.
        """
        with self._lock:
            if not self._buffer:
                return
            batch, self._buffer = self._buffer, []

        if self._upload_with_retry(batch, max_attempts=max_attempts, upload_timeout=upload_timeout):
            return

        # every attempt failed - put the batch back so the next flush
        # cycle (or the final flush on stop()) tries again, instead of
        # silently losing it.
        with self._lock:
            self._buffer = batch + self._buffer
        logger.error(
            "Giving up on uploading %d %s record(s) this cycle; "
            "they remain buffered and will be retried on the next flush.",
            len(batch), self.entity,
        )

    # -- internal ---------------------------------------------------------

    def _upload_with_retry(self, batch: List[Dict[str, Any]], max_attempts: Optional[int] = None, upload_timeout: Optional[float] = None) -> bool:
        max_attempts = max_attempts or _MAX_UPLOAD_ATTEMPTS
        upload_timeout = upload_timeout or _UPLOAD_TIMEOUT_SECONDS

        body = b"\n".join(json.dumps(row, default=str).encode("utf-8") for row in batch)
        compressed = gzip.compress(body)
        blob_path = self._build_path()

        for attempt in range(1, max_attempts + 1):
            try:
                self._bucket.blob(blob_path).upload_from_string(
                    compressed, content_type="application/json", timeout=upload_timeout
                )
                logger.info("Wrote %d %s record(s) to gs://%s/%s", len(batch), self.entity, config.gcs_bucket, blob_path)
                return True
            except Exception:
                logger.warning(
                    "Upload attempt %d/%d failed for %d %s record(s)",
                    attempt, max_attempts, len(batch), self.entity, exc_info=True,
                )
                if attempt < max_attempts:
                    time.sleep(_RETRY_BACKOFF_SECONDS * attempt)
        return False

    def _build_path(self) -> str:
        now = datetime.now(timezone.utc)
        return (
            f"{self._prefix}/{self.entity}/"
            f"dt={now:%Y-%m-%d}/hour={now:%H}/"
            f"{now:%Y%m%dT%H%M%S}-{uuid.uuid4().hex}.jsonl.gz"
        )

    def _flush_loop(self) -> None:
        while not self._stop_event.is_set():
            self._stop_event.wait(config.gcs_flush_interval_seconds)
            self.flush()


# Kept as an alias: existing code refers to this writer as "Bronze" since
# that's its primary use, but the class itself is prefix-agnostic.
GCSBronzeWriter = GCSBatchWriter
