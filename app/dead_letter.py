"""
Dead letter queue for records that fail during extraction.

If a record from an API can't be shaped into the expected fields (a
required field is missing, an unexpected type shows up, whatever) it
gets routed here instead of two bad alternatives: crashing the whole
poll cycle (which would also lose every good record already collected
in that cycle), or silently dropping the record with just a log line.

Failed records land in GCS at:
    gs://<bucket>/<dead_letter_prefix>/<source>/dt=.../hour=.../*.jsonl.gz

Each entry keeps the original raw payload, the error, and enough
context to triage and, if it turns out to be worth recovering, re-run
through a fixed extraction path later.
"""
from __future__ import annotations

import datetime as dt
import logging
import time
import traceback
from typing import Any, Dict, Optional

from .config import config
from .gcs_writer import GCSBatchWriter

logger = logging.getLogger(__name__)

_writers: Dict[str, GCSBatchWriter] = {}


def _get_writer(source: str) -> GCSBatchWriter:
    if source not in _writers:
        writer = GCSBatchWriter(source, top_level_prefix=config.gcs_dead_letter_prefix)
        writer.start()
        _writers[source] = writer
    return _writers[source]


def send(source: str, raw_payload: Any, error: Exception, context: Optional[Dict[str, Any]] = None) -> None:
    """
    Routes one bad record to the dead letter queue. Never raises - a
    problem writing to the DLQ itself is logged, not propagated, since
    the caller's job is to keep processing the rest of the batch.
    """
    entry = {
        "source": source,
        "error_type": type(error).__name__,
        "error_message": str(error),
        "traceback": traceback.format_exc(limit=5),
        "context": context or {},
        "raw_payload": raw_payload,
        "failed_at": dt.datetime.now(dt.timezone.utc).isoformat(),
    }
    logger.warning("Routing bad record from %r to dead letter queue: %s: %s", source, type(error).__name__, error)
    try:
        _get_writer(source).add(entry)
    except Exception:
        logger.exception("Failed to write to the dead letter queue itself for source %r - record is lost: %r", source, raw_payload)


def stop_all(deadline: Optional[float] = None) -> None:
    """
    deadline: a time.monotonic() timestamp by which all writers must be
    stopped. If not given, each writer gets a small default budget - see
    app/ingest.py for how the real shutdown path computes a shared one.

    Each writer's TOTAL time (thread join + the final upload attempt
    stop() makes) must come out of its slice of the deadline - a fixed
    upload timeout here would let every writer independently burn its
    own full allotment regardless of how little time is actually left.
    """
    writers = list(_writers.values())
    for i, writer in enumerate(writers):
        remaining = max(0.0, deadline - time.monotonic()) if deadline is not None else 1.5
        writers_left = len(writers) - i
        per_writer_budget = max(0.0, min(1.5, remaining / writers_left))
        join_slice = min(0.2, per_writer_budget * 0.2)
        upload_slice = max(0.0, per_writer_budget - join_slice)
        writer.stop(thread_join_timeout=join_slice, upload_timeout=upload_slice)
