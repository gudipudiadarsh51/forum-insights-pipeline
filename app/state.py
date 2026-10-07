"""
Minimal cursor/checkpoint persistence in Google Cloud Storage.

Polling connectors need to remember "how far we got" across restarts -
Hacker News's last-swept item id, GitHub's last-seen updated_at per
repo. This stores one small JSON blob per key under
gs://<bucket>/_state/<key>.json. It's deliberately simple: connectors
checkpoint once per poll, not per record, so this sees low write volume.
"""
from __future__ import annotations

import json
import logging
from typing import Any, Optional

from google.cloud import storage

from .config import config

logger = logging.getLogger(__name__)

_client: Optional[storage.Client] = None


def _get_client() -> storage.Client:
    global _client
    if _client is None:
        _client = storage.Client(project=config.gcp_project_id or None)
    return _client


def _blob(key: str):
    bucket = _get_client().bucket(config.gcs_bucket)
    return bucket.blob(f"_state/{key}.json")


def get_state(key: str) -> Optional[Any]:
    blob = _blob(key)
    try:
        if not blob.exists():
            return None
        data = json.loads(blob.download_as_bytes())
        return data.get("value")
    except Exception:
        logger.exception("Failed to read state for key %s; treating as unset", key)
        return None


def set_state(key: str, value: Any) -> None:
    try:
        _blob(key).upload_from_string(json.dumps({"value": value}), content_type="application/json")
    except Exception:
        logger.exception("Failed to persist state for key %s", key)
