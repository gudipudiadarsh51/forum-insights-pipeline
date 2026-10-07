"""
Hacker News ingestion via the Algolia HN Search API (no auth needed):
https://hn.algolia.com/api - not the raw Firebase item feed.

Algolia indexes the same HN content but supports full-text search plus
a numeric date filter, which is exactly what a niche-topic filter needs:
one query per keyword, "only items created after my last checkpoint."
This is the "filter at the source" approach - irrelevant items never
leave Algolia's servers, so they never cost us anything (no rate limit
spent inspecting things we don't care about).
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

import requests

from .config import config

logger = logging.getLogger(__name__)

SEARCH_URL = "https://hn.algolia.com/api/v1/search_by_date"
_session = requests.Session()

_HITS_PER_PAGE = 200
_MAX_PAGES = 5  # safety cap: 1,000 hits/keyword/poll is far more than a niche keyword should ever produce


def search_since(keyword: str, since_epoch: int, until_epoch: Optional[int] = None) -> List[Dict[str, Any]]:
    """
    Returns all story/comment hits matching `keyword` created after
    since_epoch (unix seconds). If until_epoch is given, also bounds the
    upper end - used to sweep a wide historical range in small windows so
    no single call gets close to the paging cap below (see backfill.py).
    """
    numeric_filters = f"created_at_i>{since_epoch}"
    if until_epoch is not None:
        numeric_filters += f",created_at_i<{until_epoch}"

    hits: List[Dict[str, Any]] = []
    for page in range(_MAX_PAGES):
        resp = _session.get(
            SEARCH_URL,
            params={
                "query": keyword,
                "tags": "(story,comment)",  # parens = OR: either tag matches
                "numericFilters": numeric_filters,
                "hitsPerPage": _HITS_PER_PAGE,
                "page": page,
            },
            timeout=config.http_timeout_seconds,
        )
        resp.raise_for_status()
        data = resp.json()
        page_hits = data.get("hits", [])
        hits.extend(page_hits)
        if len(page_hits) < _HITS_PER_PAGE:
            break
    else:
        logger.warning("Hit the %d-page safety cap searching for %r - some results may be missed", _MAX_PAGES, keyword)
    return hits
