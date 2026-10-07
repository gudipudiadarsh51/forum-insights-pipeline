"""
GitHub ingestion via the official REST API v3.

Two complementary access patterns:

1. Repo-scoped: "list issues" and "list issue comments" for a specific
   repo - used for open-source competitors, since their own repo's
   issues are direct product feedback. Both support a `since` (ISO
   8601) filter, so once a cursor is saved, each poll only asks for
   what changed.

2. Site-wide search: closed-source competitors (DataGrip, Navicat,
   TablePlus, etc.) have no repo of their own to poll, so the only way
   to catch mentions of them is GitHub's Search API - "any issue/PR on
   GitHub whose title or body mentions this keyword." This has a
   separate, stricter rate limit (30/min authenticated) and caps at
   1,000 results per query, which is a non-issue at this data volume.

Auth: a personal access token raises the core API rate limit from
60/hr to 5,000/hr (also required for private repos). Set GITHUB_TOKEN.
"""
from __future__ import annotations

import logging
import time
from typing import Any, Dict, Iterator, Optional

import requests

from .config import config

logger = logging.getLogger(__name__)

API_BASE = "https://api.github.com"


def _headers() -> Dict[str, str]:
    headers = {"Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28"}
    if config.github_token:
        headers["Authorization"] = f"Bearer {config.github_token}"
    return headers


def _get(url: str, params: Dict[str, Any]) -> requests.Response:
    resp = requests.get(url, headers=_headers(), params=params, timeout=config.http_timeout_seconds)
    if resp.status_code == 403 and resp.headers.get("X-RateLimit-Remaining") == "0":
        reset_at = int(resp.headers.get("X-RateLimit-Reset", time.time() + 60))
        wait_seconds = max(reset_at - time.time(), 1)
        logger.warning("GitHub rate limit hit, sleeping %.0fs until reset", wait_seconds)
        time.sleep(wait_seconds)
        resp = requests.get(url, headers=_headers(), params=params, timeout=config.http_timeout_seconds)
    resp.raise_for_status()
    return resp


def _paginate(url: str, params: Dict[str, Any]) -> Iterator[Dict[str, Any]]:
    params = dict(params, per_page=100)
    while url:
        resp = _get(url, params)
        yield from resp.json()
        url = resp.links.get("next", {}).get("url")
        params = {}  # subsequent requests use the fully-formed "next" link


def list_issues(owner: str, repo: str, since: Optional[str] = None) -> Iterator[Dict[str, Any]]:
    """Issues AND pull requests (GitHub's issues endpoint includes both)."""
    params: Dict[str, Any] = {"state": "all", "sort": "updated", "direction": "asc"}
    if since:
        params["since"] = since
    yield from _paginate(f"{API_BASE}/repos/{owner}/{repo}/issues", params)


def list_issue_comments(owner: str, repo: str, since: Optional[str] = None) -> Iterator[Dict[str, Any]]:
    """All comments across every issue/PR in the repo, newest changes last."""
    params: Dict[str, Any] = {"sort": "updated", "direction": "asc"}
    if since:
        params["since"] = since
    yield from _paginate(f"{API_BASE}/repos/{owner}/{repo}/issues/comments", params)


def _paginate_search(url: str, params: Dict[str, Any]) -> Iterator[Dict[str, Any]]:
    """Like _paginate, but for the Search API's {"total_count", "items": [...]} response shape."""
    params = dict(params, per_page=100)
    while url:
        resp = _get(url, params)
        yield from resp.json().get("items", [])
        url = resp.links.get("next", {}).get("url")
        params = {}


def search_issues(keyword: str, since_iso: str, until_iso: Optional[str] = None) -> Iterator[Dict[str, Any]]:
    """
    Site-wide search for issues/PRs mentioning `keyword` in the title or
    body, created after since_iso. Used for competitors with no repo of
    their own to poll directly.

    If until_iso is given, bounds the upper end too - used to sweep a
    wide historical range in small windows, since GitHub's Search API
    hard-caps at 1,000 results per query regardless of pagination
    (see backfill.py).
    """
    date_filter = f"{since_iso}..{until_iso}" if until_iso else f">{since_iso}"
    query = f'"{keyword}" in:title,body created:{date_filter}'
    params: Dict[str, Any] = {"q": query, "sort": "created", "order": "asc"}
    yield from _paginate_search(f"{API_BASE}/search/issues", params)
