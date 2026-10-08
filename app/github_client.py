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
from dataclasses import dataclass
from typing import Any, Dict, Iterator, Optional, Tuple

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


# --- GitHub: repo-scoped, categorized production-issue search ---
#
# A third mode, distinct from both repo-scoped polling (mode 1) and the
# flat, unscoped mentions search (mode 2) above: a themed keyword group,
# run against a specific set of repos chosen because that failure mode
# is actually relevant to them (locks for migration CLIs, not GUI
# clients; memory/freeze for desktop GUIs, not CLI tools).
#
# Repo groupings below follow four tool categories (desktop GUIs,
# migration CLIs, ORMs, governance platforms); each keyword category is
# assigned to whichever group it's about. This is a starting
# assignment, not a fixed taxonomy - edit TARGETED_CATEGORIES directly
# to retarget, add repos, or add categories. It lives here as code
# rather than as an env var because it's inherently structured
# (category -> repos -> keywords -> filters), not a flat list.
#
# NOT scoped to in:title,body (unlike search_issues() above) - by
# default GitHub's search covers title, body, AND comments, and the
# comment thread is often exactly where the real content lives (a
# workaround, a maintainer's diagnosis) - see dbeaver/dbeaver#17108,
# which only made sense read across three separate comments.

# dbeaver/dbeaver deliberately excluded: it's already in
# config.github_oss_repos, which polls 100% of its issues/comments
# unfiltered - a strict superset of anything a keyword search here
# could find. Searching it again would just refetch data already
# sitting in github/issues and github/comments.
_DESKTOP_GUI_REPOS: Tuple[str, ...] = ("microsoft/azuredatastudio", "schemacrawler/SchemaCrawler")
_MIGRATION_CLI_REPOS: Tuple[str, ...] = ("liquibase/liquibase", "flyway/flyway", "golang-migrate/migrate")
# prisma/prisma was renamed to prisma/orm - GitHub's Search API `repo:`
# qualifier does NOT follow repo renames (unlike the website and most
# REST endpoints), so the old name 422s with a misleading "does not
# exist" error. Confirmed live: prisma/orm returns results correctly.
_ORM_REPOS: Tuple[str, ...] = ("prisma/orm", "typeorm/typeorm", "sqlalchemy/sqlalchemy")
_GOVERNANCE_REPOS: Tuple[str, ...] = ("bytebase/bytebase", "ariga/atlas")

# label:bug,regression was removed from here after direct testing
# confirmed it silently zeroed out nearly all real signal: 978 matches
# in prisma/orm, 236 in liquibase/liquibase, and 96 in bytebase/bytebase
# all dropped to 0 the moment that filter was added, because none of
# those repos actually use labels literally named "bug" or "regression"
# - label taxonomy isn't portable across differently-governed repos the
# way state/comments/exclusions are. If a specific repo's real label
# names are known, pass them as a per-category extra_filters override
# instead of changing this shared default.
_DEFAULT_TARGETED_FILTERS: Tuple[str, ...] = (
    "state:closed",
    "comments:>3",
    "-label:invalid",
    "-label:question",
)


@dataclass(frozen=True)
class TargetedCategory:
    name: str
    repos: Tuple[str, ...]
    keywords: Tuple[str, ...]
    extra_filters: Tuple[str, ...] = _DEFAULT_TARGETED_FILTERS


TARGETED_CATEGORIES: Tuple[TargetedCategory, ...] = (
    TargetedCategory(
        name="memory_scale",
        repos=_DESKTOP_GUI_REPOS,
        keywords=("OutOfMemory", "Heap space", "Java heap", "High CPU", "Freeze", "Large schema"),
    ),
    TargetedCategory(
        name="locks_deadlocks",
        repos=_MIGRATION_CLI_REPOS,
        keywords=("Exclusive lock", "Table lock", "Lock timeout", "Deadlock", "DATABASECHANGELOGLOCK"),
    ),
    TargetedCategory(
        name="connection_timeouts",
        repos=_DESKTOP_GUI_REPOS,
        keywords=("Connection timeout", "Socket timeout", "Catalog fetch", "MetaData timeout", "Driver error"),
    ),
    TargetedCategory(
        name="schema_drift",
        repos=_ORM_REPOS,
        keywords=("Schema drift", "Drop column", "Data loss", "Migration failed", "Out of sync"),
    ),
    TargetedCategory(
        name="multi_tenant_rollout",
        repos=_GOVERNANCE_REPOS,
        keywords=("Multi tenant", "Parallel migration", "Race condition", "Partial rollout", "Pipeline failed"),
    ),
)


def build_targeted_query(category: TargetedCategory, repo: str, since_iso: str, until_iso: Optional[str] = None) -> str:
    date_filter = f"{since_iso}..{until_iso}" if until_iso else f">{since_iso}"
    keyword_group = " OR ".join(f'"{kw}"' for kw in category.keywords)
    filters = " ".join(category.extra_filters)
    return f"repo:{repo} {filters} created:{date_filter} ({keyword_group})"


def search_targeted(
    category: TargetedCategory, repo: str, since_iso: str, until_iso: Optional[str] = None
) -> Iterator[Dict[str, Any]]:
    """Repo-scoped, categorized production-issue search - see TargetedCategory above."""
    query = build_targeted_query(category, repo, since_iso, until_iso)
    params: Dict[str, Any] = {"q": query, "sort": "created", "order": "asc"}
    yield from _paginate_search(f"{API_BASE}/search/issues", params)


def list_comments_for_issue(owner: str, repo: str, issue_number: int) -> Iterator[Dict[str, Any]]:
    """All comments on one specific issue/PR - usually few, but paginated for safety."""
    yield from _paginate(f"{API_BASE}/repos/{owner}/{repo}/issues/{issue_number}/comments", {})


def search_targeted_with_comments(
    category: TargetedCategory, repo: str, since_iso: str, until_iso: Optional[str] = None
) -> Iterator[Dict[str, Any]]:
    """
    Like search_targeted(), but each yielded item also carries its full
    comment thread under "_comments".

    Why this exists: GitHub's search matches on title, body, AND
    comments by default, but the response it returns only ever contains
    the issue's own title/body - never the comment that actually
    matched. A hit here can be comment-driven with nothing relevant in
    the stored title/body at all unless the comments are fetched too.

    This also avoids the OSS-repo poller's issues/comments-in-separate-
    buckets split, which makes thread reassembly require a downstream
    SQL join - here, the thread is already fully assembled at
    extraction time, one record per match.
    """
    owner, repo_name = repo.split("/", 1)
    for item in search_targeted(category, repo, since_iso, until_iso):
        issue_number = item.get("number")
        comments = list(list_comments_for_issue(owner, repo_name, issue_number)) if issue_number else []
        item = dict(item)
        item["_comments"] = comments
        yield item
