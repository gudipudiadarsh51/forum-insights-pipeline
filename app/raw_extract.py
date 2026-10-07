"""
Shapes raw API responses into the flat dicts written to GCS. This is
extraction-time shaping, not transformation: it doesn't clean, dedupe,
score, or enrich anything - it only pulls out the fields worth keeping
and validates the handful of fields the rest of the service depends on
(mainly "id"). No sentiment, no HTML stripping, no business logic.

Every function raises ValueError with a specific message when a
required field is missing or malformed, rather than letting a bare
KeyError/TypeError surface. The caller (app/ingest.py) catches these
per-record and routes the offending record to the dead letter queue
instead of losing the whole batch or crashing the poll loop.
"""
from __future__ import annotations

import datetime as dt
from typing import Any, Dict


def _now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def _require(d: Dict[str, Any], key: str, label: str) -> Any:
    value = d.get(key)
    if value is None:
        raise ValueError(f"{label} missing required field {key!r}")
    return value


# --- Hacker News (Algolia search hits) ---

def raw_hn_hit_dict(hit: Dict[str, Any], matched_keyword: str) -> Dict[str, Any]:
    """
    Algolia's story and comment hits have different field names for the
    same concepts (title/story_title, url/story_url, text/comment_text),
    so this normalizes both into one shape, plus which keyword matched.
    """
    object_id = _require(hit, "objectID", "HN hit")

    tags = hit.get("_tags", [])
    item_type = "comment" if "comment" in tags else "story"
    if item_type == "story":
        text, title, url = hit.get("story_text"), hit.get("title"), hit.get("url")
        score, descendants, parent = hit.get("points"), hit.get("num_comments"), None
    else:
        text, title, url = hit.get("comment_text"), hit.get("story_title"), hit.get("story_url")
        score, descendants, parent = None, None, hit.get("parent_id")

    return {
        "id": str(object_id),
        "type": item_type,
        "by": hit.get("author"),
        "title": title,
        "text": text,   # raw HTML, as returned by the API
        "url": url,
        "score": score,
        "descendants": descendants,
        "parent": str(parent) if parent else None,
        "matched_keywords": [matched_keyword],
        "created_utc": hit.get("created_at_i"),  # epoch seconds
        "ingested_at": _now_iso(),
    }


# --- GitHub ---

def raw_github_issue_dict(issue: Dict[str, Any], owner: str, repo: str) -> Dict[str, Any]:
    issue_id = _require(issue, "id", "GitHub issue")
    return {
        "id": str(issue_id),
        "repo": f"{owner}/{repo}",
        "number": issue.get("number"),
        "is_pull_request": "pull_request" in issue,
        "title": issue.get("title"),
        "body": issue.get("body"),
        "state": issue.get("state"),
        "author": (issue.get("user") or {}).get("login"),
        "labels": [l.get("name") for l in issue.get("labels", []) if isinstance(l, dict)],
        "comments_count": issue.get("comments"),
        "url": issue.get("html_url"),
        "created_at": issue.get("created_at"),
        "updated_at": issue.get("updated_at"),
        "closed_at": issue.get("closed_at"),
        "ingested_at": _now_iso(),
    }


def raw_github_comment_dict(comment: Dict[str, Any], owner: str, repo: str) -> Dict[str, Any]:
    comment_id = _require(comment, "id", "GitHub comment")
    issue_url = comment.get("issue_url") or ""
    issue_number = issue_url.rstrip("/").split("/")[-1] if issue_url else None
    return {
        "id": str(comment_id),
        "repo": f"{owner}/{repo}",
        "issue_number": issue_number,
        "author": (comment.get("user") or {}).get("login"),
        "body": comment.get("body"),
        "url": comment.get("html_url"),
        "created_at": comment.get("created_at"),
        "updated_at": comment.get("updated_at"),
        "ingested_at": _now_iso(),
    }


def raw_github_mention_dict(item: Dict[str, Any], matched_keyword: str) -> Dict[str, Any]:
    """
    From the site-wide Search API: an issue/PR anywhere on GitHub whose
    title or body mentioned a keyword we're tracking - used for
    closed-source competitors with no repo of their own to poll.
    """
    item_id = _require(item, "id", "GitHub search hit")
    repo_url = item.get("repository_url") or ""
    repo = repo_url.split("/repos/", 1)[-1] if "/repos/" in repo_url else None
    return {
        "id": str(item_id),
        "repo": repo,
        "number": item.get("number"),
        "is_pull_request": "pull_request" in item,
        "title": item.get("title"),
        "body": item.get("body"),
        "state": item.get("state"),
        "author": (item.get("user") or {}).get("login"),
        "url": item.get("html_url"),
        "matched_keyword": matched_keyword,
        "created_at": item.get("created_at"),
        "updated_at": item.get("updated_at"),
        "ingested_at": _now_iso(),
    }
