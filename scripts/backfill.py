#!/usr/bin/env python
"""
One-time historical backfill for Hacker News and GitHub.

Separate from the always-on service (app/main.py) on purpose: this is a
finite, run-once-and-done job, not a poll loop. It does NOT read or write
the live service's saved cursors (app/state.py) - running this has no
effect on the continuously-running poller, and it's safe to re-run this
script itself if interrupted (Bronze is append-only; re-fetched windows
just produce duplicate raw records, which the transformation layer
downstream is expected to dedupe, same as any other overlap in Bronze).

Why this can't just be "set the lookback higher": two of the three
sources have a hard result cap per query that a single wide date range
would silently exceed:

  - Hacker News (Algolia search): this codebase's own paging cap is
    1,000 hits/query (5 pages x 200). A popular keyword over years of
    history can far exceed that.
  - GitHub's Search API: hard-capped at 1,000 results per query,
    regardless of pagination - documented by GitHub itself.
  - GitHub's repo-scoped issues/comments endpoints have NO such cap -
    they paginate fully via Link headers - so OSS repos are backfilled
    with a single wide `since` date, no windowing needed.

The fix for the first two: sweep the requested range in small time
windows (default: 7 days for HN, 30 days for GitHub search) so no
single query gets close to its cap. Window sizes are tunable if a
particular keyword is dense enough to need finer chunking - watch the
logs for hn_client's "safety cap" warning, which means a window was
too wide for that keyword and some results in it were missed.

Usage:
    python -m scripts.backfill --years 5
    python -m scripts.backfill --since 2021-01-01
    python -m scripts.backfill --since 2021-01-01 --sources hackernews
    python -m scripts.backfill --years 5 --hn-window-days 3
"""
from __future__ import annotations

import argparse
import datetime as dt
import logging
import sys
import time
from typing import Iterator, Tuple

from app import github_client, hn_client
from app.config import config
from app.dead_letter import send as send_to_dead_letter
from app.dead_letter import stop_all as stop_dead_letter
from app.gcs_writer import GCSBatchWriter
from app.raw_extract import (
    raw_github_comment_dict,
    raw_github_issue_dict,
    raw_github_mention_dict,
    raw_hn_hit_dict,
)

logging.basicConfig(
    level=getattr(logging, config.log_level.upper(), logging.INFO),
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
)
logger = logging.getLogger("backfill")

# GitHub's Search API allows 30 req/min authenticated - pace calls so we
# never trip it, rather than only reacting after a 403.
_GITHUB_SEARCH_PACING_SECONDS = 2.1


def _iso_windows(since: dt.datetime, until: dt.datetime, window_days: int) -> Iterator[Tuple[dt.datetime, dt.datetime]]:
    """Yields (window_start, window_end) pairs, oldest first, covering [since, until)."""
    cursor = since
    step = dt.timedelta(days=window_days)
    while cursor < until:
        window_end = min(cursor + step, until)
        yield cursor, window_end
        cursor = window_end


def backfill_hackernews(since: dt.datetime, until: dt.datetime, window_days: int) -> None:
    writer = GCSBatchWriter("hackernews/items")
    writer.start()
    try:
        for keyword in config.hn_search_keywords:
            total = 0
            for win_start, win_end in _iso_windows(since, until, window_days):
                hits = hn_client.search_since(keyword, int(win_start.timestamp()), int(win_end.timestamp()))
                for hit in hits:
                    try:
                        raw = raw_hn_hit_dict(hit, keyword)
                    except Exception as e:
                        send_to_dead_letter("hackernews", hit, e, context={"keyword": keyword, "backfill_window": str(win_start.date())})
                        continue
                    writer.add(raw)
                total += len(hits)
            logger.info("HN keyword %r: %d hit(s) across %s to %s", keyword, total, since.date(), until.date())
    finally:
        writer.stop()


def backfill_github_search(since: dt.datetime, until: dt.datetime, window_days: int) -> None:
    writer = GCSBatchWriter("github/mentions")
    writer.start()
    try:
        for keyword in config.github_search_keywords:
            total = 0
            for win_start, win_end in _iso_windows(since, until, window_days):
                try:
                    for item in github_client.search_issues(keyword, win_start.isoformat(), win_end.isoformat()):
                        try:
                            writer.add(raw_github_mention_dict(item, keyword))
                        except Exception as e:
                            send_to_dead_letter("github_mentions", item, e, context={"keyword": keyword, "backfill_window": str(win_start.date())})
                            continue
                        total += 1
                except Exception:
                    logger.exception("GitHub search failed for %r, window %s to %s - continuing", keyword, win_start.date(), win_end.date())
                time.sleep(_GITHUB_SEARCH_PACING_SECONDS)  # stay comfortably under 30 req/min
            logger.info("GitHub search %r: %d mention(s) across %s to %s", keyword, total, since.date(), until.date())
    finally:
        writer.stop()


def backfill_github_repos(since: dt.datetime) -> None:
    issues_writer = GCSBatchWriter("github/issues")
    comments_writer = GCSBatchWriter("github/comments")
    issues_writer.start()
    comments_writer.start()
    since_iso = since.isoformat()
    try:
        for repo_full in config.github_oss_repos:
            owner, repo = repo_full.split("/", 1)

            issue_count = 0
            for issue in github_client.list_issues(owner, repo, since=since_iso):
                try:
                    issues_writer.add(raw_github_issue_dict(issue, owner, repo))
                except Exception as e:
                    send_to_dead_letter("github_issues", issue, e, context={"repo": repo_full})
                    continue
                issue_count += 1
            logger.info("GitHub %s: %d issue(s)/PR(s) since %s", repo_full, issue_count, since.date())

            comment_count = 0
            for comment in github_client.list_issue_comments(owner, repo, since=since_iso):
                try:
                    comments_writer.add(raw_github_comment_dict(comment, owner, repo))
                except Exception as e:
                    send_to_dead_letter("github_comments", comment, e, context={"repo": repo_full})
                    continue
                comment_count += 1
            logger.info("GitHub %s: %d comment(s) since %s", repo_full, comment_count, since.date())
    finally:
        issues_writer.stop()
        comments_writer.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Historical backfill for Hacker News and GitHub.")
    parser.add_argument("--since", help="Start date, YYYY-MM-DD. Overrides --years if both given.")
    parser.add_argument("--years", type=float, default=5, help="How many years back to backfill (default 5). Ignored if --since is given.")
    parser.add_argument(
        "--sources", default="hackernews,github_search,github_repos",
        help="Comma-separated subset to run: hackernews, github_search, github_repos",
    )
    parser.add_argument("--hn-window-days", type=int, default=7, help="HN sweep window size in days (default 7)")
    parser.add_argument("--github-search-window-days", type=int, default=30, help="GitHub search sweep window size in days (default 30)")
    args = parser.parse_args()

    try:
        config.validate()
    except ValueError as e:
        logger.error(str(e))
        sys.exit(1)

    until = dt.datetime.now(dt.timezone.utc)
    if args.since:
        since = dt.datetime.strptime(args.since, "%Y-%m-%d").replace(tzinfo=dt.timezone.utc)
    else:
        since = until - dt.timedelta(days=int(args.years * 365.25))

    sources = {s.strip() for s in args.sources.split(",") if s.strip()}
    logger.info("Backfilling %s from %s to %s", ", ".join(sorted(sources)), since.date(), until.date())

    try:
        if "hackernews" in sources:
            backfill_hackernews(since, until, args.hn_window_days)
        if "github_search" in sources:
            backfill_github_search(since, until, args.github_search_window_days)
        if "github_repos" in sources:
            backfill_github_repos(since)
    finally:
        stop_dead_letter()

    logger.info("Backfill complete.")


if __name__ == "__main__":
    main()
