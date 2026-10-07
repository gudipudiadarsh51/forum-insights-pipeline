"""
Extraction service. This is the entire job of this codebase: pull data
safely and idempotently from the source APIs and land it in GCS. There
is no transformation, enrichment, deduplication, or scoring here - a
separate process, outside this codebase, is responsible for turning
Bronze into Silver/Gold.

"Safely" means two things in practice:
  - One bad record (a missing field, an unexpected shape) never takes
    down a whole poll cycle or costs the good records collected
    alongside it. It's caught at the point it's shaped and routed to
    the dead letter queue (app/dead_letter.py); everything else in that
    cycle proceeds normally.
  - A source or GCS having a bad moment doesn't crash the service -
    each polling loop retries with backoff independently of the others.

"Idempotently" means a restart doesn't reprocess data it already saw,
nor does it skip data it hasn't seen yet - both connectors persist a
cursor (app/state.py) and only ever ask their source for what's new
since that cursor.

Shutdown budget: Cloud Run (worker pools or services) sends SIGTERM,
then SIGKILL after a FIXED 10 seconds - not configurable. _SHUTDOWN_BUDGET_SECONDS
below is the total time every cleanup step (joining poll threads,
flushing every GCS writer, flushing the dead letter queue) is allowed
to take, combined - a shared deadline, not a per-step allowance, so N
writers don't each get their own timeout and sum to way more than
Cloud Run will ever wait. Set comfortably under 10s to leave margin for
Python interpreter teardown and signal-handling overhead.
"""
from __future__ import annotations

import datetime as dt
import logging
import random
import threading
import time
from typing import Callable, Dict, List

from . import dead_letter, github_client, hn_client, state
from .config import config
from .gcs_writer import GCSBatchWriter
from .raw_extract import (
    raw_github_comment_dict,
    raw_github_issue_dict,
    raw_github_mention_dict,
    raw_github_targeted_dict,
    raw_hn_hit_dict,
)

logger = logging.getLogger(__name__)

_SHUTDOWN_BUDGET_SECONDS = 7.0


class IngestionService:
    def __init__(self) -> None:
        self._stop_event = threading.Event()
        self._threads: List[threading.Thread] = []
        self._writers: List[GCSBatchWriter] = []

    def stop(self) -> None:
        logger.info("Stop requested; shutting down extraction loops...")
        self._stop_event.set()

    def run(self) -> None:
        if not config.enabled_sources:
            raise ValueError("No sources enabled. Set ENABLED_SOURCES.")
        logger.info("Enabled sources: %s", ", ".join(config.enabled_sources))

        if "hackernews" in config.enabled_sources:
            hn_writer = GCSBatchWriter("hackernews/items")
            self._writers.append(hn_writer)
            logger.info("HN keywords: %s", ", ".join(config.hn_search_keywords))
            self._threads.append(threading.Thread(
                target=self._run_with_backoff, args=(lambda: self._poll_hackernews(hn_writer),),
                name="hackernews", daemon=True,
            ))

        if "github" in config.enabled_sources:
            if config.github_oss_repos:
                issues_writer = GCSBatchWriter("github/issues")
                comments_writer = GCSBatchWriter("github/comments")
                self._writers.extend([issues_writer, comments_writer])
                logger.info("GitHub OSS repos: %s", ", ".join(config.github_oss_repos))
                self._threads.append(threading.Thread(
                    target=self._run_with_backoff,
                    args=(lambda: self._poll_github_repos(issues_writer, comments_writer),),
                    name="github-repos", daemon=True,
                ))
            if config.github_search_keywords:
                mentions_writer = GCSBatchWriter("github/mentions")
                self._writers.append(mentions_writer)
                logger.info("GitHub search keywords: %s", ", ".join(config.github_search_keywords))
                self._threads.append(threading.Thread(
                    target=self._run_with_backoff, args=(lambda: self._poll_github_mentions(mentions_writer),),
                    name="github-search", daemon=True,
                ))
            if config.github_targeted_enabled and github_client.TARGETED_CATEGORIES:
                targeted_writer = GCSBatchWriter("github/targeted")
                self._writers.append(targeted_writer)
                logger.info(
                    "GitHub targeted categories: %s",
                    ", ".join(c.name for c in github_client.TARGETED_CATEGORIES),
                )
                self._threads.append(threading.Thread(
                    target=self._run_with_backoff, args=(lambda: self._poll_github_targeted(targeted_writer),),
                    name="github-targeted", daemon=True,
                ))

        for writer in self._writers:
            writer.start()
        for t in self._threads:
            t.start()

        try:
            # wait(1) wakes immediately once stop() sets the event, unlike
            # sleep(1) which would add up to a full second of pure lag
            # before shutdown even starts - worth avoiding when every
            # second counts against a fixed 10s SIGTERM-to-SIGKILL window.
            while not self._stop_event.is_set():
                self._stop_event.wait(1)
        except KeyboardInterrupt:
            self.stop()
        finally:
            deadline = time.monotonic() + _SHUTDOWN_BUDGET_SECONDS

            # Poll threads first, but only briefly - they're daemon
            # threads with no durability risk of their own (an in-flight
            # API call just gets abandoned and re-fetched next poll).
            # The buffered data in the writers below is what actually
            # matters, so threads get a small slice of the budget, not
            # an open-ended wait.
            for t in self._threads:
                t.join(timeout=max(0.0, min(1.0, deadline - time.monotonic())))

            # Writers get whatever's left of the shared deadline, split
            # across however many remain - this bounds the TOTAL time
            # spent here, rather than giving each writer its own timeout
            # and letting them sum past what Cloud Run will ever wait.
            # Recomputed each iteration so time already spent on earlier
            # writers reduces what's left for the rest, fairly.
            #
            # Each writer's total time is thread_join_timeout PLUS
            # upload_timeout, combined - both must come out of that
            # writer's slice, or a fixed upload timeout could let every
            # writer independently burn its own full allotment regardless
            # of how little of the shared deadline is actually left.
            for i, writer in enumerate(self._writers):
                remaining = max(0.0, deadline - time.monotonic())
                writers_left = len(self._writers) - i
                per_writer_budget = max(0.2, min(2.0, remaining / writers_left))
                join_slice = min(0.3, per_writer_budget * 0.2)
                upload_slice = max(0.1, per_writer_budget - join_slice)
                writer.stop(thread_join_timeout=join_slice, upload_timeout=upload_slice)

            dead_letter.stop_all(deadline=deadline)

            elapsed = _SHUTDOWN_BUDGET_SECONDS - max(0.0, deadline - time.monotonic())
            logger.info("Shutdown cleanup finished in %.1fs (budget was %.1fs)", elapsed, _SHUTDOWN_BUDGET_SECONDS)

    # -- internal ---------------------------------------------------------

    def _run_with_backoff(self, target: Callable[[], None]) -> None:
        """Runs `target` (expected to loop forever) with reconnect/retry backoff on error."""
        backoff = config.backoff_initial_seconds
        while not self._stop_event.is_set():
            try:
                target()
                backoff = config.backoff_initial_seconds  # target returned cleanly (stop requested)
            except Exception:
                logger.exception("Unexpected error in %s, retrying in %.1fs", target, backoff)
                if self._stop_event.is_set():
                    break
                sleep_for = backoff + random.uniform(0, backoff * 0.25)
                time.sleep(sleep_for)
                backoff = min(backoff * 2, config.backoff_max_seconds)

    # -- Hacker News --------------------------------------------------------

    def _poll_hackernews(self, writer: GCSBatchWriter) -> None:
        default_since = int(time.time()) - config.hn_lookback_minutes * 60

        while not self._stop_event.is_set():
            merged: Dict[str, Dict] = {}  # objectID -> raw dict, deduped across keywords this cycle

            for keyword in config.hn_search_keywords:
                cursor_key = f"hn_search_since_{keyword}"
                since_epoch = int(state.get_state(cursor_key) or default_since)
                hits = hn_client.search_since(keyword, since_epoch)

                latest = since_epoch
                for hit in hits:
                    try:
                        raw = raw_hn_hit_dict(hit, keyword)
                    except Exception as e:
                        dead_letter.send("hackernews", hit, e, context={"keyword": keyword})
                        continue

                    existing = merged.get(raw["id"])
                    if existing:
                        existing["matched_keywords"] = sorted(set(existing["matched_keywords"]) | {keyword})
                    else:
                        merged[raw["id"]] = raw
                    latest = max(latest, hit.get("created_at_i", latest))

                if hits:
                    logger.info("HN keyword %r: %d hit(s)", keyword, len(hits))
                state.set_state(cursor_key, latest)

            for raw in merged.values():
                writer.add(raw)

            self._stop_event.wait(config.hn_poll_interval_seconds)

    # -- GitHub: repo-scoped (open-source competitors) -----------------------

    def _poll_github_repos(self, issues_writer: GCSBatchWriter, comments_writer: GCSBatchWriter) -> None:
        while not self._stop_event.is_set():
            for repo_full in config.github_oss_repos:
                owner, repo = repo_full.split("/", 1)
                try:
                    self._poll_github_repo(owner, repo, issues_writer, comments_writer)
                except Exception:
                    logger.exception("Failed to poll GitHub repo %s - continuing with other repos", repo_full)
            self._stop_event.wait(config.github_poll_interval_seconds)

    def _poll_github_repo(self, owner: str, repo: str, issues_writer: GCSBatchWriter, comments_writer: GCSBatchWriter) -> None:
        default_since = (
            dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=config.github_lookback_minutes)
        ).isoformat()

        issues_cursor_key = f"github_issues_since_{owner}_{repo}"
        issues_since = state.get_state(issues_cursor_key) or default_since
        latest_issue_ts, issue_count = issues_since, 0
        for issue in github_client.list_issues(owner, repo, since=issues_since):
            try:
                issues_writer.add(raw_github_issue_dict(issue, owner, repo))
            except Exception as e:
                dead_letter.send("github_issues", issue, e, context={"repo": f"{owner}/{repo}"})
                continue
            latest_issue_ts = max(latest_issue_ts, issue.get("updated_at") or latest_issue_ts)
            issue_count += 1
        if issue_count:
            logger.info("GitHub %s/%s: %d issue(s)/PR(s) updated", owner, repo, issue_count)
            state.set_state(issues_cursor_key, latest_issue_ts)

        comments_cursor_key = f"github_comments_since_{owner}_{repo}"
        comments_since = state.get_state(comments_cursor_key) or default_since
        latest_comment_ts, comment_count = comments_since, 0
        for comment in github_client.list_issue_comments(owner, repo, since=comments_since):
            try:
                comments_writer.add(raw_github_comment_dict(comment, owner, repo))
            except Exception as e:
                dead_letter.send("github_comments", comment, e, context={"repo": f"{owner}/{repo}"})
                continue
            latest_comment_ts = max(latest_comment_ts, comment.get("updated_at") or latest_comment_ts)
            comment_count += 1
        if comment_count:
            logger.info("GitHub %s/%s: %d comment(s) updated", owner, repo, comment_count)
            state.set_state(comments_cursor_key, latest_comment_ts)

    # -- GitHub: site-wide search (closed-source competitors) ---------------

    def _poll_github_mentions(self, writer: GCSBatchWriter) -> None:
        default_since = (
            dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=config.github_lookback_minutes)
        ).isoformat()

        while not self._stop_event.is_set():
            for keyword in config.github_search_keywords:
                cursor_key = f"github_mentions_since_{keyword}"
                since_iso = state.get_state(cursor_key) or default_since
                latest_ts, count = since_iso, 0
                try:
                    for item in github_client.search_issues(keyword, since_iso):
                        try:
                            writer.add(raw_github_mention_dict(item, keyword))
                        except Exception as e:
                            dead_letter.send("github_mentions", item, e, context={"keyword": keyword})
                            continue
                        latest_ts = max(latest_ts, item.get("updated_at") or latest_ts)
                        count += 1
                except Exception:
                    logger.exception("Failed to search GitHub for keyword %r - continuing with other keywords", keyword)
                    continue
                if count:
                    logger.info("GitHub search %r: %d mention(s)", keyword, count)
                    state.set_state(cursor_key, latest_ts)
            self._stop_event.wait(config.github_poll_interval_seconds)

    # -- GitHub: repo-scoped, categorized production-issue search -----------

    def _poll_github_targeted(self, writer: GCSBatchWriter) -> None:
        default_since = (
            dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=config.github_lookback_minutes)
        ).isoformat()

        while not self._stop_event.is_set():
            for category in github_client.TARGETED_CATEGORIES:
                for repo in category.repos:
                    cursor_key = f"github_targeted_since_{category.name}_{repo}"
                    since_iso = state.get_state(cursor_key) or default_since
                    latest_ts, count = since_iso, 0
                    try:
                        for item in github_client.search_targeted(category, repo, since_iso):
                            try:
                                writer.add(raw_github_targeted_dict(item, category.name))
                            except Exception as e:
                                dead_letter.send(
                                    "github_targeted", item, e, context={"category": category.name, "repo": repo}
                                )
                                continue
                            latest_ts = max(latest_ts, item.get("updated_at") or latest_ts)
                            count += 1
                    except Exception:
                        logger.exception(
                            "Failed to search GitHub targeted category %r on %s - continuing",
                            category.name, repo,
                        )
                        continue
                    if count:
                        logger.info("GitHub targeted %s/%s: %d hit(s)", category.name, repo, count)
                        state.set_state(cursor_key, latest_ts)
            self._stop_event.wait(config.github_poll_interval_seconds)
