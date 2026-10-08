# 0002. Windowed backfill strategy (30 days for GitHub, 7 days for HN)

Date: 2026-10-08

## Status

Accepted

## Context

Both of this pipeline's search-based sources — GitHub's Search API and
Algolia's HN Search API — cap the total number of results a single query
can return (GitHub's search endpoint hard-caps at 1,000 results regardless
of pagination). A historical backfill spanning years, run as one query per
keyword or category with no date bound, would silently truncate for any
keyword or repo popular enough to exceed that cap — the query would succeed,
return a full page of results, and give no indication that anything was
left out. This is exactly the kind of failure that doesn't show up as an
error: the job reports success, and only a result-count mismatch against
what's actually expected would reveal the gap.

This isn't hypothetical for this project: several of the busier keywords
encountered during the original HN backfill research returned result counts
in the tens of thousands, far past what a single unbounded query could
return completely.

## Decision

Sweep historical backfills as a sequence of fixed-size date windows instead
of one unbounded query: 30 days per window for GitHub-based sources
(`github_search`, `github_targeted`), 7 days for HN, each requested with an
explicit `since`/`until` boundary, swept sequentially from the backfill's
start date to now. A fixed pacing delay is inserted between windows to stay
under each API's per-minute rate limit (GitHub's search endpoint specifically
caps at 30 requests/minute, stricter than general REST API usage).

Each window's failures are caught and logged individually
(`except Exception: continue`) rather than allowed to abort the whole
backfill — confirmed in practice when `repo:prisma/prisma` 422'd on every
single window for a full 5-year sweep (see ADR 0001): the backfill
completed cleanly, logged every failure clearly, and the one broken repo
was fixable and re-run in isolation (via the `--repo` filter added
specifically for this) without re-running or re-duplicating the repos that
had already succeeded.

## Consequences

A full historical backfill requires far more individual API calls and takes
longer in wall-clock time than a single wide query would, if that wide
query actually worked. In exchange, no window can silently truncate
results regardless of how popular a keyword or how busy a repo turns out to
be, and a failure in one window or one repo is isolated, loud, and
independently re-runnable rather than threatening the completeness of the
entire sweep. The tradeoff is further duplication risk on reruns, since
Bronze has no cursor memory of previous backfill runs (by design — see the
backfill script's own docstring) — scoping reruns narrowly, as with
`--repo`, is the mitigation, not an automatic cursor.
