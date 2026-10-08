# 0001. Use GitHub's Search API for targeted production-issue discovery

Date: 2026-10-08

## Status

Accepted

## Context

The pipeline already does full, unfiltered repo-scoped polling for a core
set of competitor repos (`GITHUB_OSS_REPOS`) — every issue and every comment,
no filtering. That works well for a small, fixed set of repos the project
cares about completely, but doesn't scale to a broader set of repos (desktop
GUI clients, migration CLIs, ORMs, governance platforms) where the goal is
narrower: find production-grade failure reports (memory limits, locks,
timeouts, schema drift, rollout failures), not ingest everything.

Two options were available for this broader set: (a) extend full polling to
every new repo and filter downstream in SQL, or (b) use GitHub's Search API
with repo scoping and keyword/qualifier filters, fetching only what matches
at extraction time. Full polling was rejected for the new repos specifically
because several of them (`prisma/orm`, `liquibase/liquibase`) are large
enough that full historical polling would ingest orders of magnitude more
data than the targeted categories actually need, for repos that are not the
project's core competitor set the way the original six are.

## Decision

Use GitHub's Search API (`/search/issues`), repo-scoped per category, for
the new set of production-issue-focused repos. Keep full polling only for
the original, small, fully-trusted competitor set.

Several real constraints were discovered while building this, each of which
shaped the final implementation:

- **The Search API matches on title, body, *and* comments by default, but
  only ever returns the issue's own title/body** — never the comment that
  actually matched. A hit can be comment-driven with nothing relevant
  visible in the stored record unless comments are fetched separately per
  match (`search_targeted_with_comments`, built specifically for this).
- **The `repo:` qualifier does not follow repository renames**, unlike the
  website and most other REST endpoints. `repo:prisma/prisma` 422'd with a
  misleading "doesn't exist" error on every single query for weeks, because
  the repo had been renamed to `prisma/orm` — confirmed by reproducing the
  exact failing query directly against the live API and cross-checking
  GitHub's own org listing.
- **Label taxonomy is not portable across repos.** A single hardcoded
  `label:bug,regression` filter, applied identically across all ten target
  repos, was confirmed (by testing the same query with and without the
  filter) to silently reduce real matches from 978 to 0 in `prisma/orm`,
  236 to 0 in `liquibase/liquibase`, and 96 to 0 in `bytebase/bytebase` —
  because none of those repos actually use labels named `bug` or
  `regression`. The filter was removed from the shared default entirely
  rather than patched per-repo, since the same assumption was never
  validated for any of the ten repos individually.
- The Search API has its own, stricter rate limit (30 requests/minute) than
  general REST API usage, which is why targeted search is windowed the same
  way the original mentions search already was (see ADR 0002).

## Consequences

Targeted-category extraction is cheap relative to full polling and scales
to many more repos without ingesting data the project doesn't need. The
cost is that it inherits every quirk of GitHub's search query language —
rename blindness, taxonomy assumptions, query-length and operator limits —
none of which show up as a code bug, only as silently wrong or missing
data, discoverable only by testing real queries against the real API and
comparing counts before and after a filter change. Any future filter added
to `TargetedCategory.extra_filters` should be validated the same way
(unfiltered count vs. filtered count, on the actual target repo) before
being trusted, not assumed correct because it looks reasonable.
