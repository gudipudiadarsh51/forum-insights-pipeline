# 0003. `github_targeted` intentionally diverges from the issues/comments schema

Date: 2026-10-08

## Status

Accepted

## Context

The original repo-scoped poller writes issues and comments to two separate
Bronze prefixes (`github/issues`, `github/comments`), keyed so they can be
rejoined downstream — `silver_github_threads.sql` does exactly this, joining
`stg_github_issues` to `stg_github_comments` on `repo` + issue number to
reassemble a thread.

When `github_targeted` was built, that same separate-bucket pattern was
raised as a concern: it forces every downstream consumer to perform a join
just to see a complete thread, and a thread reassembled this way is only as
complete as the join's correctness. For `github_targeted` specifically,
there was also a sharper, concrete reason to avoid it: GitHub's Search API
matches on title, body, *and* comments by default, but only ever returns
the issue's own title/body in the response (see ADR 0001) — meaning a
match can be comment-driven with nothing in the issue record itself to
explain why it matched, unless the comments are fetched and attached at
extraction time.

Comparing the two schemas directly (field-by-field, not just by
description) surfaced two differences: `github_targeted` has two fields
the old schema doesn't (`category`, and a nested `comments` array), and the
old schema had one field (`closed_at`) that `github_targeted` was missing —
not for any principled reason, just an oversight caught during the
comparison.

## Decision

Keep `github_targeted` structurally different from `github_issues`/
`github_comments` by design, rather than forcing it to match:

- `github_targeted` embeds each match's full comment thread as a nested
  `comments` array in the same record, assembled at extraction time via
  `search_targeted_with_comments()`. No separate comments table, no join
  required to read a complete thread for this source.
- The 14 fields that do overlap (`id`, `repo`, `number`,
  `is_pull_request`, `title`, `body`, `state`, `author`, `labels`,
  `comments_count`, `url`, `created_at`, `updated_at`, `ingested_at`) use
  identical names and meanings across both sources — divergence is
  deliberate only where there's a real reason for it (the comments
  nesting), not incidental.
- `closed_at` was added to `raw_github_targeted_dict()` to close the one
  field gap that had no justification for existing.

Schema unification between the two sources, if it's ever needed for a
unified Gold-layer view, happens at the Silver/Gold boundary — either by
flattening `github_targeted`'s nested comments into separate rows to match
the old shape, or by assembling the old issues+comments pair into the same
nested-thread shape `github_targeted` already uses. This decision does not
pick which direction; it only establishes that extraction-time shape should
not be forced to match just for the sake of matching.

## Consequences

Each source's raw data is shaped for what that source actually needs
(`github_targeted` is self-contained per record; the OSS-repo poller
benefits from the efficiency of a single repo-wide comments stream, which
nesting would give up). The cost is that no single staging model can
currently read both sources with one `SELECT *` — `stg_github_targeted.sql`
has to be its own model, not a trivial extension of `stg_github_issues.sql`,
and whoever eventually builds a unified Gold view needs to make an explicit
choice about which shape wins, rather than assuming the two sources were
ever meant to line up for free.
