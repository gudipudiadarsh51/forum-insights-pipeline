# 0000. Use Architecture Decision Records

Date: 2026-10-08

## Status

Accepted

## Context

This project has already made a number of significant technical decisions that
aren't visible from reading the code alone — why GitHub's Search API is used
for some repos and full repo-scoped polling for others, why backfills are
windowed instead of run as one wide query, why certain query filters that
looked reasonable on paper were removed after they turned out to silently
discard most of the real data. Several of these decisions were only reachable
by testing live, production data directly against the real GitHub and
Algolia APIs — not something a future reader (including a future version of
the person who built this) could rediscover just by reading the source.

Without a record, decisions like these tend to get re-litigated, silently
reversed by someone who doesn't know why a thing was done a certain way, or
just forgotten until the same mistake happens again. This has already nearly
happened once in this project's own history (the label-taxonomy filter
removal, see ADR 0001) — a decision that would have otherwise been invisible
once sitting quietly in the code.

## Decision

Record every decision with non-obvious reasoning, a real tradeoff, or a
finding backed by actually testing something (not just code style or naming
preferences) as a lightweight ADR in `docs/adr/`, numbered sequentially,
using this template:

- **Status** — Proposed, Accepted, Superseded (with a link to what
  superseded it), or Deprecated.
- **Context** — what prompted the decision, including concrete evidence
  where it exists (a real API response, a measured number, a reproduction
  of a bug), not just a description of the general problem area.
- **Decision** — what was actually decided, stated plainly.
- **Consequences** — what this makes easier, what it makes harder, and
  what it doesn't solve.

ADRs are never edited after being accepted. If a decision changes, write a
new ADR that supersedes the old one and update the old one's status — the
history of "we used to do X, then switched to Y because Z" is itself
valuable and shouldn't be erased.

## Consequences

Every future decision costs a few extra minutes to write down. In exchange,
anyone picking this project up later — including its own author, months
from now — can see not just what the code does, but why it doesn't do the
more obvious-looking alternative, without having to re-derive it from
scratch or re-discover the same bug a second time.
