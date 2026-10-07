# DbSchema Market Intelligence — Extraction Service

This service does exactly one job: **pull data safely and idempotently
from Hacker News and GitHub, and land it in Google Cloud Storage.**
Nothing here transforms, cleans, deduplicates, or scores anything.

```
Hacker News (Algolia search, keyword-filtered)   --\
                                                     >-- Bronze (GCS)
GitHub (repo-scoped OSS repos + site-wide search) --/
                                                        \
                              records that fail          -> Dead Letter Queue (GCS)
                              validation during
                              extraction
```

Turning Bronze into Silver/Gold (typing, deduplication, sentiment
scoring, analysis views) is a **separate process, outside this
codebase**, that reads from GCS on its own schedule - dbt, Dataflow,
scheduled BigQuery queries, or whatever fits your stack. This repo has
no BigQuery dependency at all.

## What "safely and idempotently" means here

**Safely:**
- A single malformed record (missing a required field, an unexpected
  shape from the API) is caught at the point it's shaped and routed to
  the dead letter queue - it never crashes a poll cycle or costs the
  good records collected alongside it.
- A failed GCS upload puts the batch back in the buffer instead of
  discarding it - a network blip costs a delay, not data. Uploads retry
  up to 4 times with backoff before falling back to that.
- Each connector's polling loop retries independently with exponential
  backoff on error, so one source having a bad moment doesn't affect
  the others.

**Idempotently:**
- Both connectors persist a cursor in GCS (`gs://<bucket>/_state/*.json`)
  and only ever ask their source for what's new since that cursor - a
  restart doesn't reprocess old data or skip a gap.
- Bronze is append-only NDJSON files - re-running never overwrites
  anything, and downstream deduplication (by record id) is expected to
  happen in the transformation layer, not here.

## Dead letter queue

Records that fail shaping/validation land at:
```
gs://<bucket>/<GCS_DEAD_LETTER_PREFIX>/<source>/dt=.../hour=.../*.jsonl.gz
```

Each entry is the original raw payload plus the error type, message,
a short traceback, and context (which keyword/repo it came from). This
is meant to be inspected and, if a fix is warranted, reprocessed later
- it is not swallowed or logged-and-forgotten.

Sources currently write to: `hackernews`, `github_issues`,
`github_comments`, `github_mentions` - matching the Bronze entity each
came from.

## Filtering strategy

Every connector is scoped to a specific filter - keywords or a curated
repo list - so it never touches, stores, or spends rate-limit budget on
irrelevant data.

**Brand**: `DbSchema`

**Open-source competitors** (own repo, polled directly for issues/comments):
DBeaver, pgAdmin, phpMyAdmin, HeidiSQL, Beekeeper Studio, pgModeler

**Closed-source competitors** (no repo of their own; reachable only via
search): DataGrip, Navicat, TablePlus, dbForge, SQL Server Management
Studio, Oracle SQL Developer

**Category/positioning terms**: database design tool, ER diagram tool,
database GUI, schema management tool, schema synchronization, database
version control

Deliberately excluded: bare database *engine* names (PostgreSQL, MySQL,
MongoDB alone) - these pull in enormous volumes of unrelated backend
engineering discussion.

All configurable in `.env` - `HN_SEARCH_KEYWORDS`, `GITHUB_OSS_REPOS`,
`GITHUB_SEARCH_KEYWORDS`.

## Sources in detail

### Hacker News
Algolia HN Search API (`hn.algolia.com/api`), not the raw Firebase item
feed - genuine full-text search with a date filter, so one query per
keyword returns only matching stories/comments created since the last
check. No auth needed.

### GitHub
Two modes:
- **Repo-scoped** (`GITHUB_OSS_REPOS`): polls each open-source
  competitor's own `issues` and `issues/comments` endpoints with a
  `since` cursor.
- **Site-wide search** (`GITHUB_SEARCH_KEYWORDS`): GitHub's Search API
  (`/search/issues`) for closed-source competitors with no repo of
  their own - catches mentions of them wherever they appear on GitHub.
  Separate, stricter rate limit (30/min authenticated), caps at 1,000
  results/query - a non-issue at niche-keyword volume.

## 1. Set up GCP resources

```bash
gsutil mb -l US gs://your-bucket-name

gcloud iam service-accounts create market-intel-extract
gcloud projects add-iam-policy-binding YOUR_PROJECT \
  --member="serviceAccount:market-intel-extract@YOUR_PROJECT.iam.gserviceaccount.com" \
  --role="roles/storage.objectAdmin"

gcloud iam service-accounts keys create gcp-key.json \
  --iam-account=market-intel-extract@YOUR_PROJECT.iam.gserviceaccount.com
```

Note this service only ever needs GCS access - no BigQuery IAM roles
are required here.

## 2. Configure

```bash
cp .env.example .env
# fill in GCS_BUCKET, GCP_PROJECT_ID, GITHUB_TOKEN
```

## 3. Run

```bash
docker compose up --build -d ingest
docker compose logs -f ingest
```

Or locally without Docker:

```bash
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
export GOOGLE_APPLICATION_CREDENTIALS=./gcp-key.json
python -m app.main
```

## Data layout

**Bronze**: `gs://<bucket>/bronze/hackernews/items/...`,
`gs://<bucket>/bronze/github/issues/...`,
`gs://<bucket>/bronze/github/comments/...`,
`gs://<bucket>/bronze/github/mentions/...` - raw fields as returned by
each API, gzip NDJSON, one file per flush.

**Dead letter queue**: `gs://<bucket>/dead_letter/<source>/...` - same
NDJSON format, one entry per failed record with its error and context.

## Configuration reference (env vars)

| Variable | Required | Description |
|---|---|---|
| `ENABLED_SOURCES` | no | default `hackernews,github` |
| `HN_SEARCH_KEYWORDS` | yes, if hackernews enabled | comma-separated; one Algolia search per entry |
| `HN_POLL_INTERVAL_SECONDS` / `HN_LOOKBACK_MINUTES` | no | defaults `300` / `10080` (7 days) |
| `GITHUB_TOKEN` | recommended | raises core API rate limit to 5,000/hr |
| `GITHUB_OSS_REPOS` | at least one of these two, if github enabled | comma-separated `owner/repo` |
| `GITHUB_SEARCH_KEYWORDS` | ^ | comma-separated |
| `GITHUB_POLL_INTERVAL_SECONDS` / `GITHUB_LOOKBACK_MINUTES` | no | defaults `300` / `10080` |
| `GCP_PROJECT_ID` | no | inferred from credentials if unset |
| `GCS_BUCKET` | yes | Bronze + dead letter bucket |
| `GCS_BRONZE_PREFIX` | no | default `bronze` |
| `GCS_DEAD_LETTER_PREFIX` | no | default `dead_letter` |
| `GCS_BATCH_SIZE` / `GCS_FLUSH_INTERVAL_SECONDS` | no | flush triggers, both writers |
| `BACKOFF_INITIAL_SECONDS` / `BACKOFF_MAX_SECONDS` | no | retry backoff for both connectors |
| `LOG_LEVEL` | no | e.g. `INFO`, `DEBUG` |

## Explicitly out of scope for this codebase

- Sentiment scoring
- Deduplication / MERGE logic
- BigQuery schemas, datasets, tables
- Any Silver/Gold layer or analysis views

These live in whatever separate process consumes Bronze from GCS.
