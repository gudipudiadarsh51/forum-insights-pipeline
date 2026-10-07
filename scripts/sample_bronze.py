#!/usr/bin/env python
"""
Pulls a random sample of Bronze records matching a given keyword and
prints their titles/text so you can actually read what a keyword's
"hits" contain - not just count them.

Built specifically to answer questions like: "Navicat has 50,788 hits
over 5 years, DBeaver only has 9,688 - is that real, or is Algolia's
typo-tolerance matching unrelated content?" Counting records can't
answer that; reading them can.

Samples across ALL files for the given source, not just one hour's
worth, so the sample is representative of the whole dataset rather
than whatever happened to land in one file.

Usage:
    python -m scripts.sample_bronze --source hackernews --keyword Navicat
    python -m scripts.sample_bronze --source hackernews --keyword Navicat --sample-size 50
    python -m scripts.sample_bronze --source github_mentions --keyword DataGrip
"""
from __future__ import annotations

import argparse
import gzip
import json
import logging
import random
import sys
from typing import Any, Dict, Iterator, List

from google.cloud import storage

from app.config import config

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("sample_bronze")

_SOURCE_PATHS = {
    "hackernews": "hackernews/items",
    "github_issues": "github/issues",
    "github_comments": "github/comments",
    "github_mentions": "github/mentions",
}


def iter_records(client: storage.Client, bronze_path: str) -> Iterator[Dict[str, Any]]:
    prefix = f"{config.gcs_bronze_prefix}/{bronze_path}/"
    blobs = list(client.list_blobs(config.gcs_bucket, prefix=prefix))
    logger.info("Found %d file(s) under %s - reading all of them...", len(blobs), prefix)
    for i, blob in enumerate(blobs, 1):
        if i % 200 == 0:
            logger.info("  ...read %d/%d files", i, len(blobs))
        try:
            raw = blob.download_as_bytes()
            if blob.name.endswith(".gz"):
                raw = gzip.decompress(raw)
            for line in raw.decode("utf-8").splitlines():
                line = line.strip()
                if line:
                    yield json.loads(line)
        except Exception:
            logger.exception("Failed to read %s - skipping", blob.name)


def matches_keyword(record: Dict[str, Any], keyword: str) -> bool:
    # HN records: matched_keywords is a list. GitHub mention records: matched_keyword is a single string.
    if "matched_keywords" in record:
        return keyword in (record.get("matched_keywords") or [])
    if "matched_keyword" in record:
        return record.get("matched_keyword") == keyword
    return False


def display_text(record: Dict[str, Any]) -> str:
    for field in ("title", "text", "comment_text", "body"):
        value = record.get(field)
        if value:
            return str(value)[:200]
    return "(no title/text/body field found)"


def main() -> None:
    parser = argparse.ArgumentParser(description="Sample Bronze records matching a keyword, for manual inspection.")
    parser.add_argument("--source", required=True, choices=sorted(_SOURCE_PATHS), help="Which Bronze entity to read")
    parser.add_argument("--keyword", required=True, help="Keyword to filter on (exact match against matched_keyword(s))")
    parser.add_argument("--sample-size", type=int, default=30, help="How many matching records to print (default 30)")
    parser.add_argument("--max-scan", type=int, default=200_000, help="Safety cap on total records scanned (default 200,000)")
    args = parser.parse_args()

    client = storage.Client(project=config.gcp_project_id or None)
    bronze_path = _SOURCE_PATHS[args.source]

    matches: List[Dict[str, Any]] = []
    total_scanned = 0
    total_matched = 0

    # Reservoir sampling: keeps a fair random sample without loading
    # everything into memory or needing to know the total count upfront.
    for record in iter_records(client, bronze_path):
        total_scanned += 1
        if total_scanned > args.max_scan:
            logger.warning("Hit --max-scan cap of %d records - stopping early. Counts below are partial.", args.max_scan)
            break
        if matches_keyword(record, args.keyword):
            total_matched += 1
            if len(matches) < args.sample_size:
                matches.append(record)
            else:
                j = random.randint(0, total_matched - 1)
                if j < args.sample_size:
                    matches[j] = record

    print()
    print(f"Scanned {total_scanned:,} record(s) in {args.source}, found {total_matched:,} matching {args.keyword!r}.")
    print(f"Showing a random sample of {len(matches)}:")
    print("=" * 100)
    for i, record in enumerate(matches, 1):
        print(f"[{i}] id={record.get('id')} | {display_text(record)}")
    print("=" * 100)

    if total_matched == 0:
        print(f"No records matched {args.keyword!r} - check spelling/casing, it must match exactly.")


if __name__ == "__main__":
    main()