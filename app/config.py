"""
Configuration loading for the extraction service.

This service does ONE job: pull data safely and idempotently from the
source APIs (Hacker News, GitHub) and land it in GCS. It does not
transform, enrich, deduplicate, or score anything - that's a separate
concern, owned by whatever process turns Bronze into Silver/Gold
(dbt, Dataflow, scheduled BigQuery queries, or similar), outside this
codebase entirely.

Filtering is scoped to database design/management tool market
intelligence (for DbSchema), not a general HN/GitHub firehose - see
README "Filtering strategy."

Auth for GCS uses standard Google Application Default Credentials.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import List

from dotenv import load_dotenv

load_dotenv()


def _split_csv(value: str) -> List[str]:
    return [v.strip() for v in value.split(",") if v.strip()]


# Default filter set for the DbSchema / database design & management
# tool market. See README "Filtering strategy" for the reasoning.
_DEFAULT_HN_KEYWORDS = (
    "DbSchema,DBeaver,DataGrip,Navicat,TablePlus,pgAdmin,phpMyAdmin,"
    "MySQL Workbench,DbVisualizer,dbForge,HeidiSQL,Beekeeper Studio,"
    "database design tool,ER diagram tool,database GUI,"
    "schema management tool,schema synchronization,database version control"
)
_DEFAULT_GITHUB_OSS_REPOS = (
    "dbeaver/dbeaver,pgadmin-org/pgadmin4,phpmyadmin/phpmyadmin,"
    "HeidiSQL/HeidiSQL,beekeeper-studio/beekeeper-studio,pgmodeler/pgmodeler"
)
_DEFAULT_GITHUB_SEARCH_KEYWORDS = (
    "DbSchema,DataGrip,Navicat,TablePlus,dbForge,"
    "SQL Server Management Studio,Oracle SQL Developer"
)


@dataclass(frozen=True)
class Config:
    # Which connectors to run: any of "hackernews", "github"
    enabled_sources: List[str] = field(
        default_factory=lambda: _split_csv(os.getenv("ENABLED_SOURCES", "hackernews,github"))
    )

    http_timeout_seconds: float = float(os.getenv("HTTP_TIMEOUT_SECONDS", "15"))
    backoff_initial_seconds: float = float(os.getenv("BACKOFF_INITIAL_SECONDS", "5"))
    backoff_max_seconds: float = float(os.getenv("BACKOFF_MAX_SECONDS", "300"))

    # --- Hacker News (Algolia search, no auth required) ---
    hn_search_keywords: List[str] = field(
        default_factory=lambda: _split_csv(os.getenv("HN_SEARCH_KEYWORDS", _DEFAULT_HN_KEYWORDS))
    )
    hn_poll_interval_seconds: float = float(os.getenv("HN_POLL_INTERVAL_SECONDS", "300"))
    hn_lookback_minutes: int = int(os.getenv("HN_LOOKBACK_MINUTES", "10080"))  # 7 days

    # --- GitHub ---
    github_token: str = os.getenv("GITHUB_TOKEN", "")
    github_oss_repos: List[str] = field(
        default_factory=lambda: _split_csv(os.getenv("GITHUB_OSS_REPOS", _DEFAULT_GITHUB_OSS_REPOS))
    )
    github_search_keywords: List[str] = field(
        default_factory=lambda: _split_csv(os.getenv("GITHUB_SEARCH_KEYWORDS", _DEFAULT_GITHUB_SEARCH_KEYWORDS))
    )
    github_poll_interval_seconds: float = float(os.getenv("GITHUB_POLL_INTERVAL_SECONDS", "300"))
    github_lookback_minutes: int = int(os.getenv("GITHUB_LOOKBACK_MINUTES", "10080"))  # 7 days

    # Mode 3: repo-scoped, categorized production-issue search - see
    # github_client.TARGETED_CATEGORIES for the actual category/repo/
    # keyword definitions (structured, so it lives in code, not here).
    github_targeted_enabled: bool = os.getenv("GITHUB_TARGETED_ENABLED", "true").lower() == "true"

    # --- GCP project (used for GCS) ---
    gcp_project_id: str = os.getenv("GCP_PROJECT_ID", "")

    # --- GCS: Bronze landing + dead letter queue ---
    gcs_bucket: str = os.getenv("GCS_BUCKET", "")
    gcs_bronze_prefix: str = os.getenv("GCS_BRONZE_PREFIX", "bronze")
    gcs_dead_letter_prefix: str = os.getenv("GCS_DEAD_LETTER_PREFIX", "dead_letter")
    gcs_batch_size: int = int(os.getenv("GCS_BATCH_SIZE", "50"))
    gcs_flush_interval_seconds: float = float(os.getenv("GCS_FLUSH_INTERVAL_SECONDS", "30"))

    log_level: str = os.getenv("LOG_LEVEL", "INFO")

    def validate(self) -> None:
        missing = []
        if not self.gcs_bucket:
            missing.append("GCS_BUCKET")
        if not self.enabled_sources:
            missing.append("ENABLED_SOURCES")
        if "hackernews" in self.enabled_sources and not self.hn_search_keywords:
            missing.append("HN_SEARCH_KEYWORDS (required when 'hackernews' is enabled)")
        if "github" in self.enabled_sources and not (self.github_oss_repos or self.github_search_keywords):
            missing.append("GITHUB_OSS_REPOS or GITHUB_SEARCH_KEYWORDS (required when 'github' is enabled)")
        if missing:
            raise ValueError(
                f"Missing required configuration: {', '.join(missing)}. "
                "Set these in your environment or .env file."
            )


config = Config()
