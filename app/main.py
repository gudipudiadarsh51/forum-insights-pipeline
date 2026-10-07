"""Entry point for the Reddit ingestion service."""
from __future__ import annotations

import logging
import signal
import sys

from .config import config
from .ingest import IngestionService


def setup_logging() -> None:
    logging.basicConfig(
        level=getattr(logging, config.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)-8s [%(threadName)s] %(name)s: %(message)s",
    )
    # PRAW/prawcore are chatty at DEBUG; keep them at INFO+ unless we're debugging.
    logging.getLogger("prawcore").setLevel(logging.INFO)


def main() -> None:
    setup_logging()
    logger = logging.getLogger(__name__)

    try:
        config.validate()
    except ValueError as e:
        logger.error(str(e))
        sys.exit(1)

    service = IngestionService()

    def handle_signal(signum, frame):
        logger.info("Received signal %s", signum)
        service.stop()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    logger.info("Starting Reddit ingestion service")
    service.run()
    logger.info("Reddit ingestion service stopped")


if __name__ == "__main__":
    main()
