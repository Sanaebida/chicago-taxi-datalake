"""Standalone CLI to download a slice of the dataset to local CSV files.

The Dagster ``bronze_taxi_trips`` asset uses the very same ``fetch_pages``
function, so this script is mainly here for reproducibility / exploration
outside the orchestrator:

    python -m taxi_pipeline.ingestion.download \
        --start 2023-01-01 --end 2023-04-01 --out ./data/raw [--max-rows-per-month 200000]
"""

from __future__ import annotations

import argparse
import logging
from datetime import datetime
from pathlib import Path

from ..config import get_settings
from ..partitions import month_windows
from .soda import fetch_pages

log = logging.getLogger("taxi_pipeline.download")


def main() -> None:
    settings = get_settings()
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("--start", default=settings.period_start, help="inclusive, YYYY-MM-DD")
    parser.add_argument("--end", default=settings.period_end, help="exclusive, YYYY-MM-DD")
    parser.add_argument("--out", default="./data/raw", type=Path)
    parser.add_argument("--page-size", default=settings.soda_page_size, type=int)
    parser.add_argument("--max-rows-per-month", default=settings.ingest_max_rows_per_partition, type=int)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    start = datetime.fromisoformat(args.start)
    end = datetime.fromisoformat(args.end)

    for m_start, m_end in month_windows(start, end):
        target = args.out / f"trip_month={m_start:%Y-%m}"
        target.mkdir(parents=True, exist_ok=True)
        total = 0
        for page in fetch_pages(
            settings.soda_base_url, m_start, m_end, page_size=args.page_size,
            max_rows=args.max_rows_per_month, app_token=settings.soda_app_token,
            workers=settings.soda_workers,
        ):
            (target / f"page_{page.index:05d}.csv").write_bytes(page.content)
            total += page.n_rows
        log.info("%s: %s rows written to %s", f"{m_start:%Y-%m}", total, target)


if __name__ == "__main__":
    main()
