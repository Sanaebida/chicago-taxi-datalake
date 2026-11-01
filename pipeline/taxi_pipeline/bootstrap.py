"""One-shot initialisation of the datalake (run by the `datalake-init` service).

Waits for the object store to be reachable and creates the datalake bucket.
Idempotent: safe to run at every `docker compose up`.
"""

from __future__ import annotations

import logging
import sys
import time

from .config import get_settings
from .storage import S3ObjectStore

log = logging.getLogger("taxi_pipeline.bootstrap")


def main(max_wait_s: int = 120) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    settings = get_settings()
    if not settings.is_s3:
        log.info("Datalake on local filesystem (%s): nothing to do", settings.datalake_root)
        return 0

    client = S3ObjectStore(settings).client
    deadline = time.monotonic() + max_wait_s
    while True:
        try:
            existing = {b["Name"] for b in client.list_buckets().get("Buckets", [])}
            break
        except Exception as exc:  # noqa: BLE001 - endpoint not ready yet
            if time.monotonic() > deadline:
                log.error("Object store %s unreachable: %s", settings.s3_endpoint, exc)
                return 1
            log.info("Waiting for object store %s ...", settings.s3_endpoint)
            time.sleep(3)

    if settings.bucket in existing:
        log.info("Bucket '%s' already exists", settings.bucket)
    else:
        client.create_bucket(Bucket=settings.bucket)
        log.info("Bucket '%s' created", settings.bucket)
    return 0


if __name__ == "__main__":
    sys.exit(main())
