"""Tiny object-store abstraction used for raw (byte-level) writes.

Spark talks to the datalake directly through the s3a:// connector. This module
is only used where we need to land *bytes as-is* (bronze ingestion, manifests,
reference seeds) without going through Spark. Two backends:

* S3/MinIO via boto3 (Docker Compose stack)
* local filesystem (unit tests / offline runs)
"""

from __future__ import annotations

import json
import shutil
from abc import ABC, abstractmethod
from pathlib import Path
from typing import BinaryIO

from .config import Settings


class ObjectStore(ABC):
    @abstractmethod
    def put_bytes(self, key: str, data: bytes) -> None: ...

    @abstractmethod
    def put_fileobj(self, key: str, fileobj: BinaryIO) -> None: ...

    @abstractmethod
    def list_keys(self, prefix: str) -> list[str]: ...

    @abstractmethod
    def delete_prefix(self, prefix: str) -> int: ...

    def put_json(self, key: str, payload: dict) -> None:
        self.put_bytes(key, json.dumps(payload, indent=2, default=str).encode("utf-8"))


class S3ObjectStore(ObjectStore):
    def __init__(self, settings: Settings):
        import boto3
        from botocore.config import Config

        self.bucket = settings.bucket
        self._prefix_root = settings.datalake_root.split("://", 1)[1].split("/", 1)[1:] or [""]
        self.client = boto3.client(
            "s3",
            endpoint_url=settings.s3_endpoint,
            aws_access_key_id=settings.s3_access_key,
            aws_secret_access_key=settings.s3_secret_key,
            region_name="us-east-1",
            config=Config(signature_version="s3v4", retries={"max_attempts": 5}),
        )

    def _k(self, key: str) -> str:
        root = self._prefix_root[0].strip("/")
        return f"{root}/{key.lstrip('/')}" if root else key.lstrip("/")

    def put_bytes(self, key: str, data: bytes) -> None:
        self.client.put_object(Bucket=self.bucket, Key=self._k(key), Body=data)

    def put_fileobj(self, key: str, fileobj: BinaryIO) -> None:
        self.client.upload_fileobj(fileobj, self.bucket, self._k(key))

    def list_keys(self, prefix: str) -> list[str]:
        keys: list[str] = []
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=self._k(prefix)):
            keys.extend(obj["Key"] for obj in page.get("Contents", []))
        return keys

    def delete_prefix(self, prefix: str) -> int:
        keys = self.list_keys(prefix)
        for i in range(0, len(keys), 1000):
            chunk = [{"Key": k} for k in keys[i : i + 1000]]
            self.client.delete_objects(Bucket=self.bucket, Delete={"Objects": chunk})
        return len(keys)


class LocalObjectStore(ObjectStore):
    def __init__(self, settings: Settings):
        self.root = Path(settings.datalake_root.replace("file://", "", 1))

    def put_bytes(self, key: str, data: bytes) -> None:
        path = self.root / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)

    def put_fileobj(self, key: str, fileobj: BinaryIO) -> None:
        path = self.root / key
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "wb") as out:
            shutil.copyfileobj(fileobj, out)

    def list_keys(self, prefix: str) -> list[str]:
        base = self.root / prefix
        if not base.exists():
            return []
        return sorted(str(p.relative_to(self.root)) for p in base.rglob("*") if p.is_file())

    def delete_prefix(self, prefix: str) -> int:
        base = self.root / prefix
        if not base.exists():
            return 0
        n = sum(1 for p in base.rglob("*") if p.is_file())
        shutil.rmtree(base)
        return n


def build_object_store(settings: Settings) -> ObjectStore:
    return S3ObjectStore(settings) if settings.is_s3 else LocalObjectStore(settings)
