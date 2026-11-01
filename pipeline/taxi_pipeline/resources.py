"""Dagster resources: Spark session and datalake (object store) access."""

from __future__ import annotations

from dagster import ConfigurableResource, InitResourceContext
from pydantic import PrivateAttr
from pyspark.sql import SparkSession

from .config import Settings, get_settings
from .storage import ObjectStore, build_object_store


def build_spark_session(settings: Settings, app_name: str = "chicago-taxi-datalake") -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master(settings.spark_master)
        .config("spark.driver.memory", settings.spark_driver_memory)
        .config("spark.sql.shuffle.partitions", str(settings.spark_shuffle_partitions))
        .config("spark.sql.session.timeZone", "UTC")
        # Re-running a month only rewrites that month's partitions (idempotency).
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.ui.showConsoleProgress", "false")
    )
    if settings.is_s3:
        # hadoop-aws + aws-java-sdk-bundle jars are baked into the image (see Dockerfile).
        builder = (
            builder.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.endpoint", settings.s3_endpoint)
            .config("spark.hadoop.fs.s3a.access.key", settings.s3_access_key)
            .config("spark.hadoop.fs.s3a.secret.key", settings.s3_secret_key)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled",
                    str(settings.s3_endpoint.startswith("https")).lower())
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
            # Safer commits on object storage (no rename-based output commit issues).
            .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


class SparkResource(ConfigurableResource):
    """One local SparkSession per Dagster run, shared by all steps.

    Runs use the in-process executor, so bronze -> silver -> gold steps reuse
    the same JVM instead of paying the Spark start-up cost at every step.
    """

    app_name: str = "chicago-taxi-datalake"
    _spark: SparkSession | None = PrivateAttr(default=None)
    _owned: bool = PrivateAttr(default=False)

    def get_session(self) -> SparkSession:
        if self._spark is None:
            # Only stop at teardown a session this resource created itself.
            self._owned = SparkSession.getActiveSession() is None
            self._spark = build_spark_session(get_settings(), self.app_name)
        return self._spark

    def teardown_after_execution(self, context: InitResourceContext) -> None:
        if self._spark is not None and self._owned:
            self._spark.stop()
            self._spark = None


class DatalakeResource(ConfigurableResource):
    """Gives assets the datalake layout (paths) and a raw object-store client."""

    def settings(self) -> Settings:
        return get_settings()

    def path(self, *parts: str) -> str:
        return self.settings().path(*parts)

    def store(self) -> ObjectStore:
        return build_object_store(self.settings())
