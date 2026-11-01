# Single image used by the Dagster code server, webserver, daemon and runner.
FROM python:3.11-slim-bookworm

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    TZ=UTC \
    DAGSTER_HOME=/opt/dagster/dagster_home

# Java runtime for Spark (JAVA_HOME made architecture-independent: amd64 / arm64)
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps curl ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && ln -s "$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" /opt/java
ENV JAVA_HOME=/opt/java

# 1) Python dependencies (cached layer: only invalidated when pyproject changes)
WORKDIR /opt/pipeline
COPY pipeline/pyproject.toml .
RUN python -c "import tomllib;print('\n'.join(tomllib.load(open('pyproject.toml','rb'))['project']['dependencies']))" > /tmp/requirements.txt \
    && pip install -r /tmp/requirements.txt

# 2) S3A connector for Spark -> MinIO. Versions must match the Hadoop client
#    bundled with PySpark 3.5.x (Hadoop 3.3.4).
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_BUNDLE_VERSION=1.12.262
RUN SPARK_JARS="$(python -c 'import os,pyspark;print(os.path.join(os.path.dirname(pyspark.__file__),"jars"))')" \
    && curl -fsSL -o "$SPARK_JARS/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
       "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
    && curl -fsSL -o "$SPARK_JARS/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar" \
       "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar"

# 3) Pipeline code
COPY pipeline/ /opt/pipeline/
RUN pip install --no-deps -e /opt/pipeline

# 4) Dagster instance + workspace + helper scripts
COPY docker/dagster/dagster.yaml docker/dagster/workspace.yaml ${DAGSTER_HOME}/
COPY scripts/ /opt/scripts/
RUN chmod +x /opt/scripts/*.sh

WORKDIR /opt/pipeline
