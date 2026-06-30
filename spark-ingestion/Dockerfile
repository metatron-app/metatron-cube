# Spark image for druid-spark-ingestion.
#
# Our job jar (and its Druid deps: processing/Lucene 10) is Java 21 bytecode, but
# apache/spark:3.5.1 ships a Java 11 JRE -> UnsupportedClassVersionError (65.0 vs 55.0).
# So we keep the official Spark image (operator-compatible entrypoint + layout) and
# swap in a Java 21 JRE, repointing JAVA_HOME. Spark 3.5 runs on Java 21 with the
# module --add-opens Spark already passes (see also sparkConf in k8s/spark-ingestion.yaml).
#
# Build context is THIS dir so the root .dockerignore (**/target) doesn't hide the jar:
#   mvn -f spark-ingestion/pom.xml -DskipTests package
#   DOCKER_HOST=ssh://vm docker build -f spark-ingestion/Dockerfile \
#       -t nas2.s2dev.net:5050/adp/spark-druid-ingestion:3.5.1 spark-ingestion/
#   ssh vm 'docker push nas2.s2dev.net:5050/adp/spark-druid-ingestion:3.5.1'
FROM apache/spark:3.5.1

USER root

# --- Java 21 JRE (replaces the image's Java 11) ---
RUN set -eux; \
    apt-get update; apt-get install -y --no-install-recommends curl ca-certificates; \
    rm -rf /var/lib/apt/lists/*; \
    curl -fsSL "https://api.adoptium.net/v3/binary/latest/21/ga/linux/x64/jre/hotspot/normal/eclipse" -o /tmp/jre21.tgz; \
    mkdir -p /opt/java21; tar -xzf /tmp/jre21.tgz -C /opt/java21 --strip-components=1; rm /tmp/jre21.tgz
ENV JAVA_HOME=/opt/java21
ENV PATH=/opt/java21/bin:$PATH

# --- hadoop-aws for s3a:// (match Spark 3.5.1's bundled Hadoop 3.3.4) ---
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_BUNDLE_VERSION=1.12.262
RUN set -eux; cd "$SPARK_HOME/jars"; \
    curl -fsSL -O "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar"; \
    curl -fsSL -O "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar"

# --- Apache Iceberg (Spark 3.5 runtime + AWS bundle for S3FileIO) to read tables through the
#     Polaris REST catalog. iceberg-aws-bundle carries the AWS SDK v2 that S3FileIO needs. ---
ARG ICEBERG_VERSION=1.6.1
RUN set -eux; cd "$SPARK_HOME/jars"; \
    curl -fsSL -O "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar"; \
    curl -fsSL -O "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar"

# --- the shaded job jar (Guava/Jackson relocated inside it) ---
COPY target/druid-spark-ingestion-*-spark.jar /opt/spark/jars/druid-spark-ingestion-spark.jar

USER 185
