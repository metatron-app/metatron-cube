# syntax=docker/dockerfile:1
# Licensed to SK Telecom Co., LTD. (SK Telecom) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  SK Telecom licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# ---------------------------------------------------------------------------
# Stage 1: build the full distribution tarball (mvn install -> distribution/)
# ---------------------------------------------------------------------------
# Modernization: the project now targets Java 21 (see <maven.compiler.target>).
FROM maven:3.9-eclipse-temurin-21 AS builder

WORKDIR /src

# Warm the dependency cache: copy every pom first so a source-only change
# does not force a full re-download of the Maven repository.
COPY pom.xml ./
COPY . .

# The distribution module's `git-info` step runs distribution/bin/latest.sh,
# which shells out to git. The build context has no .git (excluded in
# .dockerignore — it is 200MB+), so the original script fails with exit 128 and
# clobbers the pre-generated git.tag/git.version/git.history. Replace it with a
# resilient version: refresh from git when a repo is present, otherwise keep the
# git.* files already shipped in the source tree (and ensure they exist).
RUN cat > distribution/bin/latest.sh <<'EOF'
#!/bin/sh
B="$1"
if command -v git >/dev/null 2>&1 && git -C "$B" rev-parse HEAD >/dev/null 2>&1; then
  git -C "$B" describe --tags --exact-match > "$B/git.tag" 2>/dev/null || : > "$B/git.tag"
  git -C "$B" rev-parse --short HEAD > "$B/git.version"
  git -C "$B" log --oneline -30 > "$B/git.history"
fi
[ -f "$B/git.tag" ]     || : > "$B/git.tag"
[ -f "$B/git.version" ] || echo unknown > "$B/git.version"
[ -f "$B/git.history" ] || : > "$B/git.history"
exit 0
EOF

# Builds all modules incl. the `distribution` module, which assembles
# distribution/target/druid-<version>-bin.tar.gz (lib/, conf/, bin/, ...).
# Note: the distribution module's `pull-deps` step downloads the Hadoop and
# extension jars, so this stage needs network access and is not fast.
# skipTests (run skipped, but test sources are compiled and test-jars produced —
# some modules depend on others' test-jars, e.g. druid-common -> java-util:tests).
RUN mvn -B clean install -DskipTests=true \
      -Dmaven.javadoc.skip=true -Dcheckstyle.skip=true -Drat.skip=true

# Unpack the assembled tarball into a fixed, version-independent path.
RUN mkdir -p /opt/druid \
      && tar -xzf distribution/target/druid-*-bin.tar.gz \
             -C /opt/druid --strip-components=1

# ---------------------------------------------------------------------------
# Stage 2: slim runtime image
# ---------------------------------------------------------------------------
# Code is compiled to Java 21 bytecode (major version 65); the JRE must be >= 21.
FROM eclipse-temurin:21-jre

LABEL org.opencontainers.image.title="metatron-cube (druid)" \
      org.opencontainers.image.description="SK Telecom Metatron-Cube / Druid fork" \
      org.opencontainers.image.source="https://github.com/SKTelecom/metatron-cube"

# Run as an unprivileged user.
RUN groupadd -r druid && useradd -r -g druid -d /opt/druid druid

COPY --from=builder /opt/druid /opt/druid
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh \
      && mkdir -p /opt/druid/var \
      && chown -R druid:druid /opt/druid

ENV DRUID_HOME=/opt/druid \
    DRUID_CONF_DIR=/opt/druid/conf/druid

WORKDIR /opt/druid
USER druid

# Druid service HTTP ports (coordinator/broker/historical/overlord/middleManager).
EXPOSE 8081 8082 8083 8090 8091

# Pass the role as the command, e.g. `docker run <img> broker`.
# Valid roles: coordinator broker historical overlord middleManager router
ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
CMD ["broker"]
