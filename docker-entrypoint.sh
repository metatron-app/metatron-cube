#!/bin/sh
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
set -e

DRUID_HOME=${DRUID_HOME:-/opt/druid}
DRUID_CONF_DIR=${DRUID_CONF_DIR:-${DRUID_HOME}/conf/druid}

ROLE="${1:-${DRUID_ROLE}}"
if [ -z "$ROLE" ]; then
  echo "usage: docker run <image> <role>   (coordinator|broker|historical|overlord|middleManager|router)" >&2
  echo "   or: set the DRUID_ROLE environment variable" >&2
  exit 1
fi

ROLE_CONF="${DRUID_CONF_DIR}/${ROLE}"
if [ ! -d "$ROLE_CONF" ]; then
  echo "unknown role '${ROLE}': no config dir at ${ROLE_CONF}" >&2
  echo "available roles:" >&2
  ls -1 "$DRUID_CONF_DIR" 2>/dev/null | sed 's/^/  - /' >&2
  exit 1
fi

# Working/scratch dirs referenced by the configs (java.io.tmpdir=var/tmp, deep
# storage, task dirs, ...). WORKDIR is DRUID_HOME so these are relative to it.
mkdir -p "${DRUID_HOME}/var/tmp" "${DRUID_HOME}/var/druid"

# JVM args come from the role's jvm.config (one flag per line). Flags in
# DRUID_JAVA_OPTS are appended afterwards, so e.g. a later -Xmx/-Xms overrides
# the one from jvm.config (the JVM honours the last occurrence).
JVM_ARGS=""
if [ -f "${ROLE_CONF}/jvm.config" ]; then
  JVM_ARGS=$(grep -v '^[[:space:]]*#' "${ROLE_CONF}/jvm.config" | xargs)
fi

# Java 17+/21 strong encapsulation: this 2016-era code (and libs like the gridkit
# perfdata JvmMonitor, mapdb, direct-buffer/Cleaner usage) reaches into JDK
# internals that are no longer open by default. Open/export what Druid needs.
JDK_MODULE_OPTS="\
--add-opens=java.base/java.lang=ALL-UNNAMED \
--add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
--add-opens=java.base/java.io=ALL-UNNAMED \
--add-opens=java.base/java.nio=ALL-UNNAMED \
--add-opens=java.base/java.util=ALL-UNNAMED \
--add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
--add-exports=java.base/jdk.internal.ref=ALL-UNNAMED \
--add-exports=java.base/sun.nio.ch=ALL-UNNAMED \
--add-exports=java.management/sun.management.counter=ALL-UNNAMED \
--add-exports=java.management/sun.management.counter.perf=ALL-UNNAMED"

# This fork's PropertiesModule resolves "_common/common.runtime.properties" and
# "<role>/runtime.properties" relative to the classpath, so the conf *parent*
# dir (the one containing _common and the role dirs) must be on the classpath --
# not the _common/<role> dirs themselves. _common is also added so log4j2 picks
# up log4j2.xml from the classpath root. guava is shaded into lib/guava and must
# precede lib/* on the classpath.
CP="${DRUID_CONF_DIR}:${DRUID_CONF_DIR}/_common:${DRUID_HOME}/lib/*:${DRUID_HOME}/lib/guava/*"

echo "starting druid role=${ROLE}"
exec java ${JDK_MODULE_OPTS} ${JVM_ARGS} ${DRUID_JAVA_OPTS} -cp "${CP}" io.druid.cli.Main server "${ROLE}"
