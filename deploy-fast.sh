#!/usr/bin/env bash
# Fast iteration deploy: recompile only the given modules and overlay their jars onto the stable :base image,
# skipping the distribution assembly / pull-deps / full tarball. Use deploy-full (bp) when deps/extensions change.
#   ./deploy-fast.sh processing server      # modules whose jars changed (default: processing)
set -e
cd "$(dirname "$0")"
REG=nas2.s2dev.net:5050/adp/metatron-cube
VER=2021.3-SNAPSHOT
MODS=("${@:-processing}")
CTX=$(mktemp -d)

echo "=== compile: ${MODS[*]} ==="
IFS=,; PL="${MODS[*]}"; unset IFS
mvn -o -q -DskipTests -pl "$PL" install \
  -Dcheckstyle.skip=true -Drat.skip=true -Dmaven.javadoc.skip=true -Denforcer.skip=true

echo "=== overlay image FROM :base ==="
echo "FROM $REG:base" > "$CTX/Dockerfile"
for m in "${MODS[@]}"; do
  jar="$m/target/druid-$m-$VER.jar"
  cp "$jar" "$CTX/"
  echo "COPY druid-$m-$VER.jar /opt/druid/lib/druid-$m-$VER.jar" >> "$CTX/Dockerfile"
done
docker buildx build --platform linux/amd64 --provenance=false -t $REG:prune --load "$CTX" >/dev/null
docker push $REG:prune 2>&1 | tail -1
rm -rf "$CTX"

echo "=== rollout ==="
kubectl -n adp-dev rollout restart deploy/standalone-atomcred
kubectl -n adp-dev rollout status deploy/standalone-atomcred --timeout=180s
echo "=== fast deploy done ==="
