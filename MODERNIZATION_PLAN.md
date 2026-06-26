# Metatron-Cube Modernization Plan — Java 21 + Lucene 10 + Modern Stack

> Goal: run the whole stack on **Java 21**, upgrade the 2015–2016-era library set
> so the JVM/Guice startup actually works, and bring **Lucene to 10.x**.
>
> This is a multi-week effort. The work is sequenced so the project **builds and
> runs after every phase** — never a big-bang.

## Why this is needed (root cause recap)

- `master` targets **Java 8** (`maven.compiler.target=1.8`). The local bump to `1.9`
  forces a Java 11 runtime.
- On Java 11+, the old libraries break. Concretely, **Guice 4.1** fails during
  injector creation and its bundled ASM can't read Java 9+ bytecode, which *masks*
  the real error (`CreationException` → `Errors.formatSource` → `IllegalArgumentException`).
- **Lucene 10 requires Java 21**; Lucene 9 requires Java 11. So "Lucene 10" and
  "old Druid core on Java 8" cannot share one runtime — the whole stack must move
  forward together.

## Current vs target versions

| Component | Current | Target | Risk |
|-----------|---------|--------|------|
| Java | 8 (target 1.8) | **21** | — |
| Guice | 4.1.0 | **6.0.0** (keeps `javax.inject`) | **High** — the crash; pervasive |
| Guava | 16.0.1 | **33.x** | **High** — removed APIs (`Throwables.propagate`, …) |
| Jackson | 2.4.6 | 2.17.x | Med — serialization-wide |
| Jersey | 1.19 (`com.sun.jersey`) | **3.x** (`org.glassfish.jersey`) | **High** — REST layer rewrite |
| Jetty | 9.3.24 | 10.x (javax) or 12 (jakarta) | High — couples with Jersey/servlet |
| servlet | javax.servlet 3.1 | javax (Jetty 10) → later jakarta | High |
| ZooKeeper / Curator | 3.4.8 / 4.0.0 | 3.9.x / 5.x | Med |
| Calcite | 1.35.0 | 1.37+ (verify Java 21) | Low–Med |
| Netty | 4.1.29 | 4.1.x latest | Low |
| Lucene | 7.7 / 8.11 / 9.8 | **10.x** (new module) | High — Java 21 + API breaks |
| aws-sdk (s3) | 1.10.21 + jets3t 0.9.4 | aws-sdk **2.x** (or v1 1.12.x interim) | High — jets3t removal |
| log4j | 2.17.1 | 2.23.x | Low |
| JAXB | jaxb-api 2.3.1 | glassfish jaxb runtime (explicit) | Low — removed from JDK 11+ |
| Hadoop (geometry coupling) | 2.3.0 | decouple or 3.x | Med — see note |

## Critical path

`Java 21 toolchain → Guice 6 → Guava 33 → (Jackson) → web stack (Jersey/Jetty) → ZK/Curator → Lucene 10 → s3/aws`

Guice and Guava unblock startup; the web stack is the largest single chunk;
Lucene 10 is the headline feature but depends on everything else being on Java 21.

---

## Phases (each ends green: `mvn install` + smoke-run all roles)

### Phase 0 — Baseline & safety net
- New branch off `master` (e.g. `modernize-java21`). Keep the `dockerize` work to rebase later.
- **Lock in a known-good reference**: build + run once on **Java 8** (revert target to 1.8,
  exclude `lucene9` from reactor). Confirms the cluster starts and tests pass — this is the
  behavioral oracle for the migration.
- Inventory test coverage; note modules with thin tests (higher migration risk).

### Phase 1 — Java 21 toolchain
- `maven.compiler.release=21` (use `release`, not source/target).
- Builder/runtime images → `eclipse-temurin:21`.
- Fix javac breakage (removed APIs, `sun.misc.*`, etc.). Expect `--add-opens`/`--add-exports`
  needs for reflection-heavy libs; collect them into each role's `jvm.config`.

### Phase 2 — Guice 4.1 → 6.0 *(unblocks the startup crash)*
- Bump `guice.version` to 6.0.0 (stays on `javax.inject`, so no annotation churn yet).
- Replace removed/changed Guice internals; verify `PolyBind`, `LifecycleScope`,
  multibindings, servlet module still compile.
- Target: all roles create their injector and start on Java 21.

### Phase 3 — Guava 16 → 33
- Sweep removed APIs. Known hits in this repo: `Throwables.propagate` (e.g. `ServerRunnable.run`),
  likely `Closeables`, `Objects.firstNonNull`, `Iterators` signatures, `MoreExecutors`.
- Mechanical but wide; do module-by-module.

### Phase 4 — Jackson 2.4 → 2.17
- Bump `jackson.version`; fix `ObjectMapper` config changes, `@JsonXxx` behavior,
  smile/datatype modules. Watch serde round-trips (segment metadata, query specs).

### Phase 5 — Web stack (Jersey 1.x → 3.x, Jetty 9.3 → 10/12)
- Largest chunk. `com.sun.jersey` → `org.glassfish.jersey`; Guice↔Jersey bridge changes;
  resource/filter registration rewrite.
- Decide javax vs jakarta now: Jetty 10 + javax keeps churn lower short-term; Jetty 12 +
  jakarta is the durable target (forces `javax.* → jakarta.*` across servlets/ws.rs).
- Recommend: **javax (Jetty 10 / Jersey 2.x) first**, then a separate jakarta sweep.

### Phase 6 — ZooKeeper 3.4 → 3.9, Curator 4 → 5
- Bump together (Curator 5 requires ZK 3.6+). Update compose `zookeeper:3.9`.

### Phase 7 — Lucene → 10.x (the headline)
- New module `lucene10-extensions` ported from `lucene9-extensions` (9→10 API breaks:
  analyzers, `IndexableField`, codecs, spatial). Keep `lucene-common` shared bits.
- Re-evaluate the `lucene-common → geometry-extensions → hadoop` coupling (see note);
  ideally decouple so Lucene no longer drags Hadoop.
- Re-enable lucene in `loadList` once green.

### Phase 8 — aws-sdk / S3
- Replace `jets3t` + aws-sdk v1 with **aws-sdk v2** (`s3`), rewrite `S3StorageDruidModule`
  / firehose. Interim option: aws-sdk v1 `1.12.x` (compiles on 21) to defer the v2 rewrite.

### Phase 9 — Cleanup
- Trim `--add-opens`, bump log4j 2.23, jaxb runtime explicit, netty latest.
- Re-apply the Docker work (Dockerfile → JDK21, restore lucene in loadList, revisit
  whether `-h hadoop-client` is still needed after Phase 7 decoupling).

---

## Notes / decisions to make

- **Hadoop coupling**: `lucene-common` uses `io.druid.query.GeomUtils/ShapeFormat` from
  `geometry-extensions`, and `geometry-extensions` is `HADOOP_DEPENDENT` because
  `GeoJsonFormatter` uses `hadoop.fs.FileSystem`. Either keep bundling `hadoop-client`,
  or (cleaner) drop the hadoop dependency from `GeoJsonFormatter` so Lucene is hadoop-free.
- **javax → jakarta**: unavoidable eventually (Jetty 12 / Jersey 3). Doing it in one
  dedicated phase after the rest is on Java 21 keeps each step reviewable.
- **Guice 6 vs 7**: 7.0 switches to `jakarta.inject`; pick it only together with the
  jakarta sweep. Until then, **Guice 6.0** (javax) is the right target.
- **Scope question**: is the goal a faithful port of the current feature set, or a slimmed
  fork (drop hive/orc/parquet/legacy-kafka modules entirely from the reactor)? Slimming
  first shrinks the migration surface a lot.

## Reusable from the Docker work already done
- `Dockerfile`, `docker-compose.yml`, `docker-entrypoint.sh`, `docker/conf/druid/*`
  all carry over; only the base image (→ JDK 21) and `loadList` (re-add lucene) change.
- The `distribution/pom.xml` extension trim stays valid.
