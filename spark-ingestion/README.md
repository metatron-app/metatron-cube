# druid-spark-ingestion

Spec-driven Spark job that builds Druid segments and publishes them — **no MapReduce, no
Hadoop indexer, no Druid task**. Spark reads the source natively, builds one segment per
`(interval, shard)` with the embeddable `druid-segment-writer`, pushes to S3 deep storage,
and the driver publishes the batch via the overlord's bulk-publish endpoint.

```
Spark read (parquet/…) → mapToPair by (segmentInterval, shard)
  → groupByKey → SegmentIngestor.buildSegment(...) → S3 deep storage   (per partition, on executors)
  → driver collects DataSegments → POST /druid/indexer/v1/segments/publish
Druid coordinator assigns → historical loads from S3 → queryable
```

## Build (online — pulls Spark)
This module is **not** in the root reactor (Spark isn't in the offline repo). Build it alone:
```sh
mvn -o -pl segment-writer -am -Dmaven.test.skip=true install   # writer into local .m2 (offline)
mvn -f spark-ingestion/pom.xml -DskipTests package             # online; downloads Spark
# -> spark-ingestion/target/druid-spark-ingestion-2021.3-SNAPSHOT-spark.jar  (Guava/Jackson relocated)
```

## The spec
`SegmentIngestSpec` — parsed with Druid's mapper, so `metrics` use **native aggregator JSON**.
See `example/ingest-spec.json`. Key fields:

| field | meaning |
|-------|---------|
| `paths` / `format` | source the Spark `DataFrameReader` loads (e.g. `s3a://…`, `parquet`) |
| `timestampColumn` | column holding the row time (millis / Timestamp / ISO string) |
| `dimensions` | dimension column names |
| `metrics` | Druid aggregator specs (`count`, `longSum`, `hyperUnique`, …) |
| `segmentGranularity` / `queryGranularity` | `DAY`/`HOUR`/… and `NONE`/… |
| `numShards` | segments per interval (`>1` → `LinearShardSpec`) |
| `bucket`/`baseKey`/`endpoint`/`disableAcl` | S3 deep-storage target (creds via `AWS_*` env) |
| `publishUrl` | overlord bulk-publish endpoint |

## Run on k8s (spark-operator)
1. Copy the secret into `spark-apps`:
   ```sh
   kubectl -n spark-apps create secret generic swd-iceberg-credentials \
     --from-literal=s3-access-key="$(kubectl -n spark-apps get secret swd-iceberg-credentials -o jsonpath='{.data.s3-access-key}' | base64 -d)" \
     --from-literal=s3-secret-key="$(kubectl -n spark-apps get secret swd-iceberg-credentials -o jsonpath='{.data.s3-secret-key}' | base64 -d)"
   ```
   (Already there if the secret lives in `spark-apps`.)
2. Bake the jar + `hadoop-aws` into a Spark 3.5.1 image (`mainApplicationFile` points at it), or
   set `mainApplicationFile` to an `s3a://`/`http://` URL.
3. `kubectl apply -f k8s/spark-ingestion.yaml` (edit the image + spec ConfigMap first).
4. Query the result on the broker once the coordinator assigns and a historical loads.

## Notes
- **Credentials**: never in the spec — both the Spark S3A reader and the writer's S3 pusher
  use `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` (default chain), injected from the secret.
- **Atomicity / versioning**: the driver uses one `version` per run and publishes all segments
  in a single call. The publish endpoint is **lock-free** — use a distinct run (version) per
  interval to avoid clobbering concurrent writers.
- **Memory**: `groupByKey` buffers a segment's rows on one executor; size `numShards`/executors
  so a single segment fits in memory. (A streaming writer is a future improvement.)
- **Java 21**: the segment writer needs the `--add-opens` shown in `sparkConf`.
